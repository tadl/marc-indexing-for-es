#!/usr/bin/env python

# xpath work Based partially on an example from
# http://stackoverflow.com/a/16699042/157515

import argparse
import logging
import logging.config
import re
import sys
import time

import configparser  # under Python 2, this is a backport
import lxml.etree as ET
import psycopg2
from elasticsearch import Elasticsearch
import elasticsearch.exceptions

parser = argparse.ArgumentParser()
parser.add_argument('--id')
parser.add_argument('--full', action='store_true')
parser.add_argument('--incremental', action='store_true')

args = parser.parse_args()

logging.config.fileConfig('index-config.ini')
config = configparser.ConfigParser()
config.read('index-config.ini')

es = Elasticsearch([config['elasticsearch']['url']])

es_index = config['elasticsearch']['index']

org_root = config.get('evergreen', 'org_root', fallback=1)

# FIXME: Hardcoded for now
game_genre_suffixes = {
    'Nintendo Wii U video games.': '(Wii U)',
    'Nintendo Wii video games.': '(Wii)',
    'PlayStation 3 video games.': '(PS3)',
    'PlayStation 2 video games.': '(PS2)',
    'PlayStation 4 video games.': '(PS4)',
    'Xbox 360 video games.': '(Xbox 360)',
    'Xbox video games.': '(Xbox)',
}

if (es.ping()):
    print("ping!")

xml_filename = None
xsl_filename = 'MARC21slim2MODS3-2.xsl'

namespace_dict = {
    'mods32': 'http://www.loc.gov/mods/v3',
    'marc': 'http://www.loc.gov/MARC21/slim',
}

indexes = {
    'title': {
        'xpath': "//mods32:mods/mods32:titleInfo[mods32:title and not (@type)]",
    },
    'title_alt': {
        'xpath': "//mods32:mods/mods32:titleInfo[mods32:title and (@type='alternative')]",
        'array': True
    },
    'author': {
        'xpath': "//mods32:mods/mods32:name[@type='personal' and mods32:role/mods32:roleTerm[text()='creator']]",
        'post_xpath': "//*[local-name()='namePart']"
    },
    'corpauthor': {
        'xpath': "//mods32:mods/mods32:name[@type='corporate' and (mods32:role/mods32:roleTerm[text()='creator'] or mods32:role/mods32:roleTerm[text()='aut'] or mods32:role/mods32:roleTerm[text()='cre'])]",
        'post_xpath': "//*[local-name()='namePart']"
    },
    'author_other': {
        'xpath':
            "//mods32:mods/mods32:name[@type='personal' and not(mods32:role/mods32:roleTerm[text()='creator'])]",
        'post_xpath': "//*[local-name()='namePart' and (not(@*[local-name()='type']) or not(@*[local-name()='type']='date'))] ",
        'array': True
    },
    'abstract': {
        'xpath': "//mods32:mods/mods32:abstract",
    },
    'contents': {
        'xpath': "//mods32:mods/mods32:tableOfContents",
    },
    'physical_description': {
        'xpath': "//mods32:mods/mods32:physicalDescription/mods32:extent",
    },
    'type_of_resource': {
        'xpath': "//mods32:mods/mods32:typeOfResource",
    },
    'publisher': {
        'xpath': "//mods32:mods/mods32:originInfo/mods32:publisher",
    },
    'publisher_location': {
        'xpath': "//mods32:mods/mods32:originInfo/mods32:place/mods32:placeTerm[@type='text']",
    },
    'record_year': {
        'xpath': "//mods32:mods/mods32:originInfo/mods32:dateIssued",
    },
    'sort_year': {
        'xpath': "//mods32:mods/mods32:originInfo/mods32:dateIssued[@encoding='marc']",
        'validation': '^\d+$',
    },
    'isbn': {
        'xpath': "//mods32:mods/mods32:identifier[@type='isbn']",
        'array': True
    },
    'series': {
        'xpath': "//mods32:mods/mods32:relatedItem[@type='series']/mods32:titleInfo/mods32:title",
        'array': True
    },
    'links': {
        'xpath': "//mods32:mods/mods32:identifier[@type='uri']",
        'array': True
    }
}

xslt = ET.parse(xsl_filename)
transform = ET.XSLT(xslt)


def insert_to_elasticsearch(output):
    if output['holdings'] == [] and output['links'] == '':
        try:
            logging.info('Removing record ID %s' % (output['id'],))
            deleteresult = es.delete(index=es_index, doc_type='record', id=output['id'])
        except(elasticsearch.exceptions.NotFoundError):
            logging.info('Record ID %s not found. No big deal.' % (output['id'],))
            pass
    else:
        indexresult = es.index(index=es_index, doc_type='record', id=output['id'], body=output)
        logging.debug(repr(indexresult))


def detect_large_print(record):
    matches = record.xpath('//*[@tag="250" or @tag="300" '
                           'or @tag="650" or @tag="655"]/*[@code="a"]',
                           namespaces=namespace_dict)
    if (len(matches)):
        for match in matches:
            if match.text and re.search(r"large (print|type)",
                         match.text, flags=re.IGNORECASE):
                return True
    return False


def get_title_display(record, output):
    title_match = record.xpath('//*[@tag="245"]/*[@code]',
                               namespaces=namespace_dict)
    if (len(title_match)):
        title_parts = []
        for subfield in title_match:
            code = subfield.get("code")
            value = subfield.text
            if value:
                if code == 'a':
                    title_parts.append(value)
                elif code == 'b':
                    title_parts.append(value)
                elif code == 'c':
                    # omit
                    pass
                elif code == 'h':
                    # include only trailing punctuation
                    match = re.search('([:;/])\s*$', value)
                    if (match):
                        title_parts.append(match.group(0))
                else:
                    title_parts.append(value)
        title_display = ' '.join(title_parts)
        # Strip trailing punctuation from title
        title_display = re.sub('\s*[:;/]\s*$', '', title_display)
        # Append (LARGE PRINT) where appropriate
        if detect_large_print(record):
            title_display += ' (LARGE PRINT)'
        else:
            # If not large print, check for videogame platform suffix
            for genre in output['genres']:
                if genre in game_genre_suffixes:
                    title_display += ' ' + game_genre_suffixes[genre]
        return title_display
    else:
        logging.warn('Found no title for record.')

def get_titles_misc(mods):
    # We'd like to return two things:
    # title_short
    # title_nonfiling
    titles = mods.xpath(
        "//mods32:mods/mods32:titleInfo[mods32:title and not (@type)]",
        namespaces=namespace_dict)
    if (len(titles)):
        first_title = titles[0]
    else:
        logging.warn('No title matches')
        return ('', '')
    non_sort_match = first_title.xpath(
        ".//*[local-name()='nonSort']",
        namespaces=namespace_dict)
    title_match = first_title.xpath(
        ".//*[local-name()='title']",
        namespaces=namespace_dict)
    if (len(non_sort_match) and non_sort_match[0].text):
        title_short = non_sort_match[0].text
    else:
        title_short = ''
    if (len(title_match) and title_match[0].text):
        title_short = title_short + title_match[0].text
        title_nonfiling = title_match[0].text
    else:
        logging.warn('No value found for title_nonfiling')
        title_nonfiling = ''
    return (title_short, title_nonfiling)


def get_subjects(mods):
    subjects = []
    matches = mods.xpath(
        "//mods32:mods/mods32:subject[mods32:geographic or mods32:name "
        "or mods32:temporal or mods32:topic]",
        namespaces=namespace_dict)
    for match in matches:
        subjects.append(' '.join(match.itertext()))
    return subjects


def get_genres(mods):
    genres = []
    matches = mods.xpath("//mods32:mods/mods32:genre", namespaces=namespace_dict)
    for match in matches:
        genres.append(' '.join(match.itertext()))
    return genres


def get_901c(record):
    id = None

    match_901 = record.xpath("marc:datafield[@tag='901']/marc:subfield[@code='c']", namespaces=namespace_dict)

    if len(match_901):
        id = match_901[0].text
    else:
        logging.error('RECORD HAS NO 901: %s' % record)

    logging.debug('Found record id %s' % id)

    return id


def index_mods(mods):
    output = {}

    for index in list(indexes.keys()):
        logging.debug('Indexing %s' % index)
        xpath = indexes[index]['xpath']
        post_xpath = None
        if 'post_xpath' in indexes[index]:
            post_xpath = indexes[index]['post_xpath']
        r = mods.xpath(xpath, namespaces=namespace_dict)
        result = None
        if len(r):
            if post_xpath:
                r = r[0].xpath(post_xpath)
            if 'array' in indexes[index] and indexes[index]['array']:
                result = []
                for element in r:
                    result.append(' '.join(element.itertext()))
            else:
                result = ' '.join(r[0].itertext())
        if result:
            if 'validation' in indexes[index]:
                pattern = indexes[index]['validation']
                if re.search(pattern, result):
                    output[index] = result
            else:
                output[index] = result
        else:
            output[index] = ''

    if (output['author'] == ''):
        logging.debug('Setting corpauthor to author')
        output['author'] = output['corpauthor']

    (output['title_short'], output['title_nonfiling']) = get_titles_misc(mods)
    output['subjects'] = get_subjects(mods)
    output['genres'] = get_genres(mods)

    return output


def index_holdings(conn, record_ids):
    holdings_dict = {}
    holdings_count = 0

    cur = conn.cursor()

    cur.execute('''
SELECT acn.record, acp.id AS copy_id, acp.barcode, ccs.name AS status,
    aou.shortname AS circ_lib, acl.id AS location_id,
    acl.name AS location, acn.label AS call_number, ocirc.due_date::DATE
FROM asset.copy acp
JOIN config.copy_status ccs ON acp.status = ccs.id
JOIN asset.copy_location acl ON acp.location = acl.id
JOIN actor.org_unit aou ON acp.circ_lib = aou.id
JOIN asset.call_number acn ON acp.call_number = acn.id
JOIN asset.opac_visible_copies aovc ON acp.id = aovc.copy_id
LEFT JOIN action.open_circulation ocirc ON ocirc.target_copy = acp.id
WHERE acn.record = ANY(%(record_ids)s::BIGINT[])
AND acp.circ_lib IN (SELECT id FROM actor.org_unit_descendants(%(org_root)s))
''', {'record_ids': record_ids, 'org_root': org_root})

    for (record, copy_id, barcode, status, circ_lib, location_id, location,
         call_number, due_date) in cur:
        holdings_count += 1
        logging.debug(
            [record, copy_id, barcode, status, circ_lib, location_id, location,
             call_number, due_date])
        if record not in holdings_dict:
            holdings_dict[record] = []
        holdings_dict[record].append(
            {'barcode': barcode, 'status': status,
             'circ_lib': circ_lib, 'location_id': location_id,
             'location': location, 'call_number': call_number,
             'due_date': due_date})
    logging.info('Fetched %s holdings.' % (holdings_count,))
    return holdings_dict


def get_db_conn():
    dbcfg = config['evergreen_db']
    conn = psycopg2.connect(dbname=dbcfg['dbname'], user=dbcfg['user'],
                            password=dbcfg['password'], host=dbcfg['host'],
                            port=dbcfg['port'])
    return conn


def load_full_state():
    conf = configparser.ConfigParser()
    conf.read('index-state.ini')
    return {
        'start_date': conf.get('full', 'start_date', fallback=None),
        'in_progress': conf.getboolean('full', 'in_progress', fallback=False),
        'last_edit_date': conf.get('full', 'last_edit_date', fallback=None),
        'last_id': conf.getint('full', 'last_id', fallback=0)
    }


def load_incremental_state():
    conf = configparser.ConfigParser()
    conf.read('index-state.ini')
    return {
        'last_edit_date': conf.get('incremental', 'last_edit_date',
                                   fallback=None),
        'last_id': conf.getint('incremental', 'last_id', fallback=0),
    }


def set_state(section, key, value):
    conf = configparser.ConfigParser()
    conf.read('index-state.ini')
    if (section not in conf):
        conf.add_section(section)
    if (value is None):
        conf.remove_option(section, key)
    else:
        conf.set(section, key, str(value))
    with open('index-state.ini', 'w') as f:
        conf.write(f)


def full_index_page(egconn, state):
    egcur = egconn.cursor()
    index_count = 0
    last_edit_date = state['last_edit_date']
    last_id = state['last_id']

    egcur.execute('''
WITH visible AS (
SELECT record
FROM asset.opac_visible_copies aovc
WHERE circ_lib IN (SELECT id FROM actor.org_unit_descendants(%(org_root)s))
UNION
SELECT record
FROM asset.call_number acn
WHERE label = '##URI##' AND owning_lib IN (
    SELECT id FROM actor.org_unit_descendants(%(org_root)s)
)
AND NOT acn.deleted
)
SELECT bre.id, bre.marc, bre.create_date, bre.edit_date, cbs.source
FROM biblio.record_entry bre
LEFT JOIN config.bib_source cbs ON bre.source = cbs.id
JOIN visible ON visible.record = bre.id
WHERE (
    NOT bre.deleted
    AND bre.active
    AND (
        %(last_edit_date)s IS NULL
        OR (
            bre.edit_date >= %(last_edit_date)s
            AND bre.id > %(last_id)s
        )
        OR bre.edit_date > %(last_edit_date)s
    )
)
ORDER BY bre.edit_date ASC, bre.id ASC
LIMIT 1000
''', {'last_edit_date': last_edit_date, 'last_id': last_id,
      'org_root': org_root})

    # Clear last_edit_date, last_id
    state['last_edit_date'] = None
    state['last_id'] = None

    results = egcur.fetchall()

    # Get just the record IDs
    record_ids = []
    for result in results:
        record_ids.append(result[0])

    holdings = index_holdings(egconn, record_ids)

    for (bre_id, marc, create_date, edit_date, source) in results:
        index_count += 1
        record = ET.fromstring(marc)
        mods = transform(record)
        output = index_mods(mods)
        output['title_display'] = get_title_display(record, output)
        output['id'] = bre_id
        output['source'] = source
        output['create_date'] = create_date
        output['edit_date'] = edit_date
        if bre_id in holdings:
            output['holdings'] = holdings[bre_id]
        else:
            output['holdings'] = []
        logging.debug(repr(output))
        insert_to_elasticsearch(output)
        # Update state vars -- the most recent value of these will be
        # written to the state file after the current loop completes
        state['last_edit_date'] = edit_date
        state['last_id'] = bre_id
    return index_count, state


def full_index(egconn):
    state = load_full_state()
    if (state['in_progress'] != True):
        logging.info('STARTING NEW full indexing run')
        egcur = egconn.cursor()
        egcur.execute("SELECT NOW();")
        full_start_date = egcur.fetchone()[0]
        logging.info("Full index start time is %s" % (full_start_date,))
        set_state('full', 'start_date', full_start_date)
        set_state('full', 'in_progress', True)
        set_state('full', 'last_edit_date', None)
        set_state('full', 'last_id', None)
    else:
        logging.info('RESUMING EXISTING full indexing run')
    logging.info("Retreived state: " + repr(state))
    # Index a "page" of records at a time
    # loop while number of records indexed != 0
    indexed_count = None
    while (indexed_count != 0):
        start_time = time.time()
        (indexed_count, state) = full_index_page(egconn, state)
        time_taken = time.time() - start_time
        if (time_taken > 0):
            time_recs_sec = indexed_count / time_taken
        else:
            time_recs_sec = 0
        logging.info('indexed %s records in %.0fs (~%.3f rec/s) '
                     'ending with date %s id %s'
                     % (indexed_count, time_taken, time_recs_sec,
                        state['last_edit_date'], state['last_id']))
        # Write state vars to state file -- supports resuming a full
        # index run
        set_state('full', 'last_edit_date', state['last_edit_date'])
        set_state('full', 'last_id', state['last_id'])
        # Rollback transaction
        egconn.rollback()
    # if number of records is zero, we are no longer "in progress"
    set_state('full', 'in_progress', None)
    set_state('full', 'last_edit_date', None)
    set_state('full', 'last_id', None)
    newstate = load_full_state()
    set_state('incremental', 'last_edit_date', newstate['start_date'])
    set_state('incremental', 'last_id', 0)
    logging.info("DONE!")


def index_single_record(egconn, record_id):
    egcur = egconn.cursor()
    logging.info("Will index single record: %s" % (record_id,))
    egcur.execute("""
SELECT bre.id, bre.marc, bre.create_date, bre.edit_date, cbs.source
FROM biblio.record_entry bre
LEFT JOIN config.bib_source cbs ON bre.source = cbs.id
WHERE bre.id = %s""", (record_id,))
    results = egcur.fetchall()

    # Get just the record IDs
    record_ids = []
    for result in results:
        record_ids.append(result[0])

    holdings = index_holdings(egconn, record_ids)

    for (bre_id, marc, create_date, edit_date, source) in results:
        record = ET.fromstring(marc)
        mods = transform(record)
        output = index_mods(mods)
        output['title_display'] = get_title_display(record, output)
        output['id'] = bre_id
        output['source'] = source
        output['create_date'] = create_date
        output['edit_date'] = edit_date
        if bre_id in holdings:
            output['holdings'] = holdings[bre_id]
        else:
            output['holdings'] = []
        logging.debug(repr(output))
        insert_to_elasticsearch(output)


def incremental_index_page(egconn, state):
    egcur = egconn.cursor()
    index_count = 0
    last_edit_date = state['last_edit_date']
    last_id = state['last_id']

    egcur.execute('''
SELECT bre.id, bre.marc, bre.create_date, bre.edit_date, cbs.source,
    GREATEST(MAX(bre.edit_date), MAX(acn.edit_date), MAX(acp.edit_date)) AS
    last_edit_date
FROM biblio.record_entry bre
LEFT JOIN config.bib_source cbs ON bre.source = cbs.id
LEFT JOIN asset.call_number acn ON bre.id = acn.record
LEFT JOIN asset.copy acp ON acp.call_number = acn.id
WHERE (
    acp.circ_lib IN (SELECT id FROM actor.org_unit_descendants(%(org_root)s))
    OR acn.owning_lib IN (
        SELECT id FROM actor.org_unit_descendants(%(org_root)s)
    )
)
AND (
    (
        (
            bre.edit_date >= %(last_edit_date)s
            OR acn.edit_date >= %(last_edit_date)s
            OR acp.edit_date >= %(last_edit_date)s
        )
        AND bre.id > %(last_id)s
    )
    OR (
        bre.edit_date > %(last_edit_date)s
        OR acn.edit_date > %(last_edit_date)s
        OR acp.edit_date > %(last_edit_date)s
    )
)
GROUP BY bre.id, bre.marc, bre.create_date, bre.edit_date, cbs.source
ORDER BY GREATEST(
    MAX(bre.edit_date), MAX(acn.edit_date), MAX(acp.edit_date)
) ASC, bre.id ASC
LIMIT 1000
''', {'last_edit_date': last_edit_date, 'last_id': last_id,
      'org_root': org_root})

    results = egcur.fetchall()

    # Get just the record IDs
    record_ids = []
    for result in results:
        record_ids.append(result[0])

    holdings = index_holdings(egconn, record_ids)

    for (bre_id, marc, create_date, edit_date, source,
         last_edit_date) in results:
        logging.info("bib %s last_edit_date %s" % (bre_id, last_edit_date))
        index_count += 1
        record = ET.fromstring(marc)
        mods = transform(record)
        output = index_mods(mods)
        output['title_display'] = get_title_display(record, output)
        output['id'] = bre_id
        output['source'] = source
        output['create_date'] = create_date
        output['edit_date'] = edit_date
        if bre_id in holdings:
            output['holdings'] = holdings[bre_id]
        else:
            output['holdings'] = []
        logging.debug(repr(output))
        insert_to_elasticsearch(output)
        # Update state vars -- the most recent value of these will be
        # written to the state file after the current loop completes
        state['last_edit_date'] = last_edit_date
        state['last_id'] = bre_id
    return index_count, state


def incremental_index(egconn):
    # For incremental, we must have:
    #  - a last_edit_date
    #  - a last_id
    # If this is our first incremental, the last_edit_date
    # will be the start_date of the full index run
    # This way, we will pick up any changes that happened during the full
    # index run
    # Our full index must not be in progress
    full_state = load_full_state()
    if (full_state['in_progress']):
        logging.error("Cannot perform incremental index while full index is "
                      "in a state of in_progress")
        sys.exit(1)
    state = load_incremental_state()
    logging.info("Loaded state " + repr(state))
    # Index a "page" of records at a time
    # loop while number of records indexed != 0
    indexed_count = None
    while (indexed_count is None) or (indexed_count == 1000):
        (indexed_count, state) = incremental_index_page(egconn, state)
        logging.info('indexed %s records ending with date %s id %s'
                     % (indexed_count, state['last_edit_date'],
                        state['last_id']))
        # Write state vars to state file -- supports resuming an
        # incremental index run
        set_state('incremental', 'last_edit_date', state['last_edit_date'])
        set_state('incremental', 'last_id', state['last_id'])
        # Rollback transaction
        egconn.rollback()
    # if we get a "page" that contains fewer records than the LIMIT count,
    # we are done for this run
    logging.info("DONE with incremental!")


if (xml_filename):
    # Index records from XML file
    collection_dom = ET.parse(xml_filename)

    collection = collection_dom.getroot()

    for record in collection:
        mods = transform(record)
        output = index_mods(mods)
        output['title_display'] = get_title_display(record)
        output['id'] = get_901c(record)
        output['create_date'] = None
        output['edit_date'] = None
        insert_to_elasticsearch(output)
else:
    # Index records from database
    egconn = get_db_conn()
    if (args.id):
        logging.info(repr(args.id))
        index_single_record(egconn, args.id)
    elif (args.full):
        full_index(egconn)
    elif (args.incremental):
        incremental_index(egconn)
    else:
        print("Must specify one of incremental or full")
        sys.exit(1)
