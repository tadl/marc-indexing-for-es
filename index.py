#!/usr/bin/env python

# xpath work Based partially on an example from
# http://stackoverflow.com/a/16699042/157515

import logging
import logging.config

import configparser  # under Python 2, this is a backport
import lxml.etree as ET
import psycopg2
from elasticsearch import Elasticsearch

logging.config.fileConfig('index-config.ini')
config = configparser.ConfigParser()
config.read('index-config.ini')

es = Elasticsearch([config['elasticsearch']['url']])

es_index = config['elasticsearch']['index']

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
    'author': {
        'xpath': "//mods32:mods/mods32:name[@type='personal' and mods32:role/mods32:roleTerm[text()='creator']]",
        'post_xpath': "//*[local-name()='namePart']"
    },
    'corpauthor': {
        'xpath': "//mods32:mods/mods32:name[@type='corporate' and (mods32:role/mods32:roleTerm[text()='creator'] or mods32:role/mods32:roleTerm[text()='aut'] or mods32:role/mods32:roleTerm[text()='cre'])]",
        'post_xpath': "//*[local-name()='namePart']"
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
    indexresult = es.index(index=es_index, doc_type='record', id=output['id'], body=output)
    logging.debug(repr(indexresult))


def get_max_update():
    # XXX: FIXME: disabled for now
    return None

    cur.execute("SELECT MAX(updated_at) FROM records;")
    result = cur.fetchone()
    max_update = result[0]
    logging.debug('Max update is %s' % max_update)
    return result[0]


def get_subjects(mods):
    subjects = []
    matches = mods.xpath("//mods32:mods/mods32:subject/mods32:topic", namespaces=namespace_dict)
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
            output[index] = result
        else:
            output[index] = ''

    if (output['author'] == ''):
        logging.info('Setting corpauthor to author')
        output['author'] = output['corpauthor']

    output['subjects'] = get_subjects(mods)
    output['genres'] = get_genres(mods)

    return output


def index_holdings(conn, rec_id):
    holdings = []

    cur = conn.cursor()

    cur.execute('''
SELECT acp.id AS copy_id, acp.barcode, ccs.name AS status, aou.shortname AS circ_lib, acl.name AS location, acn.label AS call_number
FROM asset.copy acp
JOIN config.copy_status ccs ON acp.status = ccs.id
JOIN asset.copy_location acl ON acp.location = acl.id
JOIN actor.org_unit aou ON acp.circ_lib = aou.id
JOIN asset.call_number acn ON acp.call_number = acn.id
JOIN asset.opac_visible_copies aovc ON acp.id = aovc.copy_id
WHERE acn.record = %s
''', (rec_id,))

    for (copy_id, barcode, status, circ_lib, location, call_number) in cur:
        logging.debug([copy_id, barcode, status, circ_lib, location, call_number])
        holdings.append({'barcode': barcode, 'status': status, 'circ_lib': circ_lib, 'location': location, 'call_number': call_number})

    return holdings


if (xml_filename):
    # Index records from XML file
    collection_dom = ET.parse(xml_filename)

    collection = collection_dom.getroot()

    for record in collection:
        mods = transform(record)
        output = index_mods(mods)
        output['id'] = get_901c(record)
        output['create_date'] = None
        output['edit_date'] = None
        insert_to_elasticsearch(output)
else:
    # Index records from database
    egdbcfg = config['evergreen_db']
    egconn = psycopg2.connect(dbname=egdbcfg['dbname'], user=egdbcfg['user'], password=egdbcfg['password'], host=egdbcfg['host'], port=egdbcfg['port'])
    egcur = egconn.cursor()
    max_update_date = get_max_update()
    egcur.execute('''
WITH visible AS (
SELECT record
FROM asset.opac_visible_copies aovc
WHERE circ_lib IN (SELECT id FROM actor.org_unit_descendants(22))
UNION
SELECT record
FROM asset.call_number acn
WHERE label = '##URI##' AND owning_lib IN (SELECT id FROM actor.org_unit_descendants(22))
AND NOT acn.deleted
)
SELECT bre.id, bre.marc, (bre.create_date at time zone 'UTC')::timestamp, (bre.edit_date at time zone 'UTC')::timestamp
FROM biblio.record_entry bre
JOIN visible ON visible.record = bre.id
WHERE NOT bre.deleted
AND bre.active
AND (%s IS NULL OR (bre.edit_date at time zone 'UTC')::timestamp >= %s)
ORDER BY bre.edit_date ASC, bre.id ASC
''', (max_update_date, max_update_date))
    for (bre_id, marc, create_date, edit_date) in egcur:
        record = ET.fromstring(marc)
        mods = transform(record)
        output = index_mods(mods)
        output['id'] = bre_id
        output['create_date'] = create_date
        output['edit_date'] = edit_date
        output['holdings'] = index_holdings(egconn, bre_id)
        logging.debug(repr(output))
        insert_to_elasticsearch(output)
