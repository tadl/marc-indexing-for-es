#!/usr/bin/env python

# xpath work Based partially on an example from
# http://stackoverflow.com/a/16699042/157515

import logging, logging.config, configparser
import lxml.etree as ET

import psycopg2

logging.config.fileConfig('index-config.ini')
config = configparser.ConfigParser()
config.read('index-config.ini')

dbcfg = config['target_db']

conn = psycopg2.connect(dbname=dbcfg['dbname'], user=dbcfg['user'], password=dbcfg['password'], host=dbcfg['host'], port=dbcfg['port'])

cur = conn.cursor()

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
    'abstract': {
        'xpath': "//mods32:mods/mods32:abstract",
    },
    'physical_description': {
        'xpath': "//mods32:mods/mods32:physicalDescription/mods32:extent",
    }
}

xslt = ET.parse(xsl_filename)
transform = ET.XSLT(xslt)

def insert_to_target(output):
    try:
        cur.execute("INSERT INTO records (id, created_at, updated_at, title, author, abstract, physical_description) VALUES (%s, %s, %s, %s, %s, %s, %s)", (output['id'], output['create_date'], output['edit_date'], output['title'], output['author'], output['abstract'], output['physical_description']))
        conn.commit()
    except psycopg2.IntegrityError:
        logging.warning("Insert failed, deleting then re-inserting")
        conn.rollback()
        cur.execute("DELETE FROM records WHERE id = %s", (output['id'],))
        cur.execute("INSERT INTO records (id, created_at, updated_at, title, author, abstract, physical_description) VALUES (%s, %s, %s, %s, %s, %s, %s)", (output['id'], output['create_date'], output['edit_date'], output['title'], output['author'], output['abstract'], output['physical_description']))
        conn.commit()


def get_max_update():
    cur.execute("SELECT MAX(updated_at) FROM records;")
    result = cur.fetchone()
    max_update = result[0]
    logging.debug('Max update is %s' % max_update)
    return result[0]


def index_record(record):
    output = {}

    match_901 = record.xpath("marc:datafield[@tag='901']/marc:subfield[@code='c']", namespaces=namespace_dict)

    if len(match_901):
        output['id'] = match_901[0].text

    logging.debug('Found record id %s' % output['id'])

    mods = transform(record)

    for index in indexes.keys():
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
            result = ' '.join(r[0].itertext())
        if result:
            output[index] = result
        else:
            output[index] = ''

    return output


if (xml_filename):
    # Index records from XML file
    collection_dom = ET.parse(xml_filename)

    collection = collection_dom.getroot()

    for record in collection:
        output = index_record(record)
        output['create_date'] = None
        output['edit_date'] = None
        insert_to_target(output)
else:
    # Index records from database
    egdbcfg = config['evergreen_db']
    egconn = psycopg2.connect(dbname=egdbcfg['dbname'], user=egdbcfg['user'], password=egdbcfg['password'], host=egdbcfg['host'], port=egdbcfg['port'])
    egcur = egconn.cursor()
    max_update_date = get_max_update()
    egcur.execute('''
WITH visible AS (
SELECT DISTINCT record
FROM asset.opac_visible_copies aovc
WHERE circ_lib IN (SELECT id FROM actor.org_unit_descendants(22))
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
        output = index_record(record)
        output['create_date'] = create_date
        output['edit_date'] = edit_date
        logging.debug(repr(output))
        insert_to_target(output)
