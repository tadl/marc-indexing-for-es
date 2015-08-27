#!/usr/bin/env python

# Based partially on an example from http://stackoverflow.com/a/16699042/157515

import lxml.etree as ET

xml_filename = 'test-formatted.xml'
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
    }
}

xslt = ET.parse(xsl_filename)
transform = ET.XSLT(xslt)

collection_dom = ET.parse(xml_filename)

collection = collection_dom.getroot()

for record in collection:

    mods = transform(record)

    output = {}

    match_901 = record.xpath("//marc:record/marc:datafield[@tag='901']/marc:subfield[@code='c']", namespaces=namespace_dict)

    print (ET.tostring(match_901[0], pretty_print=True))

    if len(match_901):
        output['id'] = match_901[0].text

    if output['id'] == '1027649':
        print(ET.tostring(mods, pretty_print=True))

    for index in indexes.keys():
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

    print repr(output)
