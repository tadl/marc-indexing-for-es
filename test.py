#!/usr/bin/env python

# Based partially on an example from http://stackoverflow.com/a/16699042/157515

import lxml.etree as ET

xml_filename = '46747646.xml'
xsl_filename = 'MARC21slim2MODS3-2.xsl'

dom = ET.parse(xml_filename)
xslt = ET.parse(xsl_filename)
transform = ET.XSLT(xslt)
newdom = transform(dom)
#print(ET.tostring(newdom, pretty_print=True))

namespace_dict = {'mods32': 'http://www.loc.gov/mods/v3'}

indexes = {
    'title': {
        'xpath': "//mods32:mods/mods32:titleInfo[mods32:title and not (@type)]",
    },
    'author': {
        'xpath': "//mods32:mods/mods32:name[@type='personal' and mods32:role/mods32:roleTerm[text()='creator']]",
        'post_xpath': "//*[local-name()='namePart']"
    }
}

for index in indexes.keys():
    xpath = indexes[index]['xpath']
    post_xpath = None
    if 'post_xpath' in indexes[index]:
        post_xpath = indexes[index]['post_xpath']
    r = newdom.xpath(xpath, namespaces=namespace_dict)
    result = None
    if len(r):
        if post_xpath:
            r = r[0].xpath(post_xpath)
        result = ' '.join(r[0].itertext())
    print index, result

