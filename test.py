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

title_xpath = '//mods32:mods/mods32:titleInfo[mods32:title and not (@type)]'

r = newdom.xpath(title_xpath, namespaces=namespace_dict)

title = ' '.join(r[0].itertext())

print title

author_xpath = "//mods32:mods/mods32:name[@type='personal' and mods32:role/mods32:roleTerm[text()='creator']]"

r = newdom.xpath(author_xpath, namespaces=namespace_dict)

if len(r):
    r = r[0].xpath("//*[local-name()='namePart']")
    author = ' '.join(r[0].itertext())

    print author
