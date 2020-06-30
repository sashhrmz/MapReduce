#!/usr/bin/python3
from urllib.request import urlopen
from lxml import etree
from sys import stdin

for line in stdin:
    line = line.strip()
    if line == '':
        break

    url = line.split('\t')[0]

    try:
        page_data = urlopen(url).read()
        html = etree.HTML(page_data)

        links = html.xpath('//a/@href')
        for link in links:
            if not link.startswith('http'):
                link = url + link

            link = link.split('#')[0].strip('/')
            print(link + '\t0')
    except: #skip http errors
        pass
    print(url + '\t1')
