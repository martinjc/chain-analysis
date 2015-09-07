#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
# Copyright 2014 Martin J Chorley
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import sys
import csv
import json
import codecs
import itertools

from collections import defaultdict
from Levenshtein import ratio
from urlparse import urlparse

from db_cache import MongoDBCache
from venue_match import get_min_venue_from_csv
from chain_manager import ChainManager
from chain_match import find_best_chain_match

csv_reader = csv.DictReader(open('min_venues.csv', 'r'))  #, 'utf-8'))

names = set()
name_count = 0
name_ids = defaultdict(list)

urls = defaultdict(list)
url_names = defaultdict(list)
url_count = 0

twitter = defaultdict(list)
twitter_names = defaultdict(list)
twitter_count = 0

facebook = defaultdict(list)
facebook_names = defaultdict(list)
facebook_count = 0

# find all the unique names, urls, twitter handles and facebook pages
for i, v in enumerate(csv_reader):

    if i % 10000 == 0:
        print(i)

    venue = get_min_venue_from_csv(v)
    names.add(venue['name'])
    name_count += 1
    name_ids[venue['name']].append(venue['id'])

    if venue.get('url'):
        url = urlparse(venue['url']).netloc.lstrip('www.')
        url_count += 1
        urls[url].append(venue['id'])
        url_names[url].append(venue['name'])

    if venue.get('contact'):
        if venue['contact'].get('twitter'):
            t = venue['contact']['twitter']
            twitter_count += 1
            twitter[t].append(venue['id'])
            twitter_names[t].append(venue['name'])

        if venue['contact'].get('facebook'):
            f = venue['contact']['facebook']
            facebook_count += 1
            facebook[f].append(venue['id'])
            facebook_names[f].append(venue['name'])

print('%d venues' % (name_count))
print('%d unique names' % len(names))

print('%d urls' % (url_count))
print('%d unique urls' % (len(urls.keys())))

print('%d twitter handles' % (twitter_count))
print('%d unique twitter handles' % (len(twitter.keys())))

print('%d facebook pages' % (facebook_count))
print('%d unique facebook pages' % (len(facebook.keys())))

with open('name_ids.json', 'w') as n_file:
    json.dump(name_ids, n_file)

with open('urls.json', 'w') as u_file:
    json.dump({'urls': urls}, u_file)

with open('url_names.json', 'w') as u_file:
    json.dump(url_names, u_file)

with open('twitter.json', 'w') as twitter_file:
    json.dump(twitter, twitter_file)

with open('twitter_names.json', 'w') as t_file:
    json.dump(twitter_names, t_file)

with open('fb.json', 'w') as fb_file:
    json.dump(facebook, fb_file)

with open('facebook_names.json', 'w') as f_file:
    json.dump(facebook_names, f_file)

# ChainManager handles chain operations

cache = MongoDBCache(db='fsqexp')
cm = ChainManager(db_name='fsqexp')

csv_reader = csv.DictReader(open('min_venues.csv', 'r'))

for i, v in enumerate(csv_reader):

    if i % 10000 == 0:
        print(i)

    chains = cache.get_collection('chains')

    venue = get_min_venue_from_csv(v)

    url = None
    if venue.get('url'):
        url = urlparse(venue['url']).netloc.lstrip('www.')

    t = None
    f = None
    if venue.get('contact'):
        if venue['contact'].get('twitter'):
            t = venue['contact']['twitter']

        if venue['contact'].get('facebook'):
            f = venue['contact']['facebook']

    if url or t or f:
        if len(urls[url]) > 1 or len(twitter[t]) > 1 or len(facebook[f]) > 1:
            found = False
            for chain in chains:
                if url in chain['urls'] or twitter in chain['twitter'] or facebook in chain['facebook']:
                    chain_id = chain['_id']
                    cm.add_to_chain(chain_id, [venue])
                    found = True
                    break
            if not found:
                cm.create_chain([venue])    


