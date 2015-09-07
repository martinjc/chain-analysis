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

name_ids = defaultdict(list)
url_ids = defaultdict(list)
twitter_ids = defaultdict(list)
facebook_ids = defaultdict(list)

venues = {}
chain_lookup = {}

# find all the unique names, urls, twitter handles and facebook pages
for i, v in enumerate(csv_reader):

    if i % 10000 == 0:
        print(i)

    venue = get_min_venue_from_csv(v)
    venues[venue['id']] = venue
    name_ids[venue['name']].append(venue['id'])

    if venue.get('url'):
        url = urlparse(venue['url']).netloc.lstrip("http://").lstrip('www.').lstrip().rstrip()
        if url is not "":
            url_ids[url].append(venue['id'])

    if venue.get('contact'):
        if venue['contact'].get('twitter'):
            t = venue['contact']['twitter']
            twitter_ids[t].append(venue['id'])

        if venue['contact'].get('facebook'):
            f = venue['contact']['facebook']
            facebook_ids[f].append(venue['id'])

with open('url_check.json', 'w') as urlfile:
    json.dump(url_ids, urlfile)

cache = MongoDBCache(db='fsqexp')
cm = ChainManager(db_name='fsqexp')

print 'urls'

for url, vs in url_ids.iteritems():
    if len(vs) > 1:
        if url is not " ":
            print url, len(vs)
            venue_list = [venues[v] for v in vs]
            print len(venue_list)
            chain = cm.create_chain(venue_list)
            for v in vs:
                chain_lookup[v] = chain.id

print 'twitter'

for t, vs in twitter_ids.iteritems():
    if len(vs) > 1:
        venue_list = [venues[v] for v in vs]
        chains = set()
        for v in vs:
            if chain_lookup.get(v):
                chains.add(chain_lookup[v])
        if len(chains) > 1:
            print 'more than one chain!'
        if len(chains) >  0:
            chain = cm.load_chain(chains[0])
            chain.add_to_chain(chain, venue_list)
        else:
            chain = cm.create_chain(venue_list)
            for v in vs:
                chain_lookup[v] = chain.id

print 'facebook'

for f, vs in facebook_ids.iteritems():
    if len(vs) > 1:
        venue_list = [venues[v] for v in vs]
        chains = set()
        for v in vs:
            if chain_lookup.get(v):
                chains.add(chain_lookup[v])
        if len(chains) > 1:
            print 'more than one chain!'
        if len(chains) > 0:
            chain = cm.load_chain(chains[0])
            chain.add_to_chain(chain, venue_list)
        else:
            chain = cm.create_chain(venue_list)
            for v in vs:
                chain_lookup[v] = chain.id

  


