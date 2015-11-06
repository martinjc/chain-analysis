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

import uuid
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

class Chain:

    def __init__(self):

        self.venues = set()
        self.id = uuid.uuid4().hex

    def __to_dict__(self):
        return {'id': self.id, 'venues': list(self.venues)}

    def __repr__(self):
        return json.dumps({'id': self.id, 'venues': list(self.venues)})

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


# cache = MongoDBCache(db='fsqexp')
# cm = ChainManager(db_name='fsqexp')
chains = {}

print 'name'

for name, vs in name_ids.iteritems():
    if len(vs) > 1:
        if name is not " ":
            print name, len(vs)
            chain = Chain()
            chain.venues = chain.venues | set(vs)
            chains[chain.id] = chain
            for v in vs:
                chain_lookup[v] = chain.id

print 'urls'

for u, vs in url_ids.iteritems():
    if len(vs) > 1:
        print u, len(vs)
        cchains = set()
        for v in vs:
            if chain_lookup.get(v):
                cchains.add(chain_lookup[v])
        if len(cchains) > 1:
            print 'more than one chain!'
        if len(cchains) >  0:
            chain = chains[list(cchains)[0]]
            chain.venues = chain.venues | set(vs)
        else:
            chain = Chain()
            chain.venues = chain.venues | set(vs)
            chains[chain.id] = chain
        for v in vs:
            chain_lookup[v] = chain.id

print 'twitter'

for t, vs in twitter_ids.iteritems():
    if len(vs) > 1:
        print t, len(vs)
        cchains = set()
        for v in vs:
            if chain_lookup.get(v):
                cchains.add(chain_lookup[v])
        if len(cchains) > 1:
            print 'more than one chain!'
        if len(cchains) >  0:
            chain = chains[list(cchains)[0]]
            chain.venues = chain.venues | set(vs)
        else:
            chain = Chain()
            chain.venues = chain.venues | set(vs)
            chains[chain.id] = chain
        for v in vs:
            chain_lookup[v] = chain.id

print 'facebook'

for f, vs in facebook_ids.iteritems():
    if len(vs) > 1:
        print f, len(vs)
        cchains = set()
        for v in vs:
            if chain_lookup.get(v):
                cchains.add(chain_lookup[v])
        if len(cchains) > 1:
            print 'more than one chain!'
        if len(cchains) >  0:
            chain = chains[list(cchains)[0]]
            chain.venues = chain.venues | set(vs)
        else:
            chain = Chain()
            chain.venues = chain.venues | set(vs)
            chains[chain.id] = chain
        for v in vs:
            chain_lookup[v] = chain.id

  
with open('chain_lookup_with_names.json', 'w') as chain_file:
    json.dump(chain_lookup, chain_file)

chains = [c.__to_dict__() for c in chains.values()]
with open('simple_chains_with_names.json', 'w') as chain_file:
    json.dump(chains, chain_file)

