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
import simstring
import itertools

from collections import defaultdict
from Levenshtein import ratio
from urlparse import urlparse

from db_cache import MongoDBCache
from venue_match import get_min_venue_from_csv

csv_reader = csv.DictReader(open('min_venues.csv', 'r'))  #, 'utf-8'))

names = set()
name_count = 0

# urls = set()
# url_count = 0

# twitter = set()
# twitter_count = 0

# facebook = set()
# facebook_count = 0

# db = simstring.writer('names.db')

for i, v in enumerate(csv_reader):

    if i % 10000 == 0:
        print(i)

    venue = get_min_venue_from_csv(v)
    names.add(venue['name'])
    name_count += 1

    # if venue.get('url'):
    #     url_count += 1
    #     urls.add(venue['url'])

    # if venue.get('contact'):
    #     if venue['contact'].get('twitter'):
    #         twitter_count += 1
    #         twitter.add(venue['contact']['twitter'])

    #     if venue['contact'].get('facebook'):
    #         facebook_count += 1
    #         facebook.add(venue['contact']['facebook'])

print('%d venues' % (name_count))
print('%d unique names' % len(names))

# print('%d urls' % (url_count))
# print('%d unique urls' % (len(urls)))

# print('%d twitter handles' % (twitter_count))
# print('%d unique twitter handles' % (len(twitter)))

# print('%d facebook pages' % (facebook_count))
# print('%d unique facebook pages' % (len(facebook)))

# for name in names:
#     db.insert(name)
# db.close()

ratios = defaultdict(dict)
with open('ratios.json', 'w') as ratio_file:

    ratios = defaultdict(dict)

    i = 0
    name_pairs = itertools.combinations(names, 2)

    for n1, n2 in name_pairs:

        if i % 1000000 == 0:
            print(i)
        r = ratio(n1, n2)
        if r > 0.7:
            ratios[n1][n2] = r
        i += 1

    json.dump(ratios, ratio_file)

