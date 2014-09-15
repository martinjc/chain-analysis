#!/usr/bin/env python
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

from decorators import venue_response
from db_cache import MongoDBCache
from chain_decision import ChainDecider
from venue_searcher import VenueSearcher



class LocalComparison():

    def __init__(self):

        self.vs = VenueSearcher()
        self.cd = ChainDecider()
        self.db = MongoDBCache(db='fsqexp')


    @venue_response
    def local_comparison(self, venue, radius):

        alt_chain_count = 0
        chain_alternates = []
        indie_alternates = []

        alternates = self.vs.search_alternates(venue, radius)
        for alternate in alternates:
            v = self.vs.get_venue_json(alternate['id'])
            if v['id'] != venue['id']:
                chain_id = self.cd.is_chain(v)
                if chain_id is not None:
                    alt_chain_count += 1
                    chain_alternates.append(v)
                else:
                    indie_alternates.append(v)

        return chain_alternates, indie_alternates


if __name__ == '__main__':
    

    lc = LocalComparison()

    cb = lc.vs.get_venue_json('4d00decff7b38cfab4bcd1c3')

    print lc.cd.is_chain(cb)

    c_a, i_a = lc.local_comparison(cb, 500)

    print cb['name']

    print 'chain_alternates:'

    for alt in c_a:
        if alt.get('response'):
            alt = alt['response']['venue']
        print alt['name']

    print 'indie_alternates:'

    for alt in i_a:
        if alt.get('response'):
            alt = alt['response']['venue']
        print alt['name']


