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

import csv

from decorators import venue_response
from db_cache import MongoDBCache
from chain_decision import ChainDecider
from venue_searcher import VenueSearcher



class LocalComparison():

    def __init__(self):

        self.vs = VenueSearcher()
        self.cd = ChainDecider()
        self.db = MongoDBCache(db='fsqexp')

    def get_venue_ids(self):
        venues = []
        # get all the venues from the database
        db_venues = self.db.get_collection('venues').find(timeout=False)
        
        # extract information needed for comparison
        for v in db_venues:
            # just work with venue information instead of whole response
            if v.get('response'):
                v = v['response']['venue']  
                
            venues.append(v['id'])
        return venues

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
    
    distances = [50, 100, 250, 500]
    
    data_fields = ['venue_id', 'venue_name', 'chain']
    for distance in distances:
        data_fields.append('%d_indie_names' % distance)
        data_fields.append('%d_indie_ids' % distance)
        data_fields.append('%d_chain_names' % distance)
        data_fields.append('%d_chain_ids' % distance)

    

    with open('chain_indie_data.csv', 'w') as output_file:

        writer = csv.DictWriter(output_file, data_fields)
        writer.writeheader()

        lc = LocalComparison()
        venues = lc.get_venue_ids()

        for venue in venues[:1]:

            v = lc.vs.get_venue_json(venue)
            
            data = dict.fromkeys(data_fields, "")
            data['venue_id'] = v['response']['venue']['id']
            data['venue_name'] = v['response']['venue']['name']
            data['chain'] = lc.cd.is_chain(v)

            for distance in distances:
                c_a, i_a = lc.local_comparison(v, distance)
                data['%d_chain_names' % distance] = [alt['response']['venue']['name'] for alt in c_a if alt.get('response')]
                data['%d_chain_ids' % distance] = [alt['response']['venue']['id'] for alt in c_a if alt.get('response')]
                data['%d_indie_names' % distance] = [alt['response']['venue']['name'] for alt in i_a if alt.get('response')]
                data['%d_indie_ids' % distance] = [alt['response']['venue']['id'] for alt in i_a if alt.get('response')]

            writer.writerow(data)


