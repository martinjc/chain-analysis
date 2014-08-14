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

from Levenshtein import ratio
from venue_searcher import VenueSearcher
from db_cache import MongoDBCache

class ChainDecider():

    def __init__(self):

        self.cache = MongoDBCache(db='foursq')
        self.vs = VenueSearcher()

    def check_chain_lookup(self, venue_id):

        if self.cache.document_exists('chain_id_lookup', {'_id': venue_id}):
            chain = self.cache.get_document('chain_id_lookup', {'_id': venue_id})['chain']

            if self.cache.document_exists('chains', {'_id': chain}):
                chain_data = self.cache.get_document('chains', {'_id': chain})
                chain_data['venues'].append(venue_id)
                self.cache.put_document('chains', chain_data)

    def calc_chain_distance(v1, v2):

        name_distance = ratio(v1['name'], v2['name'])
        if v1['url'] or v2['url']:
            if v1['url'] == v2['url'] and v1['url']:
                url_distance = 0.5
            else:
                url_distance = 0
        else:
            url_distance = 0
        if v1['twitter'] or v2['twitter']:
            if v1['twitter'] == v2['twitter'] and v1['twitter']:
                twitter_distance = 0.5
            else:
                twitter_distance = 0    
        else:
            twitter_distance = 0

        return name_distance, url_distance, twitter_distance


    def compare_to_cache(self, venue_id):

        v1 = self.vs.get_venue_json(venue_id)

        v2 = self.cache.get_document('venues', {'response.venue.name': v1['name']})
        return v2

        """

        for v2 in self.cache.get_collection('venues').find():

            name_distance, url_distance, twitter_distance = self.calc_chain_distance(v1, v2)
            total_distance = name_distance + url_distance + twitter_distance

            if total_distance >= 0.9:
                chain = self.cache.get_document('chain_id_lookup', {'_id': v2['id']})['chain']
                chain_data = self.cache.get_document('chains', {'_id': chain})
                chain_data['venues'].append(venue_id)
                self.cache.put_document('chains', chain_data)

                chain_id = {'_id': v2['id'], 'chain': chain}
                self.cache.put_document('chain_id_lookup', chain_id)

        """





def is_chain(venue_id):

    vs = VenueSearcher()

    venue_data = vs.get_venue_json(venue_id)

    if vs.venue_has_chain_property(venue_data):
        return True

    global_venues = vs.global_search(venue_data['name'])
    for venue in global_venues:
        print venue['name']

    global_similar_name_count = len(filter(lambda x: ratio(x, venue_data['name']) > 0.95, [venue['name'] for venue in global_venues]))

    print global_similar_name_count

    if len(global_venues) > 1 and global_similar_name_count > 0:
        global_proportion = float(len(global_venues))/global_similar_name_count
    else:
        global_proportion = 0

    print global_proportion

    local_venues = vs.local_search(venue_data, venue_data['name'], 5000)

    for venue in local_venues:
        print venue['name']

    local_similar_name_count = len(filter(lambda x: ratio(x, venue_data['name']) > 0.95, [venue['name'] for venue in local_venues]))

    print local_similar_name_count


    if len(local_venues) > 1 and local_similar_name_count > 0:
        local_proportion = float(len(local_venues))/local_similar_name_count
    else: 
        local_proportion = 0

    print local_proportion

    if global_proportion > 0.9 or local_proportion > 0.9:
        return True
    else:
        return False



if __name__ == "__main__":

    starbucks1 = '4b4ef4dbf964a520a4f726e3'
    northcliffe = '5030ef53e4b0beacbee84cef'
    starbucks2 = '5315d2d211d2c227cf2a7037'
    mcdonalds = '4c41df47520fa5933a41caac'
    tesco = '4c14b6aea1010f479fd94c18'

    cd = ChainDecider()
    print cd.compare_to_cache(starbucks2)



