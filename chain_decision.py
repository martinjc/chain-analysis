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

import uuid

from Levenshtein import ratio
from venue_searcher import VenueSearcher
from db_cache import MongoDBCache

class ChainDecider():

    def __init__(self):

        self.cache = MongoDBCache(db='foursq')
        self.vs = VenueSearcher()

    def is_chain(self, venue_id):

        venue_data = self.vs.get_venue_json(venue_id)

        chain_id = None
        chain_id = self.find_chain(venue_id)

        if chain_id is None and vs.venue_has_chain_property(venue_data):
            chain_id = uuid.uuid4().hex
            self.add_to_chain(v1['id'], chain_id)

        return chain_id


    def find_chain(self, venue_id):

        venue_data = self.vs.get_venue_json(venue_id)

        chain_id = None
        chain_id = self.check_chain_lookup(venue_id)
           if chain_id is None:
                chain_id = self.exact_compare_to_cache(venue_id)
                if chain_id is None:
                    chain_id = self.fuzzy_compare_to_whole_cache(venue_id)
                    if chain_id is None:
                        chain_id = self.global_chain_check(venue_id)
                        if chain_id is None:
                            chain_id = self.fuzzy_global_chain_check(venue_id)
                            if chain_id is None:
                                chain_id = self.local_chain_check(venue_id)
                                if chain_id is None:
                                    chain_id = self.fuzzy_local_chain_check(venue_id)
        return chain_id




    
    def add_to_chain(self, venue_id, chain_id):
        if self.cache.document_exists('chains', {'_id': chain_id}):
            chain_data = self.cache.get_document('chains', {'_id': chain_id})
            if not venue_id in chain_data['venues']:
                chain_data['venues'].append(venue_id)
                self.cache.put_document('chains', chain_data)
        else:
            chain_data = {'_id': chain_id, 'venues' = [venue_id]}
            self.cache.put_document('chains', chain_data)

        if not self.cache.document_exists('chain_id_lookup', {'_id': venue_id}):
            self.cache.put_document('chain_id_lookup', {'_id': venue_id, 'chain': chain_id})
        else:
            venue_lookup = self.cache.get_document('chain_id_lookup', {'_id': venue_id})
            venue_lookup['chain'] = chain_id
            self.cache.put_document('chain_id_lookup', venue_lookup)



    def check_chain_lookup(self, venue_id):

        chain_id = None
        if self.cache.document_exists('chain_id_lookup', {'_id': venue_id}):
            chain_id = self.cache.get_document('chain_id_lookup', {'_id': venue_id})['chain']

        # ensure all is as it should be
        self.add_to_chain(venue_id, chain_id)
        return chain_id


    def calc_chain_distance(v1, v2):

        name_distance = ratio(v1['name'], v2['name'])

        if v1.get('url') and v2.get('url'):
            if v1['url'] or v2['url']:
                if v1['url'] == v2['url'] and v1['url']:
                    url_distance = 0.5
                else:
                    url_distance = 0
            else:
                url_distance = 0
        if v1.get('contact') and v2.get('contact'):
            if v1['contact'].get('twitter') and v2['contact'].get('twitter'):
                if v1['contact']['twitter'] or v2['contact']['twitter']:
                    if v1['contact']['twitter'] == v2['contact']['twitter'] and v1['contact']['twitter']:
                        twitter_distance = 0.5
                    else:
                        twitter_distance = 0    
                else:
                    twitter_distance = 0

        return name_distance, url_distance, twitter_distance


    def exact_compare_to_cache(self, venue_id):

        v1 = self.vs.get_venue_json(venue_id)

        # first, look for exact matches
        query = {'response.venue.name': v1['name']}

        if v1.get('url'):
            query['response.venue.url'] = v1['url']
        if v1.get('contact'):
            if v1['contact'].get('twitter'):
                query['response.venue.contact.twitter'] = v1['contact']['twitter']

        venues = self.cache.get_documents('venues', query)

        for venue in venues:
            chain_id = self.check_chain_lookup(venue['response']['venue']['id'])
            if chain_id is not None:
                self.add_to_chain(v1['id'], chain_id)
                return chain_id
        return None
            
            
    def fuzzy_compare_to_whole_cache(self, venue_id):

        v1 = self.vs.get_venue_json(venue_id)

        for v2 in self.cache.get_collection('venues').find():

            name_distance, url_distance, twitter_distance = self.calc_chain_distance(v1, v2)
            total_distance = name_distance + url_distance + twitter_distance

            if total_distance >= 0.9:
                chain_id = self.check_chain_lookup(v2['id'])

                if chain_id is None:                    
                    chain_id = uuid.uuid4().hex

                self.add_to_chain(v1['id'], chain_id)
                return chain_id
        return None


    def global_chain_check(self, venue_id):

        v1 = self.vs.get_venue_json(venue_id)

        chain_id = None
        global_venues = vs.global_search(v1['name'])
        for venue in global_venues:
            chain_id = self.check_chain_lookup(venue['response']['venue']['id'])
            if chain_id is not None:
                self.add_to_chain(v1['id'], chain_id)

        return chain_id


    def fuzzy_global_chain_check(self, venue_id):
        v1 = self.vs.get_venue_json(venue_id)

        chain_id = None
        global_venues = vs.global_search(v1['name'])

        for v2 in global_venues:
            name_distance, url_distance, twitter_distance = self.calc_chain_distance(v1, v2)
            total_distance = name_distance + url_distance + twitter_distance

            if total_distance >= 0.9:
                chain_id = self.check_chain_lookup(v2['id'])

                if chain_id is None:                    
                    chain_id = uuid.uuid4().hex

                self.add_to_chain(v1['id'], chain_id)
                return chain_id
        return None


    def local_chain_check(self, venue_id):

        v1 = self.vs.get_venue_json(venue_id)

        chain_id = None
        local_venues = vs.local_search(v1, v1['name'], 5000)
        for venue in local_venues:
            chain_id = self.check_chain_lookup(venue['response']['venue']['id'])
            if chain_id is not None:
                self.add_to_chain(v1['id'], chain_id)

        return chain_id

    def fuzzy_local_chain_check(self, venue_id):
        v1 = self.vs.get_venue_json(venue_id)

        chain_id = None
        local_venues = vs.local_search(v1, v1['name'], 5000)

        for v2 in local_venues:
            name_distance, url_distance, twitter_distance = self.calc_chain_distance(v1, v2)
            total_distance = name_distance + url_distance + twitter_distance

            if total_distance >= 0.9:
                chain_id = self.check_chain_lookup(v2['id'])

                if chain_id is None:                    
                    chain_id = uuid.uuid4().hex

                self.add_to_chain(v1['id'], chain_id)
                return chain_id
        return None


def is_chain(venue_id):

    vs = VenueSearcher()

    venue_data = vs.get_venue_json(venue_id)

    if vs.venue_has_chain_property(venue_data):
        return True




if __name__ == "__main__":

    starbucks1 = '4b4ef4dbf964a520a4f726e3'
    northcliffe = '5030ef53e4b0beacbee84cef'
    starbucks2 = '5315d2d211d2c227cf2a7037'
    mcdonalds = '4c41df47520fa5933a41caac'
    tesco = '4c14b6aea1010f479fd94c18'

    cd = ChainDecider()
    print cd.exact_compare_to_cache(northcliffe)



