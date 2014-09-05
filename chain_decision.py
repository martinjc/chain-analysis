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


class ChainNotFoundError(RuntimeError):
    def __init__(self, venue_id):
        self.venue_id = venue_id
        
    def __str__( self ):
        return "Chain not found for venue: %s" % ( self.venue_id )


class ChainDecider():


    """
    ChainDecider is used to take any given Foursquare venue and decide
    whether it belongs to a chain or not. 
    """

    def __init__(self, t_distance=0.5, u_distance=0.5, sim_threshold=0.9, search_distance_m=5000):

        self.cache = MongoDBCache(db='foursq')
        self.vs = VenueSearcher()

        self.t_distance = t_distance
        self.u_distance = u_distance
        self.sim_threshold = sim_threshold
        self.search_distance_m = search_distance_m

    def is_chain(self, venue_id):

        """
        Decides if the venue belongs to a chain.
        If so, True and the chain_id are returned. 
        If the venue does not belong to any known chain, a 
        chain_id is created, and False is returned
        """

        venue_data = self.vs.get_venue_json(venue_id)

        chain_id = None
        # compare to all known chains
        chain_id = self.find_chain(venue_id)

        num_in_chain = 0

        # if no chain but is chain according to foursquare, create a new one
        if chain_id is None and vs.venue_has_chain_property(venue_data):
            chain_id = uuid.uuid4().hex
            self.add_to_chain(v1['id'], chain_id)
            return False, None, 1
        else:
            # check the size of the chain
            if self.cache.document_exists('chains', {'_id': chain_id}):
                chain_data = self.cache.get_document('chains', {'_id': chain_id})
                num_in_chain = len(chain_data['venues'])
            if num_in_chain > 1:
                return True, chain_id, num_in_chain
            else:
                return False, None, 1



    def find_chain(self, venue_id):
        """
        Attempts to locate the chain the venue belongs to
        """
        venue_data = self.vs.get_venue_json(venue_id)

        chain_id = None
        # check we don't already know which chain it belongs to
        chain_id = self.check_chain_lookup(venue_id)
        if chain_id is None:
            # check the venue isn't the same as any we already know about
            chain_id = self.exact_compare_to_cache(venue_id)
            if chain_id is None:
                # check the venue isn't similar to any we already know about
                chain_id = self.fuzzy_compare_to_whole_cache(venue_id)
                if chain_id is None:
                    # check the venue exactly against results from a global search
                    chain_id = self.global_chain_check(venue_id)
                    if chain_id is None:
                        # check the venue similarity against results from a global search
                        chain_id = self.fuzzy_global_chain_check(venue_id)
                        if chain_id is None:
                            # check the venue exactly against results from a local search
                            chain_id = self.local_chain_check(venue_id)
                            if chain_id is None:
                                # check the venue similarity against results from a local search
                                chain_id = self.fuzzy_local_chain_check(venue_id)
        return chain_id


    def add_to_chain(self, venue_id, chain_id):
        """
        Add a venue to the given chain
        """
        # check to see if the chain exists. If not, create it
        if self.cache.document_exists('chains', {'_id': chain_id}):
            chain_data = self.cache.get_document('chains', {'_id': chain_id})
            # if the venue isn't already in the chain, add it
            if not venue_id in chain_data['venues']:
                chain_data['venues'].append(venue_id)
                self.cache.put_document('chains', chain_data)
        else:
            chain_data = {'_id': chain_id, 'venues': [venue_id]}
            self.cache.put_document('chains', chain_data)

        # add the chain to the lookup document for the venue
        if not self.cache.document_exists('chain_id_lookup', {'_id': venue_id}):
            self.cache.put_document('chain_id_lookup', {'_id': venue_id, 'chain': chain_id})
        else:
            venue_lookup = self.cache.get_document('chain_id_lookup', {'_id': venue_id})
            venue_lookup['chain'] = chain_id
            self.cache.put_document('chain_id_lookup', venue_lookup)


    def check_chain_lookup(self, venue_id):
        """
        checks for a venue lookup document to see if the venue has already
        been assigned to a chain
        """
        chain_id = None
        if self.cache.document_exists('chain_id_lookup', {'_id': venue_id}):
            chain_id = self.cache.get_document('chain_id_lookup', {'_id': venue_id})['chain']

        # ensure all is as it should be
        self.add_to_chain(venue_id, chain_id)
        return chain_id


    def calc_chain_distance(self, v1, v2):

        """
        calculates distance between two venues by comparing names, 
        twitter handles and URLs
        """
        v2 = v2['response']['venue']

        #levenshtein distance of names
        name_distance = ratio(v1['name'], v2['name'])
        url_distance = 0
        twitter_distance = 0

        # compare URLs
        if v1.get('url') and v2.get('url'):
            if v1['url'] or v2['url']:
                if v1['url'] == v2['url'] and v1['url']:
                    url_distance = self.u_distance

        # compare Twitter handles
        if v1.get('contact') and v2.get('contact'):
            if v1['contact'].get('twitter') and v2['contact'].get('twitter'):
                if v1['contact']['twitter'] or v2['contact']['twitter']:
                    if v1['contact']['twitter'] == v2['contact']['twitter'] and v1['contact']['twitter']:
                        twitter_distance = self.t_distance

        return name_distance, url_distance, twitter_distance


    def exact_compare_to_cache(self, venue_id):
        """
        Checks for any venues in the cache with exactly the same name, URL and
        Twitter handle as this venue. If it finds any, it adds this venue to 
        the chain
        """

        v1 = self.vs.get_venue_json(venue_id)

        # first, look for exact matches
        query = {'response.venue.name': v1['name']}

        # see if we can match URL and/or Twitter handle too
        if v1.get('url'):
            query['response.venue.url'] = v1['url']
        if v1.get('contact'):
            if v1['contact'].get('twitter'):
                query['response.venue.contact.twitter'] = v1['contact']['twitter']

        # check the cache based on assembled query
        venues = self.cache.get_documents('venues', query)

        # see if any returned venues belong to a chain - if so add this venue to that chain
        for venue in venues:
            chain_id = self.check_chain_lookup(venue['response']['venue']['id'])
            if chain_id is not None:
                self.add_to_chain(v1['id'], chain_id)
                return chain_id
        return None


    def fuzzy_compare_to_whole_cache(self, venue_id):
        """
        Checks for any venues in the cache with similar name, or same URL and
        Twitter handle as this venue. If it finds any, it adds this venue to 
        the chain        
        """

        v1 = self.vs.get_venue_json(venue_id)

        for v2 in self.cache.get_collection('venues').find():

            name_distance, url_distance, twitter_distance = self.calc_chain_distance(v1, v2)
            total_distance = name_distance + url_distance + twitter_distance

            if total_distance >= self.sim_threshold:
                chain_id = self.check_chain_lookup(v2['response']['venue']['id'])

                if chain_id is None:                    
                    chain_id = uuid.uuid4().hex
                    self.add_to_chain(v2['response']['venue']['id'], chain_id)

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

            if total_distance >= self.sim_threshold:
                chain_id = self.check_chain_lookup(v2['response']['venue']['id'])

                if chain_id is None:                    
                    chain_id = uuid.uuid4().hex
                    self.add_to_chain(v2['response']['venue']['id'], chain_id)

                self.add_to_chain(v1['id'], chain_id)
                return chain_id
        return None


    def local_chain_check(self, venue_id):

        v1 = self.vs.get_venue_json(venue_id)

        chain_id = None
        local_venues = vs.local_search(v1, v1['name'], self.search_distance_m)
        for venue in local_venues:
            chain_id = self.check_chain_lookup(venue['response']['venue']['id'])
            if chain_id is not None:
                self.add_to_chain(v1['id'], chain_id)

        return chain_id

    def fuzzy_local_chain_check(self, venue_id):
        v1 = self.vs.get_venue_json(venue_id)

        chain_id = None
        local_venues = vs.local_search(v1, v1['name'], self.search_distance_m)

        for v2 in local_venues:
            name_distance, url_distance, twitter_distance = self.calc_chain_distance(v1, v2)
            total_distance = name_distance + url_distance + twitter_distance

            if total_distance >= self.sim_threshold:
                chain_id = self.check_chain_lookup(v2['response']['venue']['id'])

                if chain_id is None:                    
                    chain_id = uuid.uuid4().hex
                    self.add_to_chain(v2['response']['venue']['id'], chain_id)

                self.add_to_chain(v1['id'], chain_id)
                return chain_id
        return None



if __name__ == "__main__":

    starbucks1 = '526903fb11d2cd6a3c51e1ff'
    northcliffe = '5030ef53e4b0beacbee84cef'
    starbucks2 = '5315d2d211d2c227cf2a7037'
    mcdonalds = '4b6d80baf964a520b8782ce3'
    tesco = '4c14b6aea1010f479fd94c18'
    costa = '4db656b50cb6729b6ab71531'

    cd = ChainDecider()
    #print "northcliffe: %s,%s,%s" % (cd.is_chain(northcliffe))
    print "starbucks1: %s,%s,%s" % (cd.is_chain(starbucks1))
    #print "tesco: %s,%s,%s" % (cd.is_chain(tesco))
    #print "mcdonalds: %s,%s,%s" % (cd.is_chain(mcdonalds))
    #print "costa: %s,%s,%s" % (cd.is_chain(costa))



