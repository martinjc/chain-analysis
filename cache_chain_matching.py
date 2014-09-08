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

from urlparse import urlparse
from Levenshtein import ratio
from db_cache import MongoDBCache
from chain_manager import ChainManager
from venue_searcher import VenueSearcher
from venue_chain_distance import calc_chain_distance, calc_venue_distance

class CacheChainMatcher():

    def __init__(self, db_name='fsqexp', required_confidence=0.9):

        self.cache = MongoDBCache(db=db_name)
        self.vs = VenueSearcher(db_name=db_name)
        self.cm = ChainManager(db_name=db_name)
        self.required_confidence = required_confidence

    def check_chain_lookup(self, venue):
        """
        checks for a venue lookup document to see if the venue has already
        been assigned to a chain
        """

        # just need the venue data, not the whole API response
        if venue.get('response'):
            v = venue['response']['venue']
        else:
            v = venue

        chain_id = None

        if self.cache.document_exists('chain_id_lookup', {'_id': v['id']}):
            chain_id = self.cache.get_document('chain_id_lookup', {'_id': v['id']})['chain_id']

        return chain_id


    def find_best_chain(self, venue, chains):
        """
        Search a list of chains for the best match for this venue.
        """
        # just need the venue data, not the whole API response
        if venue.get('response'):
            v = venue['response']['venue']
        else:
            v = venue

        candidates = {}

        # compute how well the venue matches the chain
        for chain in chains:
            a_r, u_c, sm_c, cat_c = self.compute_confidence(v, chain['_id'])
            # ignore category confidence for now
            confidence = sum([a_r, u_c, sm_c])
            if confidence >= self.required_confidence:
                candidates[chain['_id']] = confidence

        # find the best match
        max_confidence = 0.0
        best_chain = None
        for chain_id, confidence in candidates.iteritems():
            if confidence >= max_confidence:
                max_confidence = confidence
                best_chain = chain_id

        # if a match is found and confidence is high enough, return the chain_id
        if best_chain is not None and max_confidence >= self.required_confidence:
            self.cm.add_to_chain(best_chain, v, max_confidence)
            return best_chain
        else:
            return None        


    def check_existing_chains(self, venue):

        # just need the venue data, not the whole API response
        if venue.get('response'):
            v = venue['response']['venue']
        else:
            v = venue

        chains = self.cache.get_collection('chains').find()
        return self.find_best_chain(venue, chains)


    def exact_compare_to_cache(self, venue):
        """
        Checks for any venues in the cache with exactly the same name, URL and
        Twitter handle as this venue. If it finds any, it adds this venue to 
        the chain
        """

        # just need the venue data, not the whole API response
        if venue.get('response'):
            v = venue['response']['venue']
        else:
            v = venue

        # first, look for exact matches
        query = {'response.venue.name': v['name']}

        # check the cache based on query
        venues = self.cache.get_documents('venues', query)

        # see if any returned venues belong to a chain
        # if so, compute the confidences that this venue should belong to that chain
        chains = []
        for venue in venues:
            chain_id = self.check_chain_lookup(venue)
            if chain_id is not None:
                chains.append(self.cache.get_document('chains', {'_id': chain_id}))

        return self.find_best_chain(venue, chains)


    def fuzzy_compare_to_cache(self, venue):

        # just need the venue data, not the whole API response
        if venue.get('response'):
            v = venue['response']['venue']
        else:
            v = venue

        v1 = self.vs.get_venue_json(v['id'])

        candidates = {}

        # look at all the other venues
        for v2 in self.cache.get_collection('venues').find():
            if v2.get('response'):
                v2 = v2['response']['venue']
            # make sure we're not comparing the venue against itself
            if v2['id'] != v1['id']:
                # make sure we're not comparing against venues from existing chains
                # (use 'check_existing_chains' for that)
                if self.check_chain_lookup(v2) is None:
                    n_d, u_m, sm_m, cat_m = calc_venue_distance(v1, v2)
                    # ignore cat_distance for now
                    confidence = sum([n_d, u_m, sm_m])
                    if confidence >= self.required_confidence:
                        candidates[v2['id']] = confidence

        # find the best match
        max_confidence = 0.0
        best_match = None
        for v2, confidence in candidates.iteritems():
            if confidence >= max_confidence:
                max_confidence = confidence
                best_match = v2

        # if we have a good match
        if best_match is not None and max_confidence >= self.required_confidence:
            v2 = self.vs.get_venue_json(best_match)
            # check to see if there's a chain already
            chain_id = self.check_chain_lookup(v2)
            if chain_id is not None:
                self.cm.add_to_chain(chain_id, v2, max_confidence)
                return chain_id
            else:
                return self.cm.create_chain(v1, v2, max_confidence)
        else:
            return None


    def compute_confidence(self, venue, chain_id):
        
        assert self.cache.document_exists('chains', {'_id': chain_id})

        # just need the venue data, not the whole API response
        if venue.get('response'):
            v = venue['response']['venue']
        else:
            v = venue

        # get the chain data
        chain = self.cache.get_document('chains', {'_id': chain_id})

        return calc_chain_distance(v, chain)

if __name__ == '__main__':
    
    cache = MongoDBCache(db='fsqexp')
    ccm = CacheChainMatcher()

    venues = cache.get_collection('venues').find()
    for i, venue in enumerate(venues[:10]):
        chain_id = None
        print '%d: %s' % (i, venue['response']['venue']['name'])
        chain_id = ccm.check_chain_lookup(venue)
        if chain_id == None:
            print 'check_chain_lookup failed'
            chain_id = ccm.check_existing_chains(venue)
            if chain_id == None:
                print 'check_existing_chains failed'
                chain_id = ccm.exact_compare_to_cache(venue)
                if chain_id == None:
                    print 'exact_compare_to_cache failed'
                    chain_id = ccm.fuzzy_compare_to_cache(venue)
                    if chain_id == None:
                        print 'fuzzy_compare_to_cache failed'
                    else:
                        print 'fuzzy_compare_to_cache found chain %s' % (chain_id)
                else:
                    print 'exact_compare_to_cache found chain %s' % (chain_id)
            else:
                print 'check_existing_chains found chain %s' % (chain_id)
        else:
            print 'check_chain_lookup found chain: %s' % (chain_id)

    
