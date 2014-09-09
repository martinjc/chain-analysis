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
from category_utils import CategoryTree
from venue_searcher import VenueSearcher
from venue_chain_distance import calc_chain_distance


class CacheChainMatcher():

    def __init__(self, db_name='fsqexp', required_confidence=0.9):

        self.cache = MongoDBCache(db=db_name)
        self.vs = VenueSearcher(db_name=db_name)
        self.cm = ChainManager(db_name=db_name)
        self.required_confidence = required_confidence
        self.ct = CategoryTree()

        self.no_matches = []
        self.matched = []

        self.venues = self.extract_venue_information()

        print('done init()')


    def extract_venue_information(self):

        venues = []
        # get all the venues from the database
        db_venues = self.cache.get_collection('venues').find(timeout=False)
        
        # extract information needed for comparison
        for v in db_venues:
            # just work with venue information instead of whole response
            if v.get('response'):
                v = v['response']['venue']

            venue = {}
            venue['id'] = v['id']
            venue['name'] = v['name']
            # check for url
            if v.get('url'):
                # parse url now
                venue['url'] = urlparse(v['url']).netloc
            # check for social media info
            if v.get('contact'):
                venue['contact'] = {}
                if v['contact'].get('twitter'):
                    if v['contact']['twitter'] != 'none':
                        venue['contact']['twitter'] = v['contact']['twitter']
                if v['contact'].get('facebook'):
                    venue['contact']['facebook'] = v['contact']['facebook']
            # copy any categories
            venue['categories'] = []            
            if v.get('categories'):
                venue['categories'] = v['categories']

            # don't include homes or residences
            if v.get('categories'):
                if len(v['categories']) > 0:
                    # check the first (primary) category
                    root_category = self.ct.get_root_node_for_id(v['categories'][0]['id'])
                    # if the root is not 'Homes and Residences'
                    if root_category is not None and root_category['foursq_id'] != "4e67e38e036454776db1fb3a":
                        venues.append(venue)
                # add venues with no categories
                else:
                    venues.append(venue)
            # add venues with no categories
            else:
                venues.append(venue)

        return venues


    def calc_venue_match_confidence(self, venue1, venue2):

        """
        calculates distance between two venues by comparing names, 
        social media handles, URLs and categories
        """

        # just need the venue data, not the whole API response
        if venue1.get('response'):
            v1 = venue1['response']['venue']
        else:
            v1 = venue1
        if venue2.get('response'):
            v2 = venue2['response']['venue']
        else:
            v2 = venue2

        #levenshtein distance of names
        name_distance = ratio(v1['name'], v2['name'])
        url_match = 0.0
        social_media_match = 0.0
        category_match = 0.0

        # compare URLs
        if v1.get('url') and v2.get('url'):
            if v1['url'] == v2['url']:
                url_match = 1.0

        # compare social media
        if v1.get('contact') and v2.get('contact'):
            if v1['contact'].get('twitter') and v2['contact'].get('twitter'):
                if v1['contact']['twitter'] == v2['contact']['twitter'] and v1['contact']['twitter'] and v1['contact']['twitter'] != "none":
                    social_media_match += 1.0
            if v1['contact'].get('facebook') and v2['contact'].get('facebook'):
                if v1['contact']['facebook'] == v2['contact']['facebook'] and v1['contact']['facebook'] and v1['contact']['facebook'] != "none":
                    social_media_match += 1.0

        # compare categories
        if v1.get('categories') and v2.get('categories'):
            for category1 in v1['categories']:
                for category2 in v2['categories']:
                    if category1['id'] == category2['id']:
                        category_match += 1.0

        return name_distance, url_match, social_media_match, category_match


    def add_no_match(self, venue):

        # just need the venue data, not the whole API response
        if venue.get('response'):
            v = venue['response']['venue']
        else:
            v = venue

        self.no_matches.append(v['id'])


    def add_match(self, venue):

        # just need the venue data, not the whole API response
        if venue.get('response'):
            v = venue['response']['venue']
        else:
            v = venue

        self.matched.append(v['id'])      


    def check_chain_lookup(self, venue):
        """
        Checks for a venue lookup document to see if the venue has already
        been assigned to a chain
        """

        chain_id = None

        if self.cache.document_exists('chain_id_lookup', {'_id': venue['id']}):
            chain_id = self.cache.get_document('chain_id_lookup', {'_id': venue['id']})['chain_id']

        return chain_id


    def find_best_chain(self, venue, chains):
        """
        Search a list of chains for the best match for this venue.
        """

        candidates = {}

        # compute how well the venue matches the chain
        for chain in chains:
            a_r, u_c, sm_c, cat_c = self.compute_confidence(venue, chain['_id'])
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

        chains = self.cache.get_collection('chains').find()
        return self.find_best_chain(venue, chains)


    def check_venue_candidates(self, venue, possible_matches):
        
        if len(possible_matches) == 0:
            return None

        candidates = {}
        chain_id = None
        
        # check match between all possible candidates
        for v in possible_matches:
            candidates[v['id']] = self.calc_venue_match_confidence(venue, v)

        # find the best match
        max_confidence = 0.0
        best_match = None
        for v, confidence in candidates.iteritems():
            if confidence > max_confidence:
                max_confidence = confidence
                best_match = v

        if best_match is not None and max_confidence >= self.required_confidence:
            v = self.cache.get_document('venues', {'_id': best_match})
            chain_id = self.check_chain_lookup(v)
            if chain_id is not None:
                self.cm.add_to_chain(chain_id, venue, max_confidence)
                return chain_id
            else:
                return self.cm.create_chain(venue, v, max_confidence)     
        return chain_id


    def check_venue_candidate_chains(self, venue, possible_matches):
        
        chains = []
        chain_id = None
        
        for v in possible_matches:
            chain_id = self.check_chain_lookup(v)
            if chain_id is not None:
                chains.append(self.cache.get_document('chains', {'_id': chain_id}))

        chain_id = self.find_best_chain(venue, chains)

        return chain_id

    def exact_compare_on_query(self, venue, query):
        # check the cache based on query
        venues = self.cache.get_documents('venues', query)

        # see if any returned venues belong to a chain
        # if so, compute the confidences that this venue should belong to that chain
        chain_id = self.check_venue_candidate_chains(venue, venues)
        if chain_id is not None:
            return chain_id

        # if no chain yet, but venues match, 
        # check to see if they can form a new chain
        chain_id = self.check_venue_candidates(venue, venues)
        if chain_id is not None:
            return chain_id

        return None

    def exact_compare_to_cache(self, venue):
        """
        Checks for any venues in the cache with exactly the same name, URL and
        Twitter handle as this venue. If it finds any, it adds this venue to 
        the chain
        """

        chain_id = None

        # first, look for exact matches by name
        query = {'response.venue.name': v['name']}
        chain_id = self.exact_compare_on_query(venue, query)
        if chain_id is not None:
            return chain_id

        # look for exact matches by url
        if venue.get('url') and venue['url'] and venue['url'] != '' and venue['url'] != 'none':
            query = {'response.venue.url': venue['url']}
            chain_id = self.exact_compare_on_query(venue, query)
            if chain_id is not None:
                return chain_id

        # look for exact matches by twitter
        if venue.get('contact'):
            if venue['contact'].get('twitter'):
                query = {'response.contact.twitter': venue['contact']['twitter']}
                chain_id = self.exact_compare_on_query(venue, query)
                if chain_id is not None:
                    return chain_id

        return None

    def fuzzy_compare_to_cache(self, venue, skip):

        candidates = {}

        # look at all the other venues that haven't already been compared
        for i, v in enumerate(self.venues[skip+1:]):

            # visual sign of how far through we are
            if i % 1000 == 0
                print '.',

            n_d, u_m, sm_m, cat_m = self.calc_venue_match_confidence(venue, v)
            # ignore cat_distance for now
            confidence = sum([n_d, u_m, sm_m])
            if confidence >= self.required_confidence:
                candidates[v['id']] = confidence

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
                self.cm.add_to_chain(chain_id, v1, max_confidence)
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
    ct = CategoryTree()

    # create a shallow copy of venues list
    venues = ccm.venues[:]

    # go through all venues
    for i, venue in enumerate(venues):     
        
        # let us know how far we are through the list
        chain_id = None
        print '%d: %s' % (i, venue['name'])

        # check if the venue is already in a chain
        chain_id = ccm.check_chain_lookup(venue)
        if chain_id == None:
            print 'check_chain_lookup failed'

            # compare the venue against existing chains
            chain_id = ccm.check_existing_chains(venue)
            if chain_id == None:
                print 'check_existing_chains failed'

                # see if any venues in cache match exactly
                chain_id = ccm.exact_compare_to_cache(venue)
                if chain_id == None:
                    print 'exact_compare_to_cache failed'

                    # check the rest of the venues in the cache
                    chain_id = ccm.fuzzy_compare_to_cache(venue, i)
                    if chain_id == None:
                        print 'fuzzy_compare_to_cache failed'
                        ccm.add_no_match(venue)
                    else:
                        print 'fuzzy_compare_to_cache found chain %s' % (chain_id)
                        ccm.add_match(venue)
                else:
                    print 'exact_compare_to_cache found chain %s' % (chain_id)
                    ccm.add_match(venue)
            else:
                print 'check_existing_chains found chain %s' % (chain_id)
                ccm.add_match(venue)
        else:
            print 'check_chain_lookup found chain: %s' % (chain_id)
            ccm.add_match(venue)
        print '%d with no matches' % len(ccm.no_matches)
        print '%d with matches' % len(ccm.matched)


    
