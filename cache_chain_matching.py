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

from urlparse import urlparse

from db_cache import MongoDBCache
from chain_manager import ChainManager
from category_utils import CategoryTree

from venue_match import calc_venue_match_confidence, find_best_venue_match
from chain_match import calc_chain_match_confidence, find_best_chain_match

class CacheChainMatcher():

    def __init__(self, db_name='fsqexp', required_chain_confidence=0.9, required_venue_confidence=0.9):

        self.cache = MongoDBCache(db=db_name)
        self.cm = ChainManager(db_name=db_name)
        self.ct = CategoryTree()

        self.venues = self.extract_venue_information()
        self.required_venue_confidence = required_venue_confidence
        self.required_chain_confidence = required_chain_confidence

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
                if v['contact'].get('twitter') and v['contact']['twitter'] != 'none':
                    venue['contact']['twitter'] = v['contact']['twitter']
                if v['contact'].get('facebook')and v['contact']['facebook'] != 'none':
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

    def check_chain_lookup(self, venue):
        """
        Checks for a venue lookup document to see if the venue has already
        been assigned to a chain
        """

        # just need the venue data, not the whole API response
        if venue.get('response'):
            venue = venue['response']['venue']

        chain_id = None
        print 'checking chain lookup for %s' % (venue['name'])
        if self.cache.document_exists('chain_id_lookup', {'_id': venue['id']}):
            chain_id = self.cache.get_document('chain_id_lookup', {'_id': venue['id']})['chain_id']

        return chain_id

    def check_existing_chains(self, venue):
        """
        Check all existing chains to see if this venue should be added to one of them
        """

        # just need the venue data, not the whole API response
        if venue.get('response'):
            venue = venue['response']['venue']

        print 'checking existing chains for %s' % (venue['name'])
        # get all existing chains
        chains = self.cache.get_collection('chains').find()
        # find the best match
        best_match, confidence = find_best_chain_match(venue, chains)

        if confidence >= self.required_chain_confidence:
            print 'cec found match %s for %s, confidence %f' % (best_match['names'], venue['name'], confidence)
            self.cm.add_to_chain(best_match['_id'], venue, confidence)
            return best_match['_id']
        else:
            return None

    def recursive_chain_comparison(self, venues):

        print 'recursing - chains'

        if len(venues) < 1:
            return None

        # one venue to compare against all others
        venue = venues.pop(0)

        if self.check_chain_lookup(venue) is None:
            # check existing chains
            chain_id = self.check_existing_chains(venue)
            if chain_id is None:
                # check all others to see if they're in a chain
                chains = [self.check_chain_lookup(v) for v in venues]
                # if they are, get the chain details and find the best match
                candidates = [self.cache.get_document('chains', {'_id': c['_id']}) for c in chains if c is not None]
                chain, confidence = find_best_chain_match(venue, candidates)
                if chain is not None and confidence >= self.required_chain_confidence:
                    print 'rcm found match %s for %s, confidence %f' % (chain['names'], venue['name'], confidence)
                    self.cm.add_to_chain(chain['_id'], venue, confidence)
        
        # compare remaining venues
        return self.recursive_chain_comparison(venues)


    def recursive_venue_comparison(self, venues):

        print 'recursing - venues'
        
        if len(venues) < 1:
            return None

        # one venue to compare against the others
        venue = venues.pop(0)
        if self.check_chain_lookup(venue) is None:
            # see if there's a good match
            # check existing chains
            chain_id = self.check_existing_chains(venue)
            if chain_id is None:
                match_venue, confidence = find_best_venue_match(venue, venues)
                if match_venue is not None and confidence > self.required_venue_confidence:
                    print 'rvm found match %s for %s, confidence %f' % (match_venue['name'], venue['name'], confidence)
                    # is it already in a chain?
                    chain_id = self.check_chain_lookup(match_venue)
                    # if so, add this venue
                    if chain_id is not None:
                        self.cm.add_to_chain(chain_id, venue, confidence)
                    # if not, create a new chain
                    else:
                        self.cm.create_chain(venue, match_venue, confidence)

        # compare remaining venues
        return self.recursive_venue_comparison(venues)


    def compare_query_results(self, venue, query):

        # just need the venue data, not the whole API response
        if venue.get('response'):
            venue = venue['response']['venue']

        possible_matches = self.cache.get_documents('venues', query)
        print 'cqr - %d possible matches (%s)' % (possible_matches.count(), query)
        chain_id = None

        if possible_matches.count() > 0:

            # first need to check whether any matches belong to chains
            chains = [self.check_chain_lookup(v) for v in possible_matches]
            candidates = [self.cache.get_document('chains', {'_id': c}) for c in chains if c is not None]

            # find any possible match
            chain, confidence = find_best_chain_match(venue, candidates)

            # we've found a match for this one
            if chain is not None and confidence >= self.required_chain_confidence:
                print 'found match for %s: %s - confidence %f' % (venue['name'], chain['names'], confidence)
                self.cm.add_to_chain(chain['_id'], venue, confidence)
                # check possible matches for the other results
                pm = [v for v in possible_matches]
                self.recursive_chain_comparison(pm)
                return chain['_id']
            else:
                # still check for possible matches for the other results
                pm = [v for v in possible_matches]
                self.recursive_chain_comparison(pm)

            # check to see if the venues could form a new chain
            match_venue, confidence = find_best_venue_match(venue, possible_matches)
            if match_venue is not None and confidence > self.required_venue_confidence:
                # is it already in a chain?
                chain_id = self.check_chain_lookup(match_venue)
                # if so, add this venue
                if chain_id is not None:
                    self.cm.add_to_chain(chain_id, venue, confidence)
                    # check possible matches against themselves
                    pm = [v for v in possible_matches]
                    self.recursive_venue_comparison(pm)
                    return chain_id
                # if not, create a new chain
                else:
                    chain_id = self.cm.create_chain(venue, match_venue, confidence)
                    # just created a new chain, worth checking possible matches again
                    pm = [v for v in possible_matches]
                    self.recursive_chain_comparison(pm)
                    # check possible matches against themselves
                    self.recursive_venue_comparison(pm)
                    return chain_id
            else:
                pm = [v for v in possible_matches]
                self.recursive_venue_comparison(pm)
        return None


    def exact_compare_to_cache(self, venue):
        """
        See if any of the details of the venue match any venues in the database exactly
        """
        chain_id = None

        # first, look for exact matches by name
        query = {'response.venue.name': venue['name']}
        chain_id = self.compare_query_results(venue, query)
        if chain_id is not None:
            return chain_id

        # look for exact matches by url
        if venue.get('url') and venue['url'] and venue['url'] != '' and venue['url'] != 'none':
            query = {'response.venue.url': venue['url']}
            chain_id = self.compare_query_results(venue, query)
            if chain_id is not None:
                return chain_id

        # look for exact matches by twitter
        if venue.get('contact'):
            if venue['contact'].get('twitter'):
                query = {'response.contact.twitter': venue['contact']['twitter']}
                chain_id = self.compare_query_results(venue, query)
                if chain_id is not None:
                    return chain_id

        return None

    def fuzzy_compare_to_cache(self, venue, skip=0):

        max_confidence = 0.0
        best_match = None
        # look at all the other venues that haven't already been compared
        for i, v in enumerate(self.venues[skip+1:]):

            nd, um, sm, cm = calc_venue_match_confidence(venue, v)
            confidence = sum([nd, um, sm])
            if confidence > max_confidence:
                max_confidence = confidence
                best_match = v

        if best_match is not None and max_confidence >= self.required_venue_confidence:
            print 'fz found match %s for %s, confidence %f (%f, %f, %f)' % (best_match['name'], venue['name'], max_confidence, nd, um, sm)
            chain_id = self.check_chain_lookup(best_match)
            if chain_id is not None:
                self.cm.add_to_chain(chain_id, venue, max_confidence)
                return chain_id
            else:
                return self.cm.create_chain(venue, best_match, max_confidence)           


if __name__ == '__main__':

    ccm = CacheChainMatcher()

    # create our own shallow copy of venues list
    venues = ccm.venues[:]
    for i, venue in enumerate(venues):

        chain_id = None
        print '%d: %s - %s' % (i, venue['name'], venue['id'])

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
                    else:
                        print 'fuzzy_compare_to_cache found chain %s' % (chain_id)
                else:
                    print 'exact_compare_to_cache found chain: %s' % (chain_id)    
            else:
                print 'check_existing_chains found chain: %s' % (chain_id)
        else:
            print 'check_chain_lookup found chain: %s' % (chain_id)
