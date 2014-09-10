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
from chain_manager import ChainManager, CachedChain
from category_utils import CategoryTree

from venue_match import calc_venue_match_confidence
from chain_match import calc_chain_match_confidence, find_best_chain_match

class CacheChainMatcher():

    def __init__(self, db_name='fsqexp', required_chain_confidence=0.9, required_venue_confidence=0.9):

        self.cache = MongoDBCache(db=db_name)
        self.cm = ChainManager(db_name=db_name)
        self.ct = CategoryTree()

        self.venues = self._extract_venue_information()
        self.required_venue_confidence = required_venue_confidence
        self.required_chain_confidence = required_chain_confidence

    def _extract_venue_information(self):

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
            self.cm.add_to_chain(best_match['_id'], [venue])
            return best_match['_id']
        else:
            return None


    def fuzzy_compare_to_cache(self, venue):

        chain_id = None

        # just need the venue data, not the whole API response
        if venue.get('response'):
            venue = venue['response']['venue'] 

        venue_matches = [venue]

        # look at all the other venues that haven't already been compared
        for i, v in enumerate(self.venues):

            if venue['id'] != v['id']:

                # calculate match with this venue
                nd, um, sm, cm = calc_venue_match_confidence(venue, v)
                confidence = sum([nd, um, sm])
                if confidence > self.required_venue_confidence:
                    venue_matches.append(v)

        # have we found any matches?
        if len(venue_matches) <= 1:
            return None

            # are any matches already in a chain?
            chains = set()
            for v in venue_matches:
                chain_id = self.check_chain_lookup(v)
                if chain_id is not None:
                    chains.add(chain_id)
                    venue_matches.remove(v)

            # creating a new chain
            if len(chains) == 0:
                chain = self.cm.create_chain(venue_matches)
                chain_id = chain.id
            elif len(chains) == 1:
                chain_id = list(chains)[0]
                chain = self.cm.add_to_chain(chain_id, venue_matches)
            else:
                raise RuntimeError

        return chain_id

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

                # check the rest of the venues in the cache
                chain_id = ccm.fuzzy_compare_to_cache(venue)
                if chain_id == None:
                    print 'fuzzy_compare_to_cache failed'
                else:
                    print 'fuzzy_compare_to_cache found chain %s' % (chain_id)
            else:
                print 'check_existing_chains found chain: %s' % (chain_id)
        else:
            print 'check_chain_lookup found chain: %s' % (chain_id)
