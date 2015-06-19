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
import codecs

from urlparse import urlparse
from decorators import venue_response

from db_cache import MongoDBCache
from chain_manager import ChainManager, CachedChain
from category_utils import CategoryTree

from venue_match import calc_venue_match_confidence, get_min_venue_from_csv
from chain_match import calc_chain_match_confidence, find_best_chain_match


class CacheChainMatcher():
    """
    Class to match venues to chains or other venues in a cache
    """
    def __init__(self, db_name='fsqexp', required_chain_confidence=0.9, required_venue_confidence=0.95):

        # access to the database
        self.cache = MongoDBCache(db=db_name)

        # read venues from file
        self.csv_reader = csv.DictReader(codecs.open('min_venues.csv', 'r', 'utf-8'))

        # ChainManager handles chain operations
        self.cm = ChainManager(db_name=db_name)
        # category tools
        self.ct = CategoryTree()

        # value we use to decide if two venues should be matched together
        self.required_venue_confidence = required_venue_confidence
        # value we use to decide if a venue should be part of a chain
        self.required_chain_confidence = required_chain_confidence

    @venue_response
    def check_chain_lookup(self, venue):
        """
        Checks for a venue lookup document to see if the venue has already
        been assigned to a chain
        """
        chain_id = None
        if self.cache.document_exists('chain_id_lookup', {'_id': venue['id']}):
            chain_id = self.cache.get_document('chain_id_lookup', {'_id': venue['id']})['chain_id']

        return chain_id

    @venue_response
    def check_existing_chains(self, venue):
        """
        Check all existing chains to see if this venue should be added to one of them
        """

        # get all existing chains
        chains = self.cache.get_collection('chains').find()
        # find the best match
        best_match, confidence = find_best_chain_match(venue, chains)

        if confidence >= self.required_chain_confidence:
            self.cm.add_to_chain(best_match['_id'], [venue])
            return best_match['_id']
        else:
            return None

    @venue_response
    def fuzzy_compare_to_cache(self, venue):

        chain_id = None

        venue_matches = [venue]

        # look at all the other venues that haven't already been compared
        # extract information about all the venues from the database
        # v_copy = self.cache.get_collection('venues').find(timeout=False)
        v_copy = csv.DictReader(codecs.open('min_venues.csv', 'r', 'utf-8'))

        count = 0

        print("starting at %d" % self.i)

        for csv_v in v_copy:

            v = get_min_venue_from_csv(csv_v)

            if count > self.i:

                if venue['id'] != v['id']:

                    # calculate match with this venue
                    nd, um, sm, cm = calc_venue_match_confidence(venue, v)
                    confidence = sum([nd, um, sm, cm])
                    if confidence > self.required_venue_confidence:
                        venue_matches.append(v)
            count += 1

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
        # adding to an existing chain
        elif len(chains) == 1:
            chain_id = list(chains)[0]
            chain = self.cm.add_to_chain(chain_id, venue_matches)
        # find best match out of many chains
        else:
            candidate_chains = [self.cache.get_document('chains', {"_id": chain}) for chain in chains]
            for v in venue_matches:
                chain, confidence = find_best_chain_match(v, candidate_chains)
                if confidence > self.required_chain_confidence:
                    chain_id = chain['_id']
                    chain = self.cm.add_to_chain(chain_id, [v])
        return chain_id

    def do_matching(self):

        # extract information about all the venues from the database
        # self.venues = self.cache.get_collection('venues').find(timeout=False)

        self.i = 0
        for v in self.csv_reader:

            print(v)

            venue = get_min_venue_from_csv(v)

            print(self.i)
            
            chain_id = None
            # check if the venue is already in a chain
            chain_id = self.check_chain_lookup(venue)
            if chain_id is None:
                # compare the venue against existing chains
                chain_id = self.check_existing_chains(venue)
                if chain_id is None:
                    # check the rest of the venues in the cache
                    chain_id = self.fuzzy_compare_to_cache(venue)
            self.i += 1     

if __name__ == '__main__':

    ccm = CacheChainMatcher()
    ccm.do_matching()
    

