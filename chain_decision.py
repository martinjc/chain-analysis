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


from chain_manager import ChainManager
from category_utils import CategoryTree
from venue_searcher import VenueSearcher
from cache_chain_matching import CacheChainMatcher

class ChainDecider():
    """
    ChainDecider is used to take any given Foursquare venue and decide
    whether it belongs to a chain or not. 
    """

    def __init__(self):
        self.cm = ChainManager()
        self.vs = VenueSearcher()
        self.ct = CategoryTree()
        self.ccm = CacheChainMatcher()


    def is_home(self, venue):

        # just need the venue data, not the whole API response
        if venue.get('response'):
            venue = venue['response']['venue']

        # don't include homes or residences
        if venue.get('categories'):
            if len(venue['categories']) > 0:
                # check the first (primary) category
                root_category = self.ct.get_root_node_for_id(venue['categories'][0]['id'])
                # if the root exists
                if root_category is not None:
                    # if it's not Homes and Residences
                    if root_category['foursq_id'] != "4e67e38e036454776db1fb3a":
                        return False
                    # if it is Homes and Residences
                    else:
                        return True
                # if the root doesn't exist
                else:
                    return False
            # if the venue has no categories
            else:
                return False
        # if the venue has no categories
        else:
            return False


    def is_chain(self, venue):
        """
        Find out if the venue belongs to a chain
        """

        # just need the venue data, not the whole API response
        if venue.get('response'):
            venue = venue['response']['venue']

        chain_id = None

        if not self.is_home(venue):
            # compare against the chains/venues in the cache
            chain_id = self.is_chain_cached(venue)
            #if chain_id == None:
                # check against a global search for similar venues
                #chain_id = self.is_chain_global(venue)
            if chain_id == None and self.vs.venue_has_chain_property(venue):
                # if foursquare insist it's a chain, create a new chain
                chain = self.cm.create_chain([venue])
                chain_id = chain['_id']

        return chain_id


    def is_chain_global(self, venue):

        # just need the venue data, not the whole API response
        if venue.get('response'):
            venue = venue['response']['venue']        

        # search for venues with similar names
        global_venues = self.vs.global_search(venue['name'])

        # go through all the returned venues and see if 
        # any of them belong to a chain
        for v in global_venues:
            # need to work with full response
            v = self.vs.get_venue_json(v['id'])
            if not self.is_home(v):
                chain_id = self.is_chain_cached(v)

        # now can compare the venue to the cache
        return self.is_chain_cached(venue)



    def is_chain_cached(self, venue):

        # just need the venue data, not the whole API response
        if venue.get('response'):
            venue = venue['response']['venue']        

        chain_id = None

        # check if the venue is already in a chain
        chain_id = self.ccm.check_chain_lookup(venue)
        if chain_id == None:
            # compare the venue against existing chains
            chain_id = self.ccm.check_existing_chains(venue)
            if chain_id == None:
                # check the rest of the venues in the cache
                chain_id = self.ccm.fuzzy_compare_to_cache(venue)
        return chain_id

if __name__ == "__main__":

    vs = VenueSearcher()

    starbucks1 = vs.get_venue_json('526903fb11d2cd6a3c51e1ff')
    northcliffe = vs.get_venue_json('5030ef53e4b0beacbee84cef')
    starbucks2 = vs.get_venue_json('5315d2d211d2c227cf2a7037')
    mcdonalds = vs.get_venue_json('4b6d80baf964a520b8782ce3')
    tesco = vs.get_venue_json('4c14b6aea1010f479fd94c18')
    costa = vs.get_venue_json('4db656b50cb6729b6ab71531')
    lounge = vs.get_venue_json('4b058838f964a52016b922e3')

    cd = ChainDecider()
    print "northcliffe: %s" % (cd.is_chain(northcliffe))
    print "starbucks1: %s" % (cd.is_chain(starbucks1))
    print "lounge: %s" % (cd.is_chain(lounge))
    #print "tesco: %s,%s,%s" % (cd.is_chain(tesco))
    #print "mcdonalds: %s,%s,%s" % (cd.is_chain(mcdonalds))
    #print "costa: %s,%s,%s" % (cd.is_chain(costa))
