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
from urlparse import urlparse
from db_cache import MongoDBCache
from collections import defaultdict
from category_utils import CategoryTree

class ChainManager:
    """
    ChainManager is responsible for handling all chain operations. It uses the cache
    backend to:

        * Create a new chain
        * Add a venue to a chain
        * Merge two chains together
        * Remove a venue from a chain
        * Delete a chain
    """

    def __init__(self):

        self.cache = MongoDBCache(db='fsqexp')


    def create_chain(self, venue1, venue2, confidence):
        """
        Takes two Foursquare '/venues/id/' API responses and creates
        a chain linking the two together
        """

        # just need the venue data, not the whole API response
        v1 = venue1['response']['venue']
        v2 = venue2['response']['venue']

        # check we have two different venues and that neither already belongs to a chain
        assert v1['id'] != v2['id']
        assert not self.cache.document_exists('chain_id_lookup', {'_id': v1['id']})
        assert not self.cache.document_exists('chain_id_lookup', {'_id': v2['id']})

        # create a new chain_id
        chain_id = uuid.uuid4().hex

        # create a new Chain object
        chain = {'_id': chain_id,
                'venues': set(),
                'names': set(),
                'confidences': {},
                'categories': set(),
                'urls': set(),
                'twitter': set(),
                'facebook': set()}

        # store the new Chain object
        self.cache.put_document('chains', chain)

        # add each venue to the new chain
        for venue in [v1, v2]:
            self.add_to_chain(chain_id, venue, confidence)
        

    def add_to_chain(self, chain_id, venue, confidence):
        """
        Add a venue to the specified chain. 'venue' can be a complete
        Foursquare /venues/id/ API response, just the venue portion, or a
        dict with an 'id' and 'name'
        """
        # check the chain exists
        assert self.cache.document_exists('chains', {'_id': chain_id})

        # if we've got a full API response, extract venue information
        if venue.get('response'):
            venue = venue['response']['venue']

        # retrieve the chain document and add the venue
        chain = self.cache.get_document('chains', {'_id': chain_id})
        chain['venues'].add(venue['id'])
        chain['names'].add(venue['name'])
        chain['confidences'][venue['id']] = confidence

        # add any extra details
        if venue.get('url'):
            venue_url = urlparse(venue['url']).netloc
            chain['urls'].add(venue_url)
        if venue.get('contact'):
            if venue['contact'].get('twitter'):
                chain['twitter'].add(venue['contact']['twitter'])
            if venue['contact'].get('facebook'):
                chain['facebook'].add(venue['contact']['facebook'])
        if venue.get('categories'):
            for category in venue['categories']:
                chain['categories'].add(category['id'])

        # store the updated chain
        self.cache.put_document('chains', chain)

        # add the inverse lookup
        data = {'_id': venue['id'],
                'chain_id': chain_id,
                'confidence': confidence}
        self.cache.put_document('chain_id_lookup', data)


    def remove_from_chain(self, chain_id, venue):
        """
        Remove a venue from the specified chain. 'venue' can be a complete
        Foursquare /venues/id/ API response or just the venue portion
        """

        # check the chain exists
        assert self.cache.document_exists('chains', {'_id': chain_id})

        # if we've got a full API response, extract venue information
        if venue.get('response'):
            venue = venue['response']['venue']

        # get the chain from the cache
        chain = self.cache.get_document('chains', {'_id': chain_id})

        # need to remove venue, and any details it provided to the chain
        # easiest to loop through remaining venues and replace chain details
        names = set()
        urls = set()
        twitter = set()
        facebook = set()
        categories = set()

        # construct the chain data without contributions from venue to be removed
        for v in chain['venues']:
            if v != venue['id']:
                v_data = self.cache.get_document('venues', {'_id': v})
                names.add(v_data)
                if v_data.get('url'):
                    venue_url = urlparse(v_data['url']).netloc
                    urls.add(venue_url)
                if v_data.get('contact'):
                    if v_data['contact'].get('twitter'):
                        twitter.add(v_data['contact']['twitter'])
                    if v_data['contact'].get('facebook'):
                        facebook.add(v_data['contact']['facebook'])
                if v_data.get('categories'):
                    for category in v_data['categories']:
                        categories.add(category['id'])

        # replace the chain data
        del chain['confidences'][venue['id']]
        chain['venues'].remove(venue['id'])
        chain['names'] = names
        chain['urls'] = urls
        chain['twitter'] = twitter
        chain['facebook'] = facebook
        chain['categories'] = categories

        # store the updated chain
        self.cache.put_document('chains', chain)

        # remove the inverse lookup
        self.cache.remove_document('chain_id_lookup', {'_id': venue['id']})


    def delete_chain(self, chain_id):
        # check the chain exists
        assert self.cache.document_exists('chains', {'_id': chain_id})

        # get the chain from the cache
        chain = self.cache.get_document('chains', {'_id': chain_id})

        # remove all the chain lookups
        for v in chain['venues']:
            # remove the inverse lookup
            self.cache.remove_document('chain_id_lookup', {'_id': venue['id']})

        # remove the chain
        self.cache.remove_document('chains', {'_id': chain_id})


    def merge_chains(self, chain1_id, chain2_id):
        pass
