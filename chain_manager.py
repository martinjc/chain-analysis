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
from db_cache import MongoDBCache

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

    def __init__(self, db_name='fsqexp'):

        self.cache = MongoDBCache(db=db_name)


    def create_chain(self, venues, confidences):
        """
        Takes some Foursquare '/venues/id/' API responses and creates
        a chain linking them together
        """

        v = []

        for venue in venues:
            if venue.get('response'):
                v.append(venue['response']['venue'])
            else:
                v.append(venue)

        # check we have at least two different venues
        v_ids = set([venue['id'] for venue in v])
        assert len(v_ids) > 1
        
        # check none of the venues are already in a chain
        for venue in v:
            assert not self.cache.document_exists('chain_id_lookup', {'_id': venue['id']})

        # create a new chain_id
        chain_id = uuid.uuid4().hex

        # create a new Chain object
        chain = {'_id': chain_id,
                'venues': [],
                'names': [],
                'confidences': {},
                'categories': [],
                'urls': [],
                'twitter': [],
                'facebook': []}

        # store the new Chain object
        print 'CM: created new chain: %s' % (chain_id)
        self.cache.put_document('chains', chain)

        # add each venue to the new chain
        self.add_to_chain(chain_id, v, confidences)

        return chain_id
        

    def add_to_chain(self, chain_id, venues, confidences):
        """
        Add a venue to the specified chain. 'venue' can be a complete
        Foursquare /venues/id/ API response, just the venue portion, or a
        dict with an 'id' and 'name'
        """
        # check the chain exists
        assert self.cache.document_exists('chains', {'_id': chain_id})

        for i, venue in enumerate(venues):

            # if we've got a full API response, extract venue information
            if venue.get('response'):
                venue = venue['response']['venue']

            # check the venue doesn't belong to a chain already
            assert not self.cache.document_exists('chain_id_lookup', {'_id': venue['id']})

            # retrieve the chain document and add the venue
            chain = self.cache.get_document('chains', {'_id': chain_id})
        
            if venue['id'] not in chain['venues']:
                chain['venues'].append(venue['id'])
        
            if venue['name'] not in chain['names']:
                chain['names'].append(venue['name'])
        
            chain['confidences'][venue['id']] = confidences[i]

            # add any extra details
            if venue.get('url'):
                venue_url = urlparse(venue['url']).netloc
                if not venue['url'] in chain['urls']:
                    chain['urls'].append(venue['url'])
            if venue.get('contact'):
                if venue['contact'].get('twitter'):
                    if not venue['contact']['twitter'] in chain['twitter']:
                        chain['twitter'].append(venue['contact']['twitter'])
                if venue['contact'].get('facebook'):
                    if not venue['contact']['facebook'] in chain['facebook']:
                        chain['facebook'].append(venue['contact']['facebook'])
            if venue.get('categories'):
                for category in venue['categories']:
                    if not category['id'] in chain['categories']:
                        chain['categories'].append(category['id'])

            # store the updated chain
            self.cache.put_document('chains', chain)

            # add the inverse lookup
            data = {'_id': venue['id'],
                    'chain_id': chain_id,
                    'confidence': confidence}
            self.cache.put_document('chain_id_lookup', data)

            print 'CM: added %s to chain %s' % (venue['id'], chain_id)


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
                v_data = self.cache.get_document('venues', {'_id': v})['response']['venue']
                names.add(v_data['name'])
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
        chain['names'] = list(names)
        chain['urls'] = list(urls)
        chain['twitter'] = list(twitter)
        chain['facebook'] = list(facebook)
        chain['categories'] = list(categories)

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
            self.cache.remove_document('chain_id_lookup', {'_id': v})

        # remove the chain
        self.cache.remove_document('chains', {'_id': chain_id})


    def merge_chains(self, chain1_id, chain2_id):
        pass
