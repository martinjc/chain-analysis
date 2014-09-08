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

chains = {}
chain_id_lookup = {}


class AlreadyChainedError(RuntimeError) :

    def __init__(self, venue_id, chain_id):
        self.venue_id = venue_id
        self.chain_id = chain_id

    def __str__(self):
        return 'Venue \'%s\' already in chain \'%s\'' % (self.venue_id, self.chain_id)


def add_chain(venue, confidence):
    """
    Add a new chain to the collection. A Chain object stores the venue_ids and 
    potential venue details of members of the Chain.
    """

    venue_data = venue['response']['venue']

    # check the venue doesn't already belong to a chain
    if chain_id_lookup.get(venue_data['id']):
        throw new AlreadyChainedError(venue_data['id'], chain_id_lookup[venue_data['id']])

    # create a new chain_id
    chain_id = uuid.uuid4().hex

    # create a new Chain object
    chain = {'_id': chain_id,
            'venues': set([venue_data['id']]),
            'names': set([venue_data['name']])}

    # how sure are we that this venue is part of the chain?
    confidences = {venue_data['id']: confidence}
    chain['confidences'] = confidences

    # add any details if present
    if venue_data.get('url'):
        venue_url = urlparse(venue_data['url']).netloc
        chain['urls'] = set([venue_url])
    if venue_data.get('contact'):
        if venue_data['contact'].get('twitter'):
            chain['twitter_handles'] = set([venue_data['contact']['twitter']])
    if venue_data.get('categories'):
        chain['categories'] = set()
        for category in venue_data['categories']:
            chain['categories'].add(category['id'])

    # add the chain to the collection of chains
    chains[chain_id].add(chain)

    # add a reverse lookup for the venue
    chain_id_lookup[venue_data['id']] = chain_id


def add_to_chain(chain_id, venue, confidence):

    venue_data = venue['response']['venue']

    # get the Chain object
    chain = chains[chain_id]

    # add venue name and id
    chain['venues'].add(venue_data['id'])
    chain['names'].add(venue_data['name'])

    # add our confidence value that this venue belongs to this chain
    chain['confidences'][venue_data['id']] = confidence
    
    # add any additional details if present
    if venue_data.get('url'):
        venue_url = urlparse(venue_data['url']).netloc
        chain['urls'].add(venue_url)
    if venue_data.get('contact'):
        if venue_data['contact'].get('twitter'):
            chain['twitter_handles'].add(venue_data['contact']['twitter'])
    if venue_data.get('categories'):
        for category in venue_data['categories']:
            chain['categories'].add(category['id'])



if __name__ == '__main__':
    self.cache = MongoDBCache(db='fsqexp')
