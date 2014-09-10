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
from chain_match import calc_chain_match_confidence

class CachedChain:

    def __init__(self, cache):

        self.id = uuid.uuid4().hex
        self._empty_chain()
        self.cache=cache

    
    def _from_dict(self, chain):
        self.id = chain["_id"]
        self.venues = set(chain["venues"])
        self.names = set(chain["names"])
        self.categories = set(chain["categories"])
        self.urls = set(chain["urls"])
        self.twitter = set(chain["twitter"])
        self.facebook = set(chain["facebook"])
        self.confidences = chain["confidences"]   


    def _to_dict(self):

        chain = {
            "_id": self.id,
            "venues": list(self.venues),
            "names": list(self.names),
            "categories": list(self.categories),
            "confidences": self.confidences,
            "urls": list(self.urls),
            "twitter": list(self.twitter),
            "facebook": list(self.facebook)
        }
        return chain


    def _empty_chain(self):
        
        self.venues = set()
        self.names = set()
        self.categories = set()
        self.urls = set()
        self.twitter = set()
        self.facebook = set()
        self.confidences = {}


    def calculate_confidences():
        for venue in self.venues:
            venue_data = self.cache.get_document('venues', {"_id": venue})
            nd, um, sm, cm = self.get_venue_match_confidence(venue_data)
            self.confidences[venue] = sum([nd,um,sm])       


    def prune_chain(self, required_confidence):

        self.calculate_confidences()
        to_remove = set()
        for venue, confidence in self.confidences.iteritems():
            if confidence < required_confidence:
                to_remove.add(venue)
        
        for venue in to_remove:
            self.remove_venue(self.cache.get_document('venues', {"_id": venue}))


    def get_venue_match_confidence(self, venue):

        if venue.get('response'):
            venue = venue['response']['venue']

        # if it's a new venue, just return the match confidence
        if venue['id'] not in self.venues:
            return calc_chain_match_confidence(venue, self._to_dict())
        # otherwise build a copy of the chain with the venue removed,
        # then calculate and return the confidence
        else:
            chain = CachedChain(self.cache)
            for v in self.venues:
                if v != venue['id']:
                    v_data = self.cache.get_document('venues', {"_id": v})
                    chain.add_venue(v_data)
            return chain.get_venue_match_confidence(venue)


    def remove_venue(self, venue):

        if venue.get('response'):
            venue = venue['response']['venue']   

        venues = self.venues[:]
        self._empty_chain()
        for v in venues:
            if venue['id'] != v['id']:
                self.add_venue(v)

        self.cache.remove_document('chain_id_lookup', {'_id': venue['id']})
        self.calculate_confidences()


    def add_venue(self, venue):

        if venue.get('response'):
            venue = venue['response']['venue']

        self.venues.add(venue['id'])
        self.names.add(venue['name'])
        # add any extra details
        if venue.get('url'):
            venue_url = urlparse(venue['url']).netloc
            self.urls.add(venue['url'])
        if venue.get('contact'):
            if venue['contact'].get('twitter'):
                self.twitter.add(venue['contact']['twitter'])
            if venue['contact'].get('facebook'):
                self.facebook.add(venue['contact']['facebook'])
        if venue.get('categories'):
            for category in venue['categories']:
                self.categories.add(category['id'])


    def save(self):

        chain = self._to_dict()
        self.cache.put_document('chains', chain)

        for venue in self.venues:
            # add the inverse lookup
            v = self.cache.get_document('venues', {"_id": venue})
            nd, um, sm, cm = self.get_venue_match_confidence(v)
            data = {'_id': venue,
                    'chain_id': self.id,
                    'confidence': sum([nd,um,sm])}
            self.cache.put_document('chain_id_lookup', data)


class ChainManager:
    """
    ChainManager is responsible for handling chain operations.
    """

    def __init__(self, db_name='fsqexp'):

        self.cache = MongoDBCache(db=db_name)
              

    def create_chain(self, venues):
        chain = CachedChain(self.cache)
        for venue in venues:
            chain.add_venue(venue)
        chain.save()
        return chain

    def add_to_chain(self, chain_id, venues):
        chain = self.load_chain(chain_id)
        for venue in venues:
            chain.add_venue(venue)
        chain.save()
        return chain       

    def delete_chain(self, chain):
        venues = chain.venues[:]
        for venue in venues:
            chain.remove_venue(venue)
        self.cache.remove_document('chains', {"_id": chain.id})

    def merge_chains(self, chain1, chain2):
        venues = chain1.venues[:] + chain2.venues[:]
        self.delete_chain(chain1)
        self.delete_chain(chain2)
        return self.create_chain(venues)

    def load_chain(self, chain_id):
        chain = self.cache.get_document('chains', {"_id": chain_id})
        c = CachedChain(self.cache)
        c._from_dict(chain)
        return c


