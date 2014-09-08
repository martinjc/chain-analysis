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
from db_cache import MongoDBCache



class CacheChainMatcher():

    def __init__(self):
        self.cache = MongoDBCache(db='fsqexp')

    def check_existing_chains(self, venue):

        chains
        pass

    def exact_compare_to_cache(self, venue):
        pass

    def fuzzy_compare_to_cache(self, venue):
        pass

    def compute_confidence(self, venue, chain_id):
        
        assert self.cache.document_exists('chains', {'_id': chain_id})

        # just need the venue data, not the whole API response
        if venue.get('response'):
            v = venue['response']['venue']
        else:
            v = venue

        # get the chain data
        chain = self.cache.get_document('chains', {'_id': chain_id})
        
        # calculate average name ratio
        ratios = []
        average_ratio = 0.0
        for name in chain['names']:
            ratios.append(ratio(v['name'], name))
        average_ratio = float(sum(ratios))/len(ratios)

        # check url matches
        url_confidence = 0.0
        if v.get('url'):
            url = v['url']
            if url in chain['urls']:
                url_confidence = 1.0
        
        # check social media matches
        twitter_confidence = 0.0
        facebook_confidence = 0.0
        if v.get('contact'):
            if v['contact'].get('twitter'):
                twitter = v['contact']['twitter']
                if twitter in chain['twitter']:
                    twitter_confidence = 1.0
            if v['contact'].get('facebook'):
                facebook = v['contact']['facebook']
                if facebook in chain['facebook']:
                    facebook_confidence = 1.0

        # check category matches
        categories_confidence = 0.0
        if v.get('categories'):
            for category in v['categories']:
                if category in chain['categories']:
                    categories_confidence += 1.0

        


if __name__ == '__main__':
    
