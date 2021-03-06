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

import urllib2

from _credentials import *
from itertools import ifilter
from Levenshtein import ratio
from datetime import timedelta
from db_cache import MongoDBCache
from api import APIGateway, APIWrapper


class VenueSearcher:

    def __init__(self, db_name='fsqexp'):
        
        self.gateway = APIGateway(access_token, 500, [client_id, client_secret], 5000)
        self.wrapper = APIWrapper(self.gateway)

        self.params = {
            'v' : 20140713
        }

        self.cache = MongoDBCache(db=db_name)


    def venue_has_chain_property(self, venue):
        if venue.get('page', None) is not None:
            if venue['page'].get('user', None) is not None:
                if venue['page']['user'].get('type', None) is not None:
                    return venue['page']['user']['type'] == 'chain'
        return False


    def global_search(self, query, check_fresh=False):

        params = {}
        params['v'] = self.params['v']
        params['intent'] = 'global'
        params['limit'] = 50
        params['query'] = query
        

        if self.cache.document_exists('global_searches', {'params': params}, check_fresh):
            results = self.cache.get_document('global_searches', {'params': params}, check_fresh)
            return results['response']['venues']
        else:
            try:
                results = self.wrapper.query_routine('venues', 'search', params, True)
                if not results is None:
                    results['params'] = params
                    self.cache.put_document('global_searches', results)
                return results['response']['venues']
            except urllib2.HTTPError, e:
                pass
            except urllib2.URLError, e:
                pass


    def local_search(self, venue, query, radius, check_fresh=False):

        lat = venue['location']['lat']
        lng = venue['location']['lng']

        categories = ','.join(str(category['id']) for category in venue['categories'])

        params = {}
        params['v'] = self.params['v']
        params['ll'] = '%f,%f' % (lat, lng)
        params['intent'] = 'browse'
        params['radius'] = radius
        params['limit'] = 50
        params['categoryId'] = categories
        params['query'] = query

        if self.cache.document_exists('local_searches', {'params': params}, check_fresh):
            results = self.cache.get_document('local_searches', {'params': params}, check_fresh)
            return results['response']['venues']
        else:
            try:
                results = self.wrapper.query_routine('venues', 'search', params, True)
                if results is not None:
                    results['params'] = params
                    self.cache.put_document('local_searches', results)
                return results['response']['venues']
            except urllib2.HTTPError, e:
                pass
            except urllib2.URLError, e:
                pass


    def get_venue_json(self, venue_id, check_fresh=False):

        response = None
        if self.cache.document_exists('venues', {'_id': '%s' % (venue_id)}, check_fresh):
            response = self.cache.get_document('venues', {'_id': '%s' % (venue_id)}, check_fresh)
        else:
            try:
                response = self.wrapper.query_resource('venues', venue_id, get_params=self.params, userless=True, tenacious=True)
            except urllib2.HTTPError, e:
                pass
            except urllib2.URLError, e:
                pass
            if not response is None:
                response['_id'] = venue_id
                self.cache.put_document('venues', response)

        if not response is None:
            return response['response']['venue']
        else:
            return None


    def search_alternates(self, venue, radius=500, check_fresh=False):

        lat = venue['location']['lat']
        lng = venue['location']['lng']

        categories = ','.join(str(category['id']) for category in venue['categories'])

        params = {}
        params['v'] = self.params['v']
        params['ll'] = '%f,%f' % (lat, lng)
        params['intent'] = 'browse'
        params['radius'] = radius
        params['limit'] = 50
        params['categoryId'] = categories

        if self.cache.document_exists('alternates', {'params': params}, check_fresh):
            alternatives = self.cache.get_document('alternates', {'params': params}, check_fresh)
            return alternatives['response']['venues']
        else:
            try:
                alternatives = self.wrapper.query_routine('venues', 'search', params, True, True)
                if not alternatives is None:
                    alternatives['params'] = params
                    self.cache.put_document('alternates', alternatives)
                return alternatives['response']['venues']
            except urllib2.HTTPError, e:
                pass
            except urllib2.URLError, e:
                pass   

