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

import json
from db_cache import MongoDBCache

class VenueExtractor():
    """
    Class to match venues to chains or other venues in a cache
    """
    def __init__(self, db_name='fsqexp'):

        # access to the database
        self.cache = MongoDBCache(db=db_name)

    def extract_venues(self):
        venues = self.cache.get_collection('venues')
        with open('min_venues.csv', 'w') as venues_file:

            venues_file.write('name,id,url,contact-twitter,contact-facebook,categories\n')
            venues_file.flush()
            i = 0
            for v in venues:
                if i % 1000 == 0:
                    print i
                    venues_file.flush()
                if v.get('response'):
                    v = v['response']['venue']

                min_v = {}
                min_v['id'] = v['id']
                min_v['name'] = v['name']

                if v.get('url') :
                    min_v['url'] = v['url']
                else:
                    min_v['url'] = ""
                
                if v.get('contact'):
                    min_v['contact'] = {}

                    if v['contact'].get('twitter'):
                        min_v['contact']['twitter'] = v['contact']['twitter']
                    else:
                        min_v['contact']['twitter'] = ""

                    if v['contact'].get('facebook'):
                        min_v['contact']['facebook'] = v['contact']['facebook']
                    else:
                        min_v['contact']['facebook'] = ""
                else:
                    min_v['contact'] = {}
                    min_v['contact']['twitter'] = ""
                    min_v['contact']['facebook'] = ""

                if v.get('categories'):
                    min_v['categories'] = []
                    for c in v['categories']:
                        min_v['categories'].append(c['id'])
                else:
                    min_v['categories'] = []

                venues_file.write('\"%s\",' % min_v['name'].encode('utf-8'))
                venues_file.write('%s,' % min_v['id'])
                venues_file.write('%s,' % min_v['url'].encode('utf-8'))
                venues_file.write('%s,' % min_v['contact']['twitter'].encode('utf-8'))
                venues_file.write('%s,' % min_v['contact']['facebook'].encode('utf-8'))
                venues_file.write('\"%s\"\n' % ','.join(min_v['categories']))
                i += 1


if __name__ == '__main__':
    v = VenueExtractor()
    v.extract_venues()
