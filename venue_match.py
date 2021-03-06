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

from Levenshtein import ratio
from urlparse import urlparse

def get_min_venue_from_db(venue):

    if venue.get('response'):
        venue = venue['response']['venue']

    v = {}
    v['name'] = venue['name']
    v['id'] = venue['id']
    if venue.get('url'):
        v['url'] = venue['url']
    if venue.get('contact'):
        v['contact'] = {}
        if venue['contact'].get('twitter'):
            v['contact']['twitter'] = venue['contact']['twitter']
        if venue['contact'].get('facebook'):
            v['contact']['facebook'] = venue['contact']['facebook']                    
    if venue['categories']:
        v['categories'] = []
        for category in venue['categories']:
            v['categories'].append(category['id'])

    return v

def get_min_venue_from_csv(venue):

    v = {}
    v['name'] = venue['name']
    v['id'] = venue['id']
    if venue['url'] is not "":
        v['url'] = venue['url']
    if venue['contact-twitter'] is not "" and venue['contact-facebook'] is not "":
        v['contact'] = {}
        if venue['contact-twitter'] is not "":
            v['contact']['twitter'] = venue['contact-twitter']
        if venue['contact-facebook'] is not "":
            v['contact']['facebook'] = venue['contact-facebook']                    
    if venue['categories']:
        v['categories'] = venue['categories'].split(',')

    return v

def calc_venue_match_confidence(venue1, venue2):

    """
    calculates distance between two venues by comparing names, 
    social media handles, URLs and categories
    """

    # just need the venue data, not the whole API response
    if venue1.get('response'):
        v1 = venue1['response']['venue']
    else:
        v1 = venue1
    if venue2.get('response'):
        v2 = venue2['response']['venue']
    else:
        v2 = venue2

    #levenshtein distance of names
    name_distance = ratio(v1['name'], v2['name'])
    url_match = 0.0
    social_media_match = 0.0
    category_match = 0.0

    # compare URLs
    if v1.get('url') and v2.get('url'):
        if v1['url']:
            if urlparse(v1['url']).netloc:
                if urlparse(v2['url']).netloc:
                    if urlparse(v1['url']).netloc == urlparse(v2['url']).netloc:
                        url_match = 1.0

    # compare social media
    if v1.get('contact') and v2.get('contact'):
        if v1['contact'].get('twitter') and v2['contact'].get('twitter'):
            if v1['contact']['twitter'] == v2['contact']['twitter'] and v1['contact']['twitter'] and v1['contact']['twitter'] != "none":
                social_media_match += 1.0
        if v1['contact'].get('facebook') and v2['contact'].get('facebook'):
            if v1['contact']['facebook'] == v2['contact']['facebook'] and v1['contact']['facebook'] and v1['contact']['facebook'] != "none":
                social_media_match += 1.0

    # compare categories if names match - match = +1.0, - no match = -1.0
    if name_distance > 0.9:
        c1 = set()
        c2 = set()
        if v1.get('categories') and v2.get('categories'):
            for category in v1['categories']:
                c1.add(category)
            for category in v2['categories']:
                c2.add(category)
        common = c1 & c2
        if len(common) > 0:
            category_match = 1.0
        else:
            category_match = -1.0

    return name_distance, url_match, social_media_match, category_match
    