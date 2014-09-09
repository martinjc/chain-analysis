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

def calc_chain_match_confidence(venue, chain):

    # just need the venue data, not the whole API response
    if venue.get('response'):
        v = venue['response']['venue']
    else:
        v = venue

    # calculate average name ratio
    ratios = []
    average_ratio = 0.0
    for name in chain['names']:
        ratios.append(ratio(v['name'], name))
    average_ratio = float(sum(ratios))/len(ratios)

    # check url matches
    url_confidence = 0.0
    if v.get('url'):
        url = urlparse(venue['url']).netloc
        if url in chain['urls']:
            url_confidence = 1.0
    
    # check social media matches
    social_media_confidence = 0.0
    if v.get('contact'):
        if v['contact'].get('twitter'):
            twitter = v['contact']['twitter']
            if twitter in chain['twitter']:
                social_media_confidence += 1.0
        if v['contact'].get('facebook'):
            facebook = v['contact']['facebook']
            if facebook in chain['facebook']:
                social_media_confidence += 1.0

    # check category matches
    category_confidence = 0.0
    if average_ratio > 0.9:
        c1 = set()
        c2 = set()
        if v.get('categories'):
            for category in v['categories']:
                c1.add(category['id'])
            for category in chain['categories']:
                c2.add(category)
        common = c1 & c2
        if len(common) > 0:
            category_confidence = 1.0
        else:
            category_confidence = -1.0


    return average_ratio, url_confidence, social_media_confidence, category_confidence


def find_best_chain_match(venue, candidate_chains):

    # just need the venue data, not the whole API response
    if venue.get('response'):
        v = venue['response']['venue']
    else:
        v = venue

    max_confidence = 0.0
    best_match = None
    
    for candidate in candidate_chains:
        ar, uc, sc, cc = calc_chain_match_confidence(v, candidate)
        confidence = sum([ar, uc, sc])
        if confidence > max_confidence:
            max_confidence = confidence
            best_match = candidate
    return best_match, max_confidence

