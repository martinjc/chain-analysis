from Levenshtein import ratio
from urlparse import urlparse

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
        if urlparse(v1['url']).netloc == urlparse(v2['url']).netloc and v1['url']:
            url_match = 1.0

    # compare social media
    if v1.get('contact') and v2.get('contact'):
        if v1['contact'].get('twitter') and v2['contact'].get('twitter'):
            if v1['contact']['twitter'] == v2['contact']['twitter'] and v1['contact']['twitter'] and v1['contact']['twitter'] != "none":
                social_media_match += 1.0
        if v1['contact'].get('facebook') and v2['contact'].get('facebook'):
            if v1['contact']['facebook'] == v2['contact']['facebook'] and v1['contact']['facebook'] and v1['contact']['facebook'] != "none":
                social_media_match += 1.0

    # compare categories
    if v1.get('categories') and v2.get('categories'):
        for category1 in v1['categories']:
            for category2 in v2['categories']:
                if category1['id'] == category2['id']:
                    category_match += 1.0

    return name_distance, url_match, social_media_match, category_match


def find_best_match(venue, candidate_venues):

    # just need the venue data, not the whole API response
    if venue.get('response'):
        v = venue['response']['venue']
    else:
        v = venue

    max_confidence = 0.0
    best_match = None
    
    for candidate in candidate_venues:
        if candidate.get('response'):
            c = candidate['response']['venue']
        else:
            c = candidate

            confidence = calc_venue_match_confidence(v, c)
            if confidence > max_confidence:
                max_confidence = confidence
                best_match = c

    return best_match, max_confidence