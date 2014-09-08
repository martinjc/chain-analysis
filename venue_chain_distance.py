from Levenshtein import ratio
from urlparse import urlparse

def calc_venue_distance(venue1, venue2):

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
    url_match = False
    twitter_match = False
    facebook_match = False
    category_match = 0.0

    # compare URLs
    if v1.get('url') and v2.get('url'):
        if urlparse(v1['url']).netloc == urlparse(v2['url']).netloc and v1['url']:
            url_match = True

    # compare social media
    if v1.get('contact') and v2.get('contact'):
        if v1['contact'].get('twitter') and v2['contact'].get('twitter'):
            if v1['contact']['twitter'] == v2['contact']['twitter'] and v1['contact']['twitter']:
                twitter_match = True
        if v1['contact'].get('facebook') and v2['contact'].get('facebook'):
            if v1['contact']['facebook'] == v2['contact']['facebook'] and v1['contact']['facebook']:
                facebook_match = True

    # compare categories
    if v1.get('categories') and v2.get('categories'):
        for category1 in v1['categories']:
            for category2 in v2['categories']:
                if category1['id'] == category2['id']:
                    category_match += 1.0

    return name_distance, url_match, twitter_match, facebook_match, category_match


def calc_chain_distance(venue, chain):

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
                    social_media_confidence = 1.0
            if v['contact'].get('facebook'):
                facebook = v['contact']['facebook']
                if facebook in chain['facebook']:
                    social_media_confidence = 1.0

        # check category matches
        categories_confidence = 0.0
        if v.get('categories'):
            for category in v['categories']:
                if category['id'] in chain['categories']:
                    categories_confidence += 1.0

        return average_ratio, url_confidence, social_media_confidence, categories_confidence