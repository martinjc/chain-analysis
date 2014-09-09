def calc_chain_distance(venue, chain):

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
    categories_confidence = 0.0
    if v.get('categories'):
        for category in v['categories']:
            if category['id'] in chain['categories']:
                categories_confidence += 1.0

    return average_ratio, url_confidence, social_media_confidence, categories_confidence