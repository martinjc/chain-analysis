from urlparse import urlparse
from db_cache import MongoDBCache
from category_utils import CategoryTree

cache = MongoDBCache(db='fsqexp')
ct = CategoryTree()

venues = []

# get all the venues from the database
db_venues = cache.get_collection('venues').find(timeout=False)

# extract information needed for comparison
for v in db_venues:
    # just work with venue information instead of whole response
    if v.get('response'):
        v = v['response']['venue']  
        
    venue = {}
    venue['_id'] = v['id']
    venue['name'] = v['name']
    # check for url
    if v.get('url'):
        # parse url now
        venue['url'] = urlparse(v['url']).netloc
    # check for social media info
    if v.get('contact'):
        venue['contact'] = {}
        if v['contact'].get('twitter') and v['contact']['twitter'] != 'none':
            venue['contact']['twitter'] = v['contact']['twitter']
        if v['contact'].get('facebook')and v['contact']['facebook'] != 'none':
            venue['contact']['facebook'] = v['contact']['facebook']
    # copy any categories
    venue['categories'] = []            
    if v.get('categories'):
        venue['categories'] = v['categories']

    # don't include homes or residences
    if v.get('categories'):
        if len(v['categories']) > 0:
            # check the first (primary) category
            root_category = ct.get_root_node_for_id(v['categories'][0]['id'])
            # if the root is not 'Homes and Residences'
            if root_category is not None and root_category['foursq_id'] != "4e67e38e036454776db1fb3a":
                cache.put_document('min_venues', venue)
        # add venues with no categories
        else:
            cache.put_document('min_venues', venue)
    # add venues with no categories
    else:
        cache.put_document('min_venues', venue)


