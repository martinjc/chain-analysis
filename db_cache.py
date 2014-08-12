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

import os
import time
import json
import calendar

from pymongo import *
from bson.objectid import ObjectId
from datetime import timedelta, datetime


class MongoDBCache(object):

    def __init__(self, mongo_db='mongodb://localhost:27017/', db='test', refresh_time=timedelta(days=1)):

        self.client = MongoClient(mongo_db)
        self.db = self.client[db]

        self.refresh_time = refresh_time


    def document_exists(self, collection, _id):

        """
        Checks to see if a document with the given '_id' exists within the database. Will
        also check for presence of 'last_modified' within the document, and return values
        according to document freshness
        """

        item = self.db[collection].find_one({'_id': ObjectId(_id)})

        # item exists
        if item is not None:

            # check item freshness
            if item.get('last_modified'):
                refresh_time = datetime.now() - self.refresh_time
                last_modified = datetime.fromtimestamp(item['last_modified'])

                # item is stale
                if last_modified < refresh_time:
                    return False
                # item is fresh
                else:
                    return True
            # can't check item is fresh, assume it never goes stale
            else:
                return True
        # item does not exist
        else:
            return False


    def get_document(self, collection, _id):
        assert self.document_exists(_id)
        return self.db[collection].find_one({'_id': ObjectId(_id)})


    def put_document(self, collection, _id, data):

        if not data.get('last_modified'):
            data['last_modified'] = calendar.timegm(datetime.utcnow().utctimetuple())

        data['_id'] = _id
        return self.db[collection].save(data)


        