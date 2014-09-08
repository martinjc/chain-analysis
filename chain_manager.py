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
from urlparse import urlparse
from db_cache import MongoDBCache
from collections import defaultdict
from category_utils import CategoryTree


class AlreadyChainedError(RuntimeError) :

    def __init__(self, venue_id, chain_id):
        self.venue_id = venue_id
        self.chain_id = chain_id

    def __str__(self):
        return 'Venue \'%s\' already in chain \'%s\'' % (self.venue_id, self.chain_id)


class ChainManager:
    """
    ChainManager is responsible for handling all chain operations. It uses the MongoDB
    backend to:

        1. Create a new chain
        2. Add a venue to a chain
        3. Merge two chains together
        4. Remove a venue from a chain
        5. Delete a chain
    """

    def __init__(self):

        self.cache = MongoDBCache(db='fsqexp')
        

    def createChain(self, venue1, venue2, confidence):
        pass

    def addToChain(self, chain_id, venue, confidence):
        pass

    def mergeChains(self, chain1_id, chain2_id):
        pass

    def removeFromChain(self, chain_id, venue):
        pass

    def deleteChain(self, chain_id):
        pass