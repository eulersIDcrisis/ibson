# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 Aaron Gibson (eulersidcrisis@yahoo.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""stress_tests.py.

Tests that stress corner-cases of the bson parser.
"""
import unittest
import random

# Module under test
import ibson


class DeeplyNestedDocumentTests(unittest.TestCase):

    def test_deeply_recursive_document(self):
        RECURSION_DEPTH = 10000
        # Generate a document with a key nested 1,000 items in.
        main = dict()
        nested = main
        for i in range(RECURSION_DEPTH):
            key = str(i)
            nested[key] = dict()
            nested = nested[key]
        # Let's add some high-level keys on the highest level doc for good
        # measure.
        main["test"] = "passed?"
        # This object is _deeply_ nested so that even trying to print it can
        # potentially cause recursion errors. However, this should not be a
        # problem for ibson, which tracks this "recursion" separate from the
        # call stack.
        stm = ibson.dumps(main)
        parsed_obj = ibson.loads(stm)

        # Assert that these two dictionaries are equal.
        # NOTE: We need to perform this comparison manually because this
        # comparison would otherwise fail with Recursion errors.
        actual = parsed_obj
        nested = main
        for i in range(RECURSION_DEPTH):
            key = str(i)
            self.assertIn(key, actual)
            self.assertIn(key, nested)
            actual = actual[key]
            nested = nested[key]

        self.assertEqual(main["test"], parsed_obj["test"])


if __name__ == "__main__":
    unittest.main()
