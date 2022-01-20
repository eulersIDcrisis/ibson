# -*- coding: utf-8 -*-
#
# MIT License
#
# Copyright (c) 2022 Aaron Gibson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
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
        main['test'] = 'passed?'
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

        self.assertEqual(main['test'], parsed_obj['test'])


if __name__ == '__main__':
	unittest.main()
