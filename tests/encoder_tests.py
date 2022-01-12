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
"""encoder_tests.py.

Unittests for encoding BSON documents.
"""
import unittest
import ibson


class BSONEncoderTests(unittest.TestCase):
    """Module to test BSONDecoder."""

    def test_int32(self):
        obj = dict(value=123)
        actual = ibson.dumps(obj)
        expected = b'\x10\x00\x00\x00\x10value\x00{\x00\x00\x00\x00'
        self.assertEqual(actual, expected)

    def test_int64(self):
        obj = dict(value=2 ** 33)  # (int should store as an int64)
        actual = ibson.dumps(obj)
        expected = (
            b'\x14\x00\x00\x00\x12value\x00\x00\x00\x00\x00\x02\x00\x00\x00'
            b'\x00')
        self.assertEqual(actual, expected)

    def test_double(self):
        # Test the full load.
        obj = dict(value=3.1459)
        actual = ibson.dumps(obj)
        expected = b'\x14\x00\x00\x00\x01value\x00&\xe4\x83\x9e\xcd*\t@\x00'
        self.assertEqual(actual, expected)

    def test_null(self):
        # Test the full load.
        obj = dict(value=None)
        actual = ibson.dumps(obj)
        expected = b'\x0c\x00\x00\x00\nvalue\x00\x00'
        self.assertEqual(actual, expected)

    def test_bool_true(self):
        # Test the full load.
        obj = dict(value=True)
        actual = ibson.dumps(obj)
        expected = b'\r\x00\x00\x00\x08value\x00\x01\x00'
        self.assertEqual(actual, expected)

    def test_bool_false(self):
        # Test the full load.
        obj = dict(value=False)
        actual = ibson.dumps(obj)
        expected = b'\r\x00\x00\x00\x08value\x00\x00\x00'
        self.assertEqual(actual, expected)

    def test_utf8_string(self):
        # Test the full load.
        obj = dict(value=u'Ωhello')
        actual = ibson.dumps(obj)
        expected = (
            b'\x18\x00\x00\x00\x02value\x00\x08\x00\x00\x00\xce\xa9hello\x00'
            b'\x00')
        self.assertEqual(actual, expected)

    def test_nested_documents(self):
        obj = dict(key=dict(value='a'), key2='b')
        actual = ibson.dumps(obj)
        expected = (
            b'(\x00\x00\x00\x03key\x00\x12\x00\x00\x00\x02value\x00\x02\x00'
            b'\x00\x00a\x00\x00\x02key2\x00\x02\x00\x00\x00b\x00\x00')
        self.assertEqual(actual, expected)

    def test_array(self):
        obj = dict(value=[1, 2, 3, 4, 5])
        actual = ibson.dumps(obj)
        expected = (
            b'4\x00\x00\x00\x04value\x00(\x00\x00\x00\x100\x00\x01\x00\x00\x00'
            b'\x101\x00\x02\x00\x00\x00\x102\x00\x03\x00\x00\x00\x103\x00\x04'
            b'\x00\x00\x00\x104\x00\x05\x00\x00\x00\x00\x00')
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
