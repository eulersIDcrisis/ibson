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


class BSONDecoderTests(unittest.TestCase):
    """Test cases for standard BSON decoding."""

    def test_int32(self):
        # dict(value=123)
        data = b'\x10\x00\x00\x00\x10value\x00{\x00\x00\x00\x00'

        obj = ibson.loads(data)
        self.assertEqual(obj, dict(value=123))

    def test_int64(self):
        # dict(value=2 ** 33)  (int should store as an int64)
        data = b'\x14\x00\x00\x00\x12value\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00'

        obj = ibson.loads(data)
        num = 2 ** 33
        self.assertEqual(obj, dict(value=num))

    def test_double(self):
        # Test the full load.
        # dict(value=3.1459)
        data = b'\x14\x00\x00\x00\x01value\x00&\xe4\x83\x9e\xcd*\t@\x00'

        obj = ibson.loads(data)
        # Be nice and do an 'almost equal' comparison for the value.
        # But still assert there is only one key.
        self.assertEqual(set(['value']), set(obj.keys()))
        self.assertIn('value', obj)
        # Should be accurate to 7 decimals.
        self.assertAlmostEqual(3.1459, obj['value'])

    @unittest.skip('Datetime needs special handling for now.')
    def test_datetime_field(self):
        # Test the full load.
        # dict(value=datetime.datetime)
        data = b'\x14\x00\x00\x00\tvalue\x00\xb0\\\xcaS~\x01\x00\x00\x00'
        # Corresponds to: January 13, 2022, 2:14:38 pm on PST.
        utc_ts = 1642112078
        expected = datetime.datetime.fromtimestamp(
            utc_ts, datetime.timezone.utc)
        obj = ibson.loads(data)
        self.assertEqual(obj, dict(value=expected))

    def test_null(self):
        # Test the full load.
        # dict(value=None)
        data = b'\x0c\x00\x00\x00\nvalue\x00\x00'
        obj = ibson.loads(data)
        self.assertEqual(obj, dict(value=None))

    def test_bool_true(self):
        # Test the full load.
        # dict(value=True)
        data = b'\r\x00\x00\x00\x08value\x00\x01\x00'
        obj = ibson.loads(data)
        self.assertEqual(obj, dict(value=True))

    def test_bool_false(self):
        # Test the full load.
        # dict(value=True)
        data = b'\r\x00\x00\x00\x08value\x00\x00\x00'
        obj = ibson.loads(data)
        self.assertEqual(obj, dict(value=False))

    def test_utf8_string(self):
        # Test the full load.
        # dict(value=u'Ωhello')
        data = b'\x18\x00\x00\x00\x02value\x00\x08\x00\x00\x00\xce\xa9hello\x00\x00'
        obj = ibson.loads(data)
        self.assertEqual(obj, dict(value=u'Ωhello'))

    def test_nested_documents(self):
        # dict(key=dict(value='a'), key2='b')
        data = (
            b'(\x00\x00\x00\x03key\x00\x12\x00\x00\x00\x02value\x00'
            b'\x02\x00\x00\x00a\x00\x00\x02key2\x00\x02\x00\x00\x00b\x00\x00'
        )
        obj = ibson.loads(data)
        self.assertEqual(obj, dict(key=dict(value='a'), key2='b'))

    def test_array(self):
        # dict(value=[1, 2, 3, 4, 5])
        data = (
            b'4\x00\x00\x00\x04value\x00(\x00\x00\x00\x100\x00\x01\x00\x00\x00'
            b'\x101\x00\x02\x00\x00\x00\x102\x00\x03\x00\x00\x00\x103\x00\x04'
            b'\x00\x00\x00\x104\x00\x05\x00\x00\x00\x00\x00')
        obj = ibson.loads(data)
        self.assertEqual(obj, dict(value=[1, 2, 3, 4, 5]))


if __name__ == '__main__':
    unittest.main()
