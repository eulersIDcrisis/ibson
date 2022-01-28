# -*- coding: utf-8 -*-
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
"""encoder_tests.py.

Unittests for encoding BSON documents.
"""
import unittest
import datetime
# Main import
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
        expected = (
            b'\x18\x00\x00\x00\x02value\x00\x08\x00\x00\x00\xce\xa9hello\x00'
            b'\x00')
        actual = ibson.dumps(obj)
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

    def test_datetime_field(self):
        # Test the full load.
        # dict(value=datetime.datetime)
        data = b'\x14\x00\x00\x00\tvalue\x00\x87>\xafy~\x01\x00\x00\x00'
        # Corresponds to: datetime.datetime(2022, 1, 20, 22, 50, 35, 15000)
        # With UTC timezone.
        utc_ms = 1642719035015
        expected = datetime.datetime.fromtimestamp(
            utc_ms / 1000.0, tz=datetime.timezone.utc)
        obj = ibson.loads(data)
        self.assertIn('value', obj)
        self.assertEqual(set(obj.keys()), set(['value']))
        # Compare the value a bit looser.
        actual_dt = obj['value']
        self.assertEqual(actual_dt, expected)

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
