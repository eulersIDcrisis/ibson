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
"""codec_util.py.

Common utilities for BSON encoding and decoding.
"""
import struct


# Define common structures for 'unpacking' bytes here.
BYTE_STRUCT = struct.Struct('B')
"""Struct to unpack a single byte."""


INT32_STRUCT = struct.Struct('<i')
"""Struct to unpack a 32-bit signed integer in little-endian format."""


UINT32_STRUCT = struct.Struct('<I')
"""Struct to unpack a 32-bit unsigned integer in little-endian format."""


INT64_STRUCT = struct.Struct('<q')
"""Struct to unpack a 64-bit signed integer in little-endian format."""


UINT64_STRUCT = struct.Struct('<Q')
"""Struct to unpack a 64-bit unsigned integer in little-endian format."""


DOUBLE_STRUCT = struct.Struct('<d')
"""Struct to unpack a double (i.e. 64-bit float) in little-endian format."""


class DocumentFrame(object):

    def __init__(self, key, fpos, parent=None, length=None, is_array=False,
                 ext_data=None):
        self.parent = parent
        self.key = key
        self.fpos = fpos
        self.offset = fpos
        self.length = length
        self.is_array = is_array
        self.ext_data = ext_data
