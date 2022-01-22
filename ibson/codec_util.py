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
