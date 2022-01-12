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
"""encoder.py.

Encoding utilities for BSON documents.
"""
import io
from collections import deque
# Local imports.
import ibson.codec_util as util
import ibson.errors as errors


def _format_key(key):
    if isinstance(key, (bytes, bytearray)):
        return key
    # Cast non-string types into a string.
    if not isinstance(key, str):
        key = str(key)
    # Encode the string into UTF-8 or ASCII.
    return key.encode('utf-8')


def encode_document(encoder, obj, stm):
    if not isinstance(obj, dict):
        raise Exception("Invalid Object type: {}, expected 'dict'".format(
            type(obj)))
    # The stream should be seekable. If it isn't, then it should be wrapped
    # appropriately using 'io' module utilities.

    # Initialize this with an initial length of 4 to account for the first
    # 'size' field that precedes any (nested) BSON document.
    curr_frame = util.DocumentFrame(
        '', stm.tell(), 4, False, iter(obj.items()))
    current_stack = deque()
    current_stack.append(curr_frame)

    buff = util.INT32_STRUCT.pack(0)
    stm.write(buff)

    while current_stack:
        frame = current_stack[-1]

        # Iterate over the keys of the document and extract the next item to
        # export. If there are no more items to export, 'StopIteration' will
        # be raised.
        try:
            key, val = next(frame.ext_data)
            # Write out the current key.
            bkey = _format_key(key)
            stm.write(bkey)
            # Remember the null terminator.
            stm.write(b'\x00')

            # Check if the nested field is a dict type.
            if isinstance(val, dict):
                # Nested document. Create a new frame to iterate over.
                # Write out the 'nested document' opcode.
                stm.write(b'\x03')
                new_frame = util.DocumentFrame(
                    key, stm.tell(), 4, False, iter(val.items()))
                current_stack.append(new_frame)
                continue
            elif isinstance(val, (list, tuple)):
                # Nested document. Create a new frame to iterate over.
                # Write out the 'array' opcode.
                stm.write(b'\x04')

                new_frame = util.DocumentFrame(
                    key, stm.tell(), 4, False, enumerate(val))
                current_stack.append(new_frame)
                continue

            # Now, write out the field depending on the type.
            print("VAL: ", val)

        except StopIteration:
            # First, write out the null-terminator that ends each document.
            stm.write(b'\x00')
            # Process the current frame. At this point, the total size of the
            # document is known, so write that out as appropriate. We need to
            # seek _back_ to the start of this (potentially nested) document,
            # write the length, then resume back to where we were before.
            curr_pos = stm.tell()

            # Get the number of bytes written since the end.
            length = curr_pos - frame.fpos
            stm.seek(frame.fpos)
            buff = util.INT32_STRUCT.pack(length)
            stm.write(buff)
            stm.seek(curr_pos)

            # Now, pop the given frame from the stack, since we are done with
            # this nested document.
            current_stack.pop()


class BSONEncoder(object):
    """Encoder that writes python objects to a BSON byte stream."""

    def __init__(self):
        self._opcode_mapping = {
        }

    def dumps(self, obj):
        """Serialize the given object into a BSON byte stream."""
        with io.BytesIO() as stm:
            self.dump(obj, stm)
            return stm.getValue()

    def dump(self, obj, stm):
        """Serialize the given object into a BSON file."""
        # First, assert that the object is a python dictionary.
        if not isinstance(obj, dict):
            raise errors.BSONEncodeError('Root object must be a dict!')

        # Initialize this with an initial length of 4 to account for the first
        # 'size' field that precedes any (nested) BSON document.
        curr_frame = util.DocumentFrame(
            '', stm.tell(), 4, False, iter(obj.items()))
        current_stack = deque()
        current_stack.append(curr_frame)

        buff = util.INT32_STRUCT.pack(0)
        stm.write(buff)

        while current_stack:
            frame = current_stack[-1]

            # Iterate over the keys of the document and extract the next item
            # to export. If there are no more items to export, 'StopIteration'
            # will be raised.
            try:
                key, val = next(frame.ext_data)
                # Write out the current key.
                bkey = _format_key(key)
                stm.write(bkey)
                # Remember the null terminator.
                stm.write(b'\x00')

                # Check if the nested field is a dict type.
                if isinstance(val, dict):
                    # Nested document. Create a new frame to iterate over.
                    # Write out the 'nested document' opcode.
                    stm.write(b'\x03')
                    new_frame = util.DocumentFrame(
                        key, stm.tell(), 4, False, iter(val.items()))
                    current_stack.append(new_frame)
                    continue
                elif isinstance(val, (list, tuple)):
                    # Nested document. Create a new frame to iterate over.
                    # Write out the 'array' opcode.
                    stm.write(b'\x04')

                    new_frame = util.DocumentFrame(
                        key, stm.tell(), 4, False, enumerate(val))
                    current_stack.append(new_frame)
                    continue

                # Now, write out the field depending on the type.
                print("VAL: ", val)

            except StopIteration:
                # First, write out the null-terminator that ends each document.
                stm.write(b'\x00')
                # Process the current frame. At this point, the total size of
                # the document is known, so write that out as appropriate. We
                # need to seek _back_ to the start of this (potentially nested)
                # document, write the length, then resume back to where we were
                # before.
                curr_pos = stm.tell()

                # Get the number of bytes written since the end.
                length = curr_pos - frame.fpos
                stm.seek(frame.fpos)
                buff = util.INT32_STRUCT.pack(length)
                stm.write(buff)
                stm.seek(curr_pos)

                # Now, pop the given frame from the stack, since we are done
                # with this nested document.
                current_stack.pop()
