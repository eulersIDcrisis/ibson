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
import uuid
import decimal
import datetime
from functools import partial
from collections import deque
import collections.abc as abc
# Local imports.
import ibson.codec_util as util
import ibson.errors as errors


_INT32_LOWERBOUND = -2 ** 31
_INT32_UPPERBOUND = 2 ** 32 - 1


def _format_key(key):
    if isinstance(key, (bytes, bytearray)):
        return key
    # Cast non-string types into a string.
    if not isinstance(key, str):
        key = str(key)
    # Encode the string into UTF-8 or ASCII.
    return key.encode('utf-8')


class EncoderFrame(object):

    def __init__(self, key, fpos, parent=None, object_iterator=None,
                 on_done_callback=None):
        self._key = key
        self._starting_fpos = fpos
        self._parent = parent
        self._object_iterator = object_iterator
        self._on_done_callback = on_done_callback

    @property
    def key(self):
        """Return the current key for this frame."""
        return self._key

    @property
    def starting_fpos(self):
        """Return the starting position when the frame was created.

        NOTE: This can be None if the file isn't seekable.
        """
        return self._starting_fpos

    @property
    def parent(self):
        """Return the parent for this frame.

        If 'None', then this frame is the root frame.
        """
        return self._parent

    @property
    def object_iterator(self):
        """Return the iterator over the contents of the current frame."""
        return self._object_iterator

    def close(self):
        """Close this encoding frame by invoking any registered callbacks."""
        if self._on_done_callback:
            self._on_done_callback(self)


def _write_length_for_frame(stm, frame):
    # First, write the null-terminator for this frame. (Both document/dicts
    # and array/list types end with a \x00 character after the main contents
    # have been written.)
    stm.write(b'\x00')
    curr_pos = stm.tell()
    try:
        length = curr_pos - frame.starting_fpos
        # TODO -- Assert the length is less than 2^32
        stm.seek(frame.starting_fpos)
        stm.write(util.INT32_STRUCT.pack(length))
    finally:
        stm.seek(curr_pos)


class BSONEncoder(object):
    """Encoder that writes python objects to a BSON byte stream."""

    def dumps(self, obj):
        """Serialize the given object into a BSON byte stream."""
        with io.BytesIO() as stm:
            self.dump(obj, stm)
            return stm.getvalue()

    def dump(self, obj, stm):
        """Serialize the given object into a BSON file."""
        # First, assert that the object is a python dictionary.
        if not isinstance(obj, abc.Mapping):
            raise errors.BSONEncodeError('Root object must be a dict!')

        # Create the initial frame to iterate over from the given object and
        # output stream.
        initial_frame = EncoderFrame(
            # Current key pertaining to this frame. Use '' (empty string) for
            # the root.
            '',
            # Store the current position in the stream. This is needed later
            # when writing out the length.
            stm.tell(),
            # Store the parent frame. A parent of 'None' implies the root.
            parent=None,
            # The external data attached to this frame should be the iterator
            # over the elements of this current document. If requested, this
            # could use an appropriate sorting algorithm.
            object_iterator=iter(obj.items()),
            # Register this callback to invoke when exiting this frame.
            on_done_callback=partial(_write_length_for_frame, stm)
        )
        # Write out the initial length as '0'. This will be filled in later.
        stm.write(util.INT32_STRUCT.pack(0))

        for key, val, current_stack in self._encode_generator(initial_frame):
            try:
                frame = current_stack[-1]
                if isinstance(val, dict):
                    # This implies a nested document. Call 'write_document' to
                    # handle the stack appropriately, then continue.
                    new_frame = self.write_document(key, val, frame, stm)
                    # Send back this new frame to traverse.
                    current_stack.append(new_frame)
                elif isinstance(val, (list, tuple)):
                    # This implies a nested array/list/tuple. For now, we are
                    # generous and will encode this as an array.
                    new_frame = self.write_array(key, val, frame, stm)
                    # Send back this new frame to traverse.
                    current_stack.append(new_frame)
                else:
                    self.write_value(key, val, frame, stm)
            except errors.BSONEncodeError as e:
                # Update these exception types with the current stack. This
                # helps make the message more understandable in a more general
                # context.
                e.update_with_stack(current_stack)
                raise e
            except Exception as exc:
                # Reraise the exception, but with some context about it.
                new_exc = errors.BSONEncodeError(
                    key, str(exc), fpos=stm.tell())
                new_exc.update_with_stack(current_stack)
                raise new_exc from exc

    def write_document(self, key, val, current_frame, stm):
        if not isinstance(val, abc.Mapping):
            raise errors.BSONEncodeError(
                key, 'Object is not of the proper type!')

        # Write out the 0x03 opcode and the key first.
        stm.write(b'\x03')
        self._write_raw_key(key, stm)

        # Register a new frame, to write the contents of this nested document.
        # This frame should _not_ include the opcode or the key as the start.
        frame = EncoderFrame(
            # Current key pertaining to this frame. Use '' (empty string) for
            # the root.
            key,
            # Store the current position in the stream. This is needed later
            # when writing out the length.
            stm.tell(),
            # Store the parent frame. A parent of 'None' implies the root.
            parent=current_frame,
            # The external data attached to this frame should be the iterator
            # over the elements of this current document. If requested, this
            # could use an appropriate sorting algorithm.
            object_iterator=iter(val.items()),
            # Register this callback to invoke when exiting this frame.
            on_done_callback=partial(_write_length_for_frame, stm)
        )

        # Write out an initial 'length' of this document as 0. This field will
        # need to be updated later once the actual length is known.
        stm.write(util.INT32_STRUCT.pack(0))

        # Return the newly generated frame.
        return frame

    def write_array(self, key, val, current_frame, stm):
        if not isinstance(val, abc.Collection):
            raise errors.BSONEncodeError(
                key, 'Object is not of the proper type!')

        # Write out the 0x04 opcode and the key first.
        stm.write(b'\x04')
        self._write_raw_key(key, stm)

        # Register a new frame, to write the contents of this nested array.
        frame = EncoderFrame(
            # Current key pertaining to this frame. Use '' (empty string) for
            # the root.
            key,
            # Store the current position in the stream. This is needed later
            # when writing out the length.
            stm.tell(),
            # Store the parent frame. A parent of 'None' implies the root.
            parent=current_frame,
            # The external data attached to this frame should be the iterator
            # over the elements of this array. BSON encodes arrays the same as
            # documents, with integers (cast as strings) for the index keys.
            # Thus, the need to enumerate over the contents.
            object_iterator=iter(enumerate(val)),
            # Register this callback to invoke when exiting this frame.
            on_done_callback=partial(_write_length_for_frame, stm)
        )

        # Write out an initial 'length' of this document as 0. This field will
        # need to be updated later once the actual length is known.
        stm.write(util.INT32_STRUCT.pack(0))

        return frame

    def write_value(self, key, val, current_stack, stm):
        # Handle each case.
        if val is None:
            self.write_null(key, stm)
        # NOTE: Check against 'bool' BEFORE 'int'; otherwise, bool values might
        # first compare as an int insteead.
        elif isinstance(val, bool):
            self.write_bool(key, val, stm)
        elif isinstance(val, int):
            # Handle whether a 32 or 64-bit integer.
            if val < _INT32_LOWERBOUND or val > _INT32_UPPERBOUND:
                self.write_int64(key, val, stm)
            else:
                self.write_int32(key, val, stm)
        elif isinstance(val, (float, decimal.Decimal)):
            self.write_float(key, float(val), stm)
        elif isinstance(val, datetime.datetime):
            self.write_datetime(key, val, stm)
        elif isinstance(val, str):
            self.write_string(key, val, stm)
        elif isinstance(val, uuid.UUID):
            self.write_uuid(key, val, stm)
        elif isinstance(val, (bytes, bytearray, memoryview)):
            self.write_binary(key, val, stm)
        else:
            # TODO -- Check for any custom overrides.
            raise errors.BSONEncodeError(
                key, 'Cannot encode object of type: {}'.format(type(val)))

    def write_int32(self, key, val, stm):
        stm.write(b'\x10')
        self._write_raw_key(key, stm)
        stm.write(util.INT32_STRUCT.pack(val))

    def write_int64(self, key, val, stm):
        stm.write(b'\x12')
        self._write_raw_key(key, stm)
        stm.write(util.INT64_STRUCT.pack(val))

    def write_float(self, key, val, stm):
        stm.write(b'\x01')
        self._write_raw_key(key, stm)
        stm.write(util.DOUBLE_STRUCT.pack(val))

    def write_bool(self, key, val, stm):
        stm.write(b'\x08')
        self._write_raw_key(key, stm)
        stm.write(b'\x01' if val else b'\x00')

    def write_null(self, key, stm):
        stm.write(b'\x0A')
        self._write_raw_key(key, stm)

    def write_min_key(self, key, stm):
        stm.write(b'\xFF')
        self._write_raw_key(key, stm)

    def write_max_key(self, key, stm):
        stm.write(b'\x7F')
        self._write_raw_key(key, stm)

    def write_datetime(self, key, dt, stm):
        stm.write(b'\x09')
        self._write_raw_key(key, stm)
        utc_ts = int(1000 * dt.timestamp())
        stm.write(util.INT64_STRUCT.pack(utc_ts))

    def write_string(self, key, val, stm):
        stm.write(b'\x02')
        self._write_raw_key(key, stm)
        # This should only really be called with 'str' types, but we'll
        # be generous for now.
        raw_str = val.encode('utf-8') if isinstance(val, str) else val
        length = len(raw_str) + 1  # Add one for the \x00 at the end.
        stm.write(util.INT32_STRUCT.pack(length))
        stm.write(raw_str)
        # Write the null-terminator character.
        stm.write(b'\x00')

    def write_uuid(self, key, val, stm):
        # UUIDs are written as a binary type, with a 0x04 subtype.
        stm.write(b'\x05')
        self._write_raw_key(key, stm)
        # NOTE: uuid.UUID().bytes should already be in little-endian.
        data = val.bytes
        stm.write(util.INT32_STRUCT.pack(len(data)))
        stm.write(len(data))
        # Binary subtype of 0x04 is for UUIDs.
        stm.write(b'\x04')
        stm.write(data)

    def write_bytes(self, key, val, stm):
        stm.write(b'\x05')
        self._write_raw_key(key, stm)
        stm.write(util.INT32_STRUCT.pack(len(val)))
        # 'Generic' binary subtype
        stm.write(b'\x00')
        stm.write(val)

    def _write_raw_key(self, key, stm):
        # If the given key is already a 'bytes/bytearray' type, write it out.
        if isinstance(key, (bytes, bytearray, memoryview)):
            stm.write(key)
            stm.write(b'\x00')
            return
        if isinstance(key, int):
            key = str(key)
        elif not isinstance(key, str):
            raise errors.BSONEncodeError(
                "Cannot encode invalid 'key' (type: {}): {}".format(
                    type(key), key))
        # Write out the key as a string (encoded via UTF-8 per the spec).
        stm.write(key.encode('utf-8'))

        # Write the null-terminator character after the key, as mandated by
        # the BSON specification.
        stm.write(b'\x00')

    def _encode_generator(self, initial_frame):
        """Yield the document frames, starting with the given initial frame.

        At each iteration, this generator yields a tuple of:
            (key, value, current_stack)
        where:
         - key: The current 'key' being parsed.
         - value: The value mapping to that 'key'
         - current_stack: The stack of document frames.

        For the most part, "key" and "value" are self-explanatory; they map to
        the key and value of the current object (usually a dict). If the object
        is an array, then the 'key' is the index of the element in that array.

        The "current_stack" object is more interesting; this stores a stack of
        document frames that hold the state of the parser. This is used to
        handle the encoding of nested documents without requiring the whole
        nested document to be held in memory.
        """
        # Initially, the current stack is empty.
        current_stack = deque()
        current_stack.append(initial_frame)

        # While there are still frames to parse, continue iterating over each
        # object. Each frame should store the iterator over that particular
        # document's contents.
        while current_stack:
            # Get the last frame.
            curr_frame = current_stack[-1]
            try:
                key, val = next(curr_frame.object_iterator)

                yield key, val, current_stack
            except StopIteration:
                # When reaching the end of the current frame, call the
                # 'finalizer' callback of the frame, then remove it from the
                # current stack.
                removed_frame = current_stack.pop()
                if removed_frame:
                    removed_frame.close()
