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
from operator import itemgetter
# Local imports.
import ibson.codec_util as util
import ibson.errors as errors
import ibson.types as types


def _format_key(key):
    if isinstance(key, (bytes, bytearray)):
        return key
    # Cast non-string types into a string.
    if not isinstance(key, str):
        key = str(key)
    # Encode the string into UTF-8 or ASCII.
    return key.encode('utf-8')


class EncoderFrame(object):
    """Object storing various information while exporting an object.

    This stores:
     - key: Immediate key this object is referenced by in its parent. (The
            root document has the empty key).
     - starting_fpos: Position in the file where this frame started. For
            array/dict types, this is the position in the file where the
            "length" parameter is set.
     - parent: Parent frame (or None if the root element)
     - object_iterator: Iterator over the contents of this current frame.
            This should return tuples of: (key, value)
     - on_done_callback: Callback that is invoked when the frame is finally
            popped off of the traversal stack.
    """

    def __init__(self, key, fpos, parent=None, object_iterator=None):
        self._key = key
        self._starting_fpos = fpos
        self._parent = parent
        self._object_iterator = object_iterator
        self._on_done_callbacks = []

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
        if self._on_done_callbacks:
            for callback in self._on_done_callbacks:
                callback(self)
        # Clear the callbacks to help resolve any circular references.
        self._on_done_callbacks = []

    def add_done_callback(self, cb):
        """Add the given callback to be invoked when the frame is closed.

        'cb' is expected to have the following signature:
            func(frame) -> Any
        where 'frame' is this frame instance.

        NOTE: The callbacks are executed in the order that they are added.
        """
        if not callable(cb):
            raise TypeError('Cannot add non-callable type: {}'.format(cb))
        self._on_done_callbacks.append(cb)


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


def _register_length(length_list, stm, frame):
    # The frame stores the starting position, and 'stm.tell()' stores the
    # current position at the time of the callback; the difference should be
    # the length. The frame's starting position is also where the calculated
    # length should be written.
    #
    # Lists are passed by reference, so just add the tuple to the list here.
    calculated_length = stm.tell() - frame.starting_fpos
    length_list.append((frame.starting_fpos, calculated_length))


class BSONEncoder(object):
    """Encoder that writes python objects to a BSON byte stream."""

    def __init__(self, traversal_callback=None):
        """Create an encoder for BSON documents.

        Parameters
        ----------
        traversal_callback: (dict) -> Iterator[Any, Any] or None
            Callback to invoke on a 'document' (dict type). Useful to set the
            order that the items are serialized (if desired). If 'None', then
            the encoder uses the equivalent of: 
                lambda val: iter(val.items())
        """
        if traversal_callback is None:
            self._traverse_document_iterator = lambda val: iter(val.items())
        elif not callable(traversal_callback):
            raise TypeError("'traversal_callback' should be callable!")
        else:
            self._traverse_document_iterator = traversal_callback

    def write_unknown_object(self, key, val, stm):
        """Write out an object of (unknown) type.

        By default, this simply raises an exception that the type could not be
        encoded. Callers can register types with custom encodings.
        """
        raise errors.BSONEncodeError(
            "Unrecognized type to encode: {}".format(type(val)))

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

        length_fields = []

        # Create the initial frame.
        initial_frame = self.write_document('', obj, None, stm, is_array=False)
        # Add the length callback to actually write the correct length.
        initial_frame.add_done_callback(
            # The 'frame' argument is passed during the callback.
            partial(_register_length, length_fields, stm)
        )

        for key, val, current_stack in self._encode_generator(initial_frame):
            try:
                frame = current_stack[-1]
                if isinstance(val, (list, tuple, dict)):
                    is_array = not isinstance(val, dict)

                    # This implies a nested document. Call 'write_document' to
                    # handle the stack appropriately, then continue.
                    new_frame = self.write_document(
                        key, val, frame, stm, is_array=is_array)

                    # For the new frame, register a callback to write over the
                    # proper length once the document is written (and thus the
                    # actual length is known).
                    new_frame.add_done_callback(
                        # The 'frame' argument is passed during the callback.
                        partial(_register_length, length_fields, stm)
                    )

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

        # At the end, all of the lengths need to be updated in the stream.
        length_fields.sort(key=itemgetter(0))
        for fpos, doc_length in length_fields:
            stm.seek(fpos)
            stm.write(util.INT32_STRUCT.pack(doc_length))

        # Set the stm position to the end of the file for consistency.
        stm.seek(0, io.SEEK_END)

    def write_document(self, key, val, current_frame, stm, is_array=False):
        """Start a (nested) document at the given key.

        If 'key' is empty and the current_frame is None, then the document is
        assumed to be the root frame (and thus the key and opcode are omitted
        when writing out the stream).

        This should return an 'EncoderFrame' object that contains the iterator
        that iterates over the contents of the nested document. When this is
        exhausted, the frame will be popped from the stack and the applicable
        "on_done_callbacks" will be invoked.

        Returns
        -------
        frame: EncoderFrame
            The frame that should be pushed onto the current stack.
        """
        # Write out the opcode and the key first, but only iff applicable.
        #
        # NOTE: Skip this step if the key and the current frame implies that
        # we are at the document root. This avoids the need to write out the
        # opcode and the raw key, for otherwise the same operation.
        if key or current_frame is not None:
            if is_array:
                stm.write(b'\x04')
            else:
                stm.write(b'\x03')
            self._write_raw_key(key, stm)

        if is_array:
            obj_itr = iter(enumerate(val))
        else:
            obj_itr = self._traverse_document_iterator(val)

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
            # over the elements of this current document.
            object_iterator=obj_itr
        )

        # When the frame is done, write out the null-terminator for both
        # documents and arrays.
        frame.add_done_callback(lambda frame: stm.write(b'\x00'))

        # Write out an initial 'length' of this document as 0. This field will
        # need to be updated later once the actual length is known.
        stm.write(util.INT32_STRUCT.pack(0))

        # Return the newly generated frame.
        return frame


    def write_value(self, key, val, current_stack, stm):
        # NOTE: The order of these checks does matter since some types can be
        # cast to some of the other types (i.e. bools to ints, etc.).
        if val is None:
            self.write_null(key, stm)
        # NOTE: Check against 'bool' BEFORE 'int'; otherwise, bool values might
        # first compare as an int insteead.
        elif isinstance(val, bool):
            self.write_bool(key, val, stm)
        elif isinstance(val, int):
            if isinstance(val, types.Int32):
                self.write_int32(key, val, stm)
            elif isinstance(val, types.Int64):
                self.write_int64(key, val, stm)
            elif isinstance(val, types.UInt64):
                self.write_uint64(key, val, stm)
            # Handle ordinary integers by casting down to the smallest type
            # that can contain them. If the caller wanted an explicit type,
            # they can use the explicit types given above.
            elif val < util.INT32_LOWERBOUND or val > util.INT32_UPPERBOUND:
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
            self.write_bytes(key, val, stm)
        else:
            self.write_unknown_object(key, val, stm)

    def write_int32(self, key, val, stm):
        """Write out an Int32 to the stream with the given key and value."""
        stm.write(b'\x10')
        self._write_raw_key(key, stm)
        stm.write(util.INT32_STRUCT.pack(val))

    def write_int64(self, key, val, stm):
        """Write out an Int64 to the stream with the given key and value."""
        stm.write(b'\x12')
        self._write_raw_key(key, stm)
        stm.write(util.INT64_STRUCT.pack(val))

    def write_uint64(self, key, val, stm):
        """Write out an UInt64 to the stream with the given key and value."""
        stm.write(b'\x12')
        self._write_raw_key(key, stm)
        stm.write(util.UINT64_STRUCT.pack(val))

    def write_float(self, key, val, stm):
        """Write out a double to the stream with the given key and value."""
        stm.write(b'\x01')
        self._write_raw_key(key, stm)
        stm.write(util.DOUBLE_STRUCT.pack(val))

    def write_bool(self, key, val, stm):
        """Write out a boolean to the stream with the given key and value."""
        stm.write(b'\x08')
        self._write_raw_key(key, stm)
        stm.write(b'\x01' if val else b'\x00')

    def write_null(self, key, stm):
        """Write out NULL (None) to the stream with the given key."""
        stm.write(b'\x0A')
        self._write_raw_key(key, stm)

    def write_min_key(self, key, stm):
        """Write out the 'min key' to the stream."""
        stm.write(b'\xFF')
        self._write_raw_key(key, stm)

    def write_max_key(self, key, stm):
        """Write out the 'max key' to the stream."""
        stm.write(b'\x7F')
        self._write_raw_key(key, stm)

    def write_datetime(self, key, dt, stm):
        """Write out a datetime to the stream with the given key and value."""
        stm.write(b'\x09')
        self._write_raw_key(key, stm)
        utc_ts = int(1000 * dt.timestamp())
        stm.write(util.INT64_STRUCT.pack(utc_ts))

    def write_string(self, key, val, stm):
        """Write out a (UTF-8) string to the stream with the given key."""
        stm.write(b'\x02')
        self._write_raw_key(key, stm)
        # This should only really be called with 'str' types, but we'll
        # be generous for now.
        raw_str = val.encode('utf-8') if isinstance(val, str) else val
        # NOTE: The length is in bytes _not_ UTF-8 characters.
        length = len(raw_str) + 1  # Add one for the \x00 at the end.
        stm.write(util.INT32_STRUCT.pack(length))
        stm.write(raw_str)
        # Write the null-terminator character.
        stm.write(b'\x00')

    def write_uuid(self, key, val, stm):
        """Write out a UUID to the stream with the given key and value."""
        # UUIDs are written as a binary type, with a 0x04 subtype.
        self.write_bytes(key, val.bytes, stm, subtype=4)

    def write_bytes(self, key, val, stm, subtype=0):
        """Write out the byte stream with the given key and value.

        NOTE: This also permits writing out a custom binary subtype to denote
        which type of binary stream this is. If not specified, this defaults
        to 0, the default/generic subtype.
        """
        stm.write(b'\x05')
        self._write_raw_key(key, stm)
        stm.write(util.INT32_STRUCT.pack(len(val)))
        stm.write(util.BYTE_STRUCT.pack(subtype))
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
