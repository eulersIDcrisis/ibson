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
"""decoder.py.

Decoding utilities for BSON.

This defines the primary ``BSONDecoder`` class that handles decoding some
BSON document into a python dictionary. It supports some common features to
control exactly how some objects should be interpreted, as well as handling
for some custom types.

In order to support a wider variety of use-cases, this uses functional-style
programming in a few places for traversing the BSON document structure. This
can enable "scanning/searching" operations of large BSON documents without
requiring the caller to decode the entire document. This also manages the
parsing stack externally (i.e. does NOT use recursion), so that deeply nested
BSON documents can still be parsed without any recursion overflow issues (of
course, there are still possible memory issues for the external stack itself,
but this is substantially larger in most cases as the appropriate memory is
allocated on the heap).

In order to work effectively, the underlying stream that is passed into the
decoder should be seekable; this is usually an acceptable requirement for most
uses; large BSON documents, for example, will most likely be stored as a file,
which should be seekable for most OS's.
If for some reason the underlying stream is _not_ seekable (i.e. reading from
a socket), then the caller should then first load the contents into memory
(i.e. via ``io.BytesIO()`` or similar), which will then be seekable. This is
no worse than what would be required anyway.
"""
import io
import uuid
import datetime
from collections import namedtuple, deque

# Local imports.
import ibson.codec_util as util
import ibson.errors as errors

# element     ::=     "\x01" e_name double    64-bit binary floating point
#     |   "\x02" e_name string    UTF-8 string
#     |   "\x03" e_name document  Embedded document
#     |   "\x04" e_name document  Array
#     |   "\x05" e_name binary    Binary data
#     |   "\x06" e_name   Undefined (value) — Deprecated
#     |   "\x07" e_name (byte*12)     ObjectId
#     |   "\x08" e_name "\x00"    Boolean "false"
#     |   "\x08" e_name "\x01"    Boolean "true"
#     |   "\x09" e_name int64     UTC datetime
#     |   "\x0A" e_name   Null value
#     |   "\x0B" e_name cstring cstring   Regular expression - The first cstring is the regex pattern, the second is the regex options string. Options are identified by characters, which must be stored in alphabetical order. Valid options are 'i' for case insensitive matching, 'm' for multiline matching, 'x' for verbose mode, 'l' to make \w, \W, etc. locale dependent, 's' for dotall mode ('.' matches everything), and 'u' to make \w, \W, etc. match unicode.
#     |   "\x0C" e_name string (byte*12)  DBPointer — Deprecated
#     |   "\x0D" e_name string    JavaScript code
#     |   "\x0E" e_name string    Symbol. — Deprecated
#     |   "\x0F" e_name code_w_s  JavaScript code w/ scope — Deprecated
#     |   "\x10" e_name int32     32-bit integer
#     |   "\x11" e_name uint64    Timestamp
#     |   "\x12" e_name int64     64-bit integer
#     |   "\x13" e_name decimal128    128-bit decimal floating point
#     |   "\xFF" e_name Min key
#     |   "\x7F" e_name Max key

def parse_64bit_float(stm):
    buff = stm.read(util.DOUBLE_STRUCT.size)
    return util.DOUBLE_STRUCT.unpack(buff)[0]


def parse_int32(stm):
    buff = stm.read(util.INT32_STRUCT.size)
    return util.INT32_STRUCT.unpack(buff)[0]


def parse_int64(stm):
    buff = stm.read(util.INT64_STRUCT.size)
    return util.INT64_STRUCT.unpack(buff)[0]


def parse_uint64(stm):
    buff = stm.read(util.UINT64_STRUCT.size)
    return util.UINT64_STRUCT.unpack(buff)[0]


def parse_byte(stm):
    buff = stm.read(util.BYTE_STRUCT.size)
    return util.BYTE_STRUCT.unpack(buff)[0]


def _scan_for_null_terminator(buff):
    for i, x in enumerate(buff):
        if x == 0:
            return i
    return -1


def parse_ename(stm, decode=True):
    """Parse out a C-string (null-terminated string).

    If 'decode=True' (default), then convert the parsed string into UTF-8
    automatically.
    """
    index = -1
    data = bytearray()
    while True:
        # Peek the data, but do not (yet) consume it.
        peeked_data = stm.read(1024)
        # Scan for the null-terminator.
        index = _scan_for_null_terminator(peeked_data)
        # Did not find the null terminator. Add this data to the current
        # buffer, update the stream's position, then run through again.
        if index < 0:
            # Should be the same data read by 'stm.peek()'.
            data.extend(peeked_data)
            continue
        # Otherwise, only read in the amount of data required to get to the
        # null-terminator.
        data.extend(peeked_data[:index + 1])

        # Seek back the number of bytes we did not end up reading.
        stm.seek(index - len(peeked_data) + 1, 1)
        break

    # The last character _should_ be the null-terminator. Assert that this is
    # so, but then remove the character when decoding.
    assert data[-1] == 0x00
    data.pop()

    # 'index' stores the index of the null terminator, or -1 if it was not
    # found. Realistically, this should be positive to include at least one
    # character. The current contents that were parsed are stored into 'data',
    # which we can now encode as a string (if requested).
    if decode:
        try:
            return data.decode('utf-8')
        except Exception:
            pass
    return data


def parse_utf8_string(stm):
    """Parse out a UTF-8 string from the stream."""
    buff = stm.read(util.INT32_STRUCT.size)
    length, = util.INT32_STRUCT.unpack(buff)
    # Read 'length' bytes.
    data = stm.read(length)
    # The last byte _should_ be the null-terminator.
    assert data[length - 1] == 0, "Last byte not the null-terminator!"

    # Decode this data as UTF-8.
    return data[:-1].decode('utf-8')


def parse_bool(stm):
    buff = stm.read(util.BYTE_STRUCT.size)
    if buff[0] == 0x00:
        return False
    elif buff[0] == 0x01:
        return True
    # Should never happen.
    raise Exception("Invalid bool type parsed!")


def parse_binary(stm):
    buff = stm.read(util.INT32_STRUCT.size)
    length, = util.INT32_STRUCT.unpack(buff)
    buff = stm.read(util.BYTE_STRUCT.size)
    subtype, = util.BYTE_STRUCT.unpack(buff)

    # Read exactly 'length' bytes after.
    data = stm.read(length)

    # Handle UUID implicitly.
    if subtype in [0x03, 0x04]:
        return uuid.UUID(bytes=data)
    return data


def parse_null(stm):
    return None, 0


def parse_utc_datetime(stm):
    buff = stm.read(util.INT64_STRUCT.size)
    utc_ms, = util.INT64_STRUCT.unpack(buff)
    result = datetime.datetime.fromtimestamp(utc_ms / 1000.0)
    return result


def parse_datetime(stm):
    return parse_utc_datetime(stm)


ELEMENT_OPCODES = {
    0x01: parse_64bit_float,
    0x02: parse_utf8_string,
    # 0x03: _parse_document,
    # 0x04: _parse_array,
    0x05: parse_binary,
    # 0x06: _handle_undefined,
    # 0x07: _parse_object_id,
    0x08: parse_bool,
    0x09: parse_utc_datetime,
    0x0A: parse_null,
    # 0x0B: _parse_regex,
    # 0x0C: _parse_db_pointer,
    # 0x0D: _parse_js_code,
    # 0x0E: _parse_symbol,
    # 0x0F: _parse_js_code_with_scope,
    0x10: parse_int32,
    0x11: parse_datetime,
    0x12: parse_int64,
    # 0x13: _parse_decimal128,
    # 0xFF: _parse_min_key,
    # 0x7F: _parse_max_key
}


class DecodeEvents(object):
    """Placeholder class for events when decoding a BSON document."""

    NESTED_DOCUMENT = object()
    """Event that denotes the start of a nested document with the given key.

    NOTE: The end of this nested document is flagged by an 'END_DOCUMENT'
    event.
    """

    NESTED_ARRAY = object()
    """Event that denotes the start of a nested array with the given key.

    NOTE: The end of this nested document if flagged by an 'END_DOCUMENT'
    event.
    """

    END_DOCUMENT = object()
    """Event that denotes the end of a nested document or array."""

    SKIP_KEY = object()
    """Event that denotes to skip the current key."""


class BSONScanner(object):

    def __init__(self, min_key_object=None, max_key_object=None, null_object=None):
        # By default, initialize the opcode mapping here. Subclasses should
        # register this mapping using the helper call to:
        # - register_opcode(opcode, callback)
        #
        # By default, most of the common types are already implemented, and
        # this class's constructor arguments handle some common cases.
        self._opcode_mapping = {
            0x01: parse_64bit_float,
            0x02: parse_utf8_string,
            # 0x03: _parse_document,
            # 0x04: _parse_array,
            0x05: self.scan_binary,
            0x06: self.scan_undefined,
            # 0x07: _parse_object_id,
            0x08: parse_bool,
            0x09: parse_utc_datetime,
            # 0x0A implies 'NULL', so return the configured NULL object.
            0x0A: lambda stm: null_object,
            # 0x0B: _parse_regex,
            # 0x0C: _parse_db_pointer,
            # 0x0D: _parse_js_code,
            # 0x0E: _parse_symbol,
            # 0x0F: _parse_js_code_with_scope,
            0x10: parse_int32,
            0x11: parse_datetime,
            0x12: parse_int64,
            # 0x13: _parse_decimal128,
            # Return the min/max objects for these opcodes.
            0x7F: lambda stm: max_key_object,
            0xFF: lambda stm: min_key_object,
        }

    def register_opcode(self, opcode, callback):
        """Register a custom callback to parse this opcode.

        NOTE: 'callback' is expected to have the signature:
            callback(stm) -> result
        """
        self._opcode_mapping[opcode] = callback

    def scan_binary(self, stm):
        return parse_binary(stm)

    def iterdecode(self, stm):
        """Iterate over the given BSON stream and (incrementally) decode it.

        This returns a generator that yields tuples of the form:
            (frame, key, value)
        where:
         - frame: The current frame as a DocumentFrame.
         - key: The key pertaining to this frame.
         - value: The parsed value

        It is possible to request to "skip" decoding a frame by sending the
        special DecodeEvents.SKIP_KEYS object back to this generator. In this
        case, it is NOT strictly guaranteed that the frame will be skipped!
        Rather, it is a hint to the generator that it can skip the next key if
        desired. This feature is useful to skip decoding nested documents when
        searching for a specific key (for example) and can hint to the system
        when it is okay to skip reading.
        """
        # The stream should be seekable. If it isn't, then it should be wrapped
        # appropriately using 'io' module utilities.
        #
        # Get the current position in the stream.
        fpos = stm.tell()

        # The first field in any BSON document is its length. Fetch that now.
        length = parse_int32(stm)
        # The root key is the empty key.
        curr_frame = util.DocumentFrame('', fpos, length, False)

        # Initialize the stack with the root document.
        current_stack = deque()
        current_stack.append(curr_frame)

        # Start with the first 'yield' for the entire document.
        client_req = (yield (
            current_stack[-1], '', DecodeEvents.NESTED_DOCUMENT
        ))

        while current_stack:
            # Peek the current stack frame, which is at the end of the stack.
            frame = current_stack[-1]

            # A 'frame' consists of:
            #   <opcode> + <null-terminated key> + <value>
            # Parse that here.
            opcode = parse_byte(stm)

            # If 'opcode' is zero, this implies the end of this document.
            if opcode == 0x00:
                current_stack.pop()
                client_req = (yield (
                    frame, frame.key, DecodeEvents.END_DOCUMENT))
                continue

            # Parse the key for the next element.
            key = parse_ename(stm)

            # Check the 'nested document' case first.
            client_req = None
            if opcode in [0x03, 0x04]:
                nested_fpos = stm.tell()
                # A nested array. Create a new DocumentFrame type and push it
                # to the current stack.
                length = parse_int32(stm)
                is_array = bool(opcode == 0x04)
                if is_array:
                    result = DecodeEvents.NESTED_ARRAY
                else:
                    result = DecodeEvents.NESTED_DOCUMENT

                # These given two opcodes imply nested documents. If the caller
                # responded with a request of "SKIP_KEY", then skip the key and
                # do not bother parsing any of those frames; instead, seek past
                # the perceived length of the document.
                client_req = (yield (frame, key, result))
                if client_req is DecodeEvents.SKIP_KEY:
                    # Seek ahead based on the parsed length.
                    stm.seek(length, 1)
                else:
                    # Create a new frame, with current_frame as the parent.
                    new_frame = util.DocumentFrame(
                        key, nested_fpos, length, is_array, parent=frame
                    )
                    current_stack.append(new_frame)
                continue

            # Depending on opcode, make the appropriate callback otherwise.
            result = self.process_opcode(opcode, stm, current_stack)

            # Confusing. Basically, the client can 'signal' a few operations to
            # this generator by calling the '.send()' method. If no '.send()'
            # is called and the caller just iterates over the given contents,
            # then 'client_req' will be 'None'.
            # If 'client_req' is something other than None, try and process a
            # few cases.
            client_req = (yield (frame, key, result))

    def process_opcode(self, opcode, stm, traversal_stk):
        """Process the given opcode and return the appropriate value.

        The result of this operation depends on the opcode.
        """
        callback = self._opcode_mapping.get(opcode)
        if not callback:
            raise Exception("Invalid opcode: {}".format(opcode))
        return callback(stm)


class BSONDecoder(BSONScanner):
    """Basic BSONDecoder object that decodes a BSON byte stream.

    This decoder is designed to decode the stream into a python 'dict'. Some
    of the common BSON types are decoded as expected, such as:
     - UUIDs
     - datetime
     - strings (as UTF-8)

    More customized objects can be handled as well by registering the proper
    handlers via: 'register_opcode()'
    which should parse out custom opcode types.
    """

    def loads(self, data):
        with io.BytesIO(data) as stm:
            return self.load(stm)

    def load(self, stm):
        generator = self.decode(stm)

        frame = None
        for frame, key, val in generator:
            if val is DecodeEvents.NESTED_DOCUMENT:
                frame.ext_data = dict()
            elif val is DecodeEvents.NESTED_ARRAY:
                frame.ext_data = []
            elif val is DecodeEvents.END_DOCUMENT:
                continue
            else:
                frame.ext_data[key] = val
        if not frame:
            raise BSONDecodeError('Invalid end state!')

        if frame:
            return frame.ext_data
        return None
