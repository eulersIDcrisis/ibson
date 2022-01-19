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
"""errors.py.

Exceptions for the ibson module.
"""


class BSONError(Exception):
    """General exception for BSON errors."""


class BSONEncodeError(BSONError):
    """Exception raised while encoding a document to a byte stream."""

    def __init__(self, key, msg, *args, fpos=None):
        super(BSONEncodeError, self).__init__(msg, *args)
        self._key = key
        self._fpos = fpos
        self._msg = msg

    def update_with_stack(self, stk):
        tentative_key = '.'.join([
            frame.key.replace('.', '\\.') for frame in stk
        ])
        self._key = '{}.{}'.format(tentative_key, self._key)

    @property
    def key(self):
        """Key this error pertains to (could be the empty string)."""
        return self._key

    def __str__(self):
        """Return this exception as a string."""
        msg = super(BSONEncodeError, self).__str__()
        return u'Encode key: {} -- {}'.format(self.key, msg)


class BSONDecodeError(BSONError):
    """Exception raised while decoding the stream."""


class InvalidBSONOpcode(BSONDecodeError):
    """Exception denoting an invalid BSON opcode."""

    def __init__(self, opcode):
        msg = "Invalid opcode encountered: {}".format(opcode)
        super(InvalidBSONOpcode, self).__init__(msg)
