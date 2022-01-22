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

    @property
    def fpos(self):
        """Return the position in the stream that the error pertains to.

        NOTE: This can return None if the error does not pertain to the
        stream position or if it otherwise could not be extracted.
        """
        return self._fpos

    def __str__(self):
        """Return this exception as a string."""
        msg = super(BSONEncodeError, self).__str__()
        if self._fpos is not None:
            return u'Encode key: {}, fpos: {} -- {}'.format(
                self.key, self.fpos, msg)
        return u'Encode key: {} -- {}'.format(self.key, msg)


class BSONDecodeError(BSONError):
    """Exception raised while decoding the stream."""


class InvalidBSONOpcode(BSONDecodeError):
    """Exception denoting an invalid BSON opcode."""

    def __init__(self, opcode):
        msg = "Invalid opcode encountered: {}".format(opcode)
        super(InvalidBSONOpcode, self).__init__(msg)
