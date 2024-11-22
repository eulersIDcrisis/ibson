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
from ibson.encoder import BSONEncoder
from ibson.decoder import BSONDecoder


def dump(obj, stm, /, **kwargs):
    """Write the object out to the given seekable binary stream.

    NOTE: Any additional keyword arguments are passed to the underlying
    BSONEncoder constructor.

    Parameters
    ----------
    obj: dict
        A dictionary object containing the contents to write out.
    """
    encoder = BSONEncoder(**kwargs)
    return encoder.dump(obj, stm)


def dumps(obj, /, **kwargs):
    """Write the object out to a bytes array.

    Parameters
    ----------
    obj: dict
        A dictionary object containing the contents to write out.

    NOTE: Any additional keyword arguments are passed to the underlying
    BSONEncoder constructor.

    Returns
    -------
    bytes: The bytes corresponding to the encoded BSON document.
    """
    encoder = BSONEncoder(**kwargs)
    return encoder.dumps(obj)


def load(stm, /, cls=BSONDecoder, **kwargs):
    decoder = None
    try:
        decoder = cls(stm, **kwargs)
        return decoder.decode()
    finally:
        if decoder:
            decoder.close()


def loads(data, cls=BSONDecoder, **kwargs):
    decoder = None
    try:
        decoder = cls(data, **kwargs)
        return decoder.decode()
    finally:
        if decoder:
            decoder.close()
