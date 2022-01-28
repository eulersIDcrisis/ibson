# ibson

BSON (Binary JSON) parsing library.

## Usage

This library is designed to implement a basic BSON library with an interface
that is similar to python's native JSON parsing library. In particular, this
has expected usage:
```python
import ibson


obj = {
    "a": {
        "b": [1, 2, 3],
        "uuid": uuid.uuid1()
    }
}

buffer = ibson.dumps(obj)
new_obj = ibson.loads(buffer)

# Evaluates as 'True'
new_obj == obj
```

This mimics the existing `bson` library for python, but also permits reading
from and writing to (seekable) streams and files as well:
```python

with open('file.bson', 'wb') as stm:
    ibson.dump(obj, stm)

# Elsewhere
with open('file.bson', 'rb') as stm:
    new_obj = ibson.load(stm)

# Should evaluate True
new_obj == obj
```
NOTE: It is important that the file is opened in binary mode, not text mode!

Under the hood, this library is designed in a similar manner as a SAX-style
event-driven parser; it avoids explicit recursion wherever possible and has
calls that permit iterating over the contents using generators with an
interface that can even permit skipping keys/fields altogether. Since the
parsing stack is maintained separately, it can also be used to verify and
attempt to fix some issues.

## How It Works

This library works by noting that the byte offset needed in a few places to
(de)serialize BSON is already implicitly tracked in seekable streams via the
call to: `fp.tell()`, omitting the need to track the byte counts directly.
In places where these byte counts are not directly accessible, the caller is
likely already loading the content into a bytearray or binary stream that can
become seekable anyway. When this field is needed before the value is actually
available (i.e. the `length` of a document before the document is written),
this simply registers the position the length needs to be written, writes out
a placeholder value (0), then retroactively writes out these lengths when they
finally are known, hence the need for the writable stream to also be seekable.
(As a slight optimization, these lengths are sorted and written from the start
to the end of the file again when the encoder is done to effectively make to
sequential passes instead of an arbitrary number of random-access passes.)

This library also strives to reduce memory-consumption as best as reasonable
with an iterative parser, intentionally avoiding recursion where possible; the
parser tracks the stack on the heap and also stores various fields internally
so as to avoid loading everything parsed into memory when just traversing the
document, in a manner analogous to SAX-style parsers for XML. (When decoding
and storing the document as a python `dict`, yes, that will be in memory.)
