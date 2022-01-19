# ibson

BSON (Binary JSON) parsing library.

## Usage

This library is designed to implement a basic BSON library that behaves
similarly to python's native JSON parsing library. In particular, this has
expected usage:
```python
import ibson


obj = {
    "a": {
        "b": [1, 2, 3],
        "dt": datetime.utcnow(),
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

## How It Works

This library works by noting that the byte offset needed in a few places to
(de)serialize BSON is already implicitly tracked in (seekable) streams via
the call to: `fp.tell()`; instead of maintaining this value directly, the same
information can be extracted internally when needed by calculating differences
in these stream positions.
