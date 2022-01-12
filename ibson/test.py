import io
from pybson import encoder, decoder

with open('pybson/test.bson', 'rb') as stm:
    decoder.decode_document(None, stm)

# stm = io.BytesIO()
# x = dict(a=dict(nested=['asdf']), b=1, c=3.4)
# encoder.encode_document(None, x, stm)
# stm.getvalue()
# # Try and decode?
# stm.seek(0)
# decoder.decode_document(None, stm)
