#####  COMPRESSION  ######
import bz2
import base64
import pickle

def compress(data):
    return bz2.compress(
        base64.b64encode(
            pickle.dumps(
                data
            )
        )
    )


def decompress(data):
    return pickle.loads(
        base64.b64decode(
            bz2.decompress(
                data
            )
        )
    )