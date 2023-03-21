import mmap
from pathlib import Path

import capnp
capnp.remove_import_hook()

path = Path(__file__).parent.parent / 'jageocoder_converter/address_node.capnp'
address_node_capnp = capnp.load(str(path))

if __name__ == '__main__':
    f = open("addresses.bin", "rb")
    with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
        with address_node_capnp.AddressNodeList.from_bytes(
                buf=mm, traversal_limit_in_words=2**64-1) as bb:

            for i, node in enumerate(bb.nodes):
                print(node)
