from pathlib import Path
import random

import capnp
from jageocoder_converter.capnp_manager import CapnpTable

capnp.remove_import_hook()
path = Path(__file__).parent.parent / 'jageocoder_converter/address_node.capnp'
address_node_capnp = capnp.load(str(path))


def print_node(node):
    while True:
        print(
            "[{}]{}->{}".format(node.id, node.name, node.siblingId),
            end=""
        )
        if node.parentId == 0:
            print()
            break

        print(" | ", end="")
        node = capnp_table.get_record(pos=node.parentId)


if __name__ == '__main__':
    capnp_table = CapnpTable(
        dbdir=Path(__file__).parent.parent / "hokkaido",
        tablename="address_node"
    )
    config = capnp_table.get_config()

    # for node in capnp_table.retrieve_records(limit=100, offset=0):
    #     print_node(node)

    random.seed()
    for _ in range(10000):
        node = capnp_table.get_record(
            pos=random.randrange(config["length"]))
        print_node(node)
