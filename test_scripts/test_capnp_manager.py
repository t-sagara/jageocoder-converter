from pathlib import Path
import random

from PortableTab import CapnpTable


address_nodes = None


def print_node(node):
    global address_nodes
    while True:
        print(
            "[{}]{}->{}".format(node.id, node.name, node.siblingId),
            end=""
        )
        if node.parentId == 0:
            print()
            break

        print(" | ", end="")
        node = address_nodes.get_record(pos=node.parentId)


if __name__ == '__main__':
    address_nodes = CapnpTable(
        db_dir=Path(__file__).parent.parent / "db_hokkaido",
        tablename="address_node"
    )

    for node in address_nodes.retrieve_records(
            limit=100, offset=0, as_dict=True):
        print_node(node)

    random.seed()
    for _ in range(10000):
        node = address_nodes.get_record(
            pos=random.randrange(address_nodes.count_records()))
        print_node(node)
