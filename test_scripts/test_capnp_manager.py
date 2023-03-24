from pathlib import Path
import random

from PortableTab import CapnpTable


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
        db_dir=Path(__file__).parent.parent / "hokkaido",
        tablename="address_node"
    )

    for node in capnp_table.retrieve_records(
            limit=100, offset=0, as_dict=True):
        print_node(node)

    random.seed()
    for _ in range(10000):
        node = capnp_table.get_record(
            pos=random.randrange(capnp_table.count_records()))
        print_node(node)
