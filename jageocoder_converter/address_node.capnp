@0xe814104a96261278;

struct AddressNode {
    id @0 :UInt32;
    name @1 :Text;
    nameIndex @2 :Text;
    x @3 :Float32;
    y @4 :Float32;
    level @5 :Int8;
    priority @6 :Int8;
    note @7 :Text;
    parentId @8 :UInt32;
    dataset @9 :UInt8;
    siblingId @10 :UInt32;
}

struct AddressNodeList {
    nodes @0 :List(AddressNode);
}
