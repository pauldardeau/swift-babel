import tables

include StoragePolicy


type
    StoragePolicyCollection* = ref object
        default_policy*: StoragePolicy
        by_name*: Table[string, StoragePolicy]
        by_index*: Table[int, StoragePolicy]


proc get_by_name(name: string): StoragePolicy =
    return nil

proc get_by_index(index: int): StoragePolicy =
    return nil
