type
    StoragePolicy* = ref object
        class_name*: string
        idx*: int
        is_default*: bool
        is_deprecated*: bool
        ring_name*: string
        policy_type*: string
        alias_list*: seq[string]
        #object_ring*: Ring



proc is_default*(this: StoragePolicy): bool =
    return this.is_default
