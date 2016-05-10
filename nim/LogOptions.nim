import tables


type
    LogOptions* = ref object
        dict*: Table[string, string]


method get*(this: LogOptions, key: string): string {.base.} =
    return this.dict[key];

method get*(this: LogOptions, key: string, default_value: string): string {.base.} =
    return this.dict[key];

