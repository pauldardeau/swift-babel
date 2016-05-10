import tables

type
    Config* = ref object
        dict*: Table[string, string]


method get*(this: Config, key: string): string {.base.} =
    return this.dict[key];

method get*(this: Config, key: string, default_value: string): string {.base.} =
    return this.dict[key];

