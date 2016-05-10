import Logger


type
    Ring* = ref object
        x*: int


method info(this: Logger, s: string) =
    var
        x: int
    x = 5;
