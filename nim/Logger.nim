type
    LoggerObj* = object
        x*: int

    Logger* = ref LoggerObj


method info(this: Logger, s: string) {.base.} =
    var
        x: int
    x = 5;
