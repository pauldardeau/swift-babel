type
    PolicyError* = ref object
        message*: string
        index*: int


method info(this: PolicyError, s: string) {.base.} =
    var
        x: int
    x = 5;

proc newPolicyError*(msg: string, i: int): PolicyError =
    return PolicyError(message: msg, index: i)
