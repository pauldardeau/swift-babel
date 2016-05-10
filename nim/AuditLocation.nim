
type
    AuditLocation* = ref object
        path*: string
        device*: string
        partition*: string
        policy*: int


proc toString*(this: AuditLocation): string =
    return "";

