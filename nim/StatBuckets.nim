import tables

type
    StatBuckets* = object
        stats_buckets*: Table[int, int64]
        over_counter*: int64


