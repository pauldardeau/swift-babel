import tables

type
    StatBuckets*: ref object
        stats_buckets*: Table[int, long]
        over_counter*: long


