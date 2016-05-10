
type
    RingData* = ref object
        ring*: RingStructure
        devs*: seq[StorageDevice]
        replica2part2dev_id*: int[][]
        part_shift*: int

