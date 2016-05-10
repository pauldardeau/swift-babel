import RingStructure;


/**
 * Partitioned consistent hashing ring data (used for serialization).
 */
class RingData {

private:
    private RingStructure ring_structure;
    //private Object devs;
    //private Object _replica2part2dev_id;
    //private Object _part_shift;


public:
    /**
     *
     * @param replica2part2dev_id
     * @param devs
     * @param part_shift
     */
    this(RingStructure ring_structure) {
        this.ring_structure = ring_structure;
    }

    static RingData load(string filename) {
        return RingData.load(filename, false);
    }

    /**
     * Load ring data from a file.
     *
     * @param filename Path to a file serialized by the save() method.
     * @param metadata_only If true, only load `devs` and `part_shift`.
     * @return A RingData instance containing the loaded data.
     */
    static RingData load(string filename, bool metadata_only) {
        return RingStructure.deserialize_v1(filename);
    }

    /**
     * Serialize this RingData instance to disk.
     *
     * @param filename File into which this instance should be serialized.
     * @param mtime time used to override mtime for gzip, default or None
     *                 if the caller wants to include time
     */
    /*
    void save(string filename, mtime=1300507380.0) {
    }
    */
}

