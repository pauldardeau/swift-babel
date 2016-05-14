

/**
 * Partitioned consistent hashing ring data (used for serialization).
 */
class RingData {

    _ring: RingStructure
    devs: Array[StorageDevice]
    _replica2part2dev_id: Array[Array[Int]]
    _part_shift: Int = 0


    /**
     *
     * @param replica2part2dev_id
     * @param devs
     * @param part_shift
     */
    public RingData(RingStructure ring) {
        this._ring = ring;
    }

    /**
     *
     * @param gz_file
     * @return
     */
    public static RingStructure deserialize_v1(GZIPInputStream gz_file) {
        return RingData.deserialize_v1(gz_file, false);
    }

    /**
     * Deserialize a v1 ring file into a dictionary with `devs`, `part_shift`,
     *  and `replica2part2dev_id` keys.
     *
     * If the optional kwarg `metadata_only` is True, then the
     *  `replica2part2dev_id` is not loaded and that key in the returned
     *  dictionary just has the value `[]`.
     *
     * @param file gz_file An opened file-like object which has already
     *                       consumed the 6 bytes of magic and version.
     * @param bool metadata_only: If True, only load `devs` and `part_shift`
     * @return A dict containing `devs`, `part_shift`, and
     *            `replica2part2dev_id`
     */
    public static RingStructure deserialize_v1(GZIPInputStream gz_file, boolean metadata_only) {

        return RingStructure.deserialize_v1(filename);
    }

    /**
     *
     * @param filename
     * @return
     */
    public static RingData load(String filename) {
        return RingData.load(filename, false);
    }

    /**
     * Load ring data from a file.
     *
     * @param filename Path to a file serialized by the save() method.
     * @param metadata_only If true, only load `devs` and `part_shift`.
     * @return A RingData instance containing the loaded data.
     */
    public static RingData load(String filename, boolean metadata_only) {
        return RingStructure.deserialize_v1(filename);
    }

    /*
    public void serialize_v1(file_obj) {
        this.ring.serialize_v1(filename);
    }
    */

    /*
    public void save(String filename) {
        save(filename, 1300507380);
    }
    */

    /**
     * Serialize this RingData instance to disk.
     *
     * @param filename File into which this instance should be serialized.
     * @param mtime time used to override mtime for gzip, default or None
     *                 if the caller wants to include time
     */
    /*
    public void save(String filename, mtime=1300507380.0) {
        // Override the timestamp so that the same ring data creates
        // the same bytes on disk. This makes a checksum comparison a
        // good way to see if two rings are identical.
        tempf = NamedTemporaryFile(dir=".", prefix=filename, delete=false);
        gz_file = GzipFile(filename, mode="wb", fileobj=tempf, mtime=mtime);
        this.serialize_v1(gz_file);
        gz_file.close();
        tempf.flush();
        os.fsync(tempf.fileno());
        tempf.close();
        os.chmod(tempf.name, 0o644);
        os.rename(tempf.name, filename);
    }
    */

    /**
     *
     * @return
     */
    /*
    public Map<String,String> to_dict() {
        return {"devs": this.devs,
                "replica2part2dev_id": this._replica2part2dev_id,
                "part_shift": this._part_shift}
    }
    */
}

