#include "RingData.h"

using namespace std;


RingData RingData::load(const string& filename,
                        bool metadata_only) {
    gz_file = GzipFile(filename, "rb");
    // Python 2.6 GzipFile doesn't support BufferedIO
    if (hasattr(gz_file, "_checkReadable")) {
        gz_file = BufferedReader(gz_file);
    }

    // See if the file is in the new format
    magic = gz_file.read(4);
    if (magic == "R1NG") {
        int format_version, = struct.unpack("!H", gz_file.read(2));
        if (format_version == 1) {
            ring_data = cls.deserialize_v1(
                gz_file, metadata_only=metadata_only);
        } else {
            raise Exception("Unknown ring format version %d" %
                            format_version);
        }
    } else {
        // Assume old-style pickled ring
        gz_file.seek(0);
        ring_data = pickle.load(gz_file);
    }

    if (not hasattr(ring_data, "devs")) {
        ring_data = RingData(ring_data["replica2part2dev_id"],
                             ring_data["devs"], ring_data["part_shift"]);
    }
    return ring_data;
}
 
RingData::RingData(RingStructure ring) {
}

/*
void RingData::serialize_v1(FILE* file_obj) {
    // Write out new-style serialization magic and version
    file_obj.write(struct.pack("!4sH", "R1NG", 1));
    ring = this->to_dict();
    json_encoder = json.JSONEncoder(sort_keys=true);
    json_text = json_encoder.encode(
        {"devs": ring["devs"], "part_shift": ring["part_shift"],
         "replica_count": len(ring["replica2part2dev_id"])});
    json_len = len(json_text);
    file_obj.write(struct.pack("!I", json_len));
    file_obj.write(json_text);
    for (part2dev_id in ring["replica2part2dev_id"]) {
        file_obj.write(part2dev_id.tostring());
    }
}
*/

/*
void RingData::save(const string& filename) {
    // Override the timestamp so that the same ring data creates
    // the same bytes on disk. This makes a checksum comparison a
    // good way to see if two rings are identical.
    tempf = NamedTemporaryFile(dir=".", prefix=filename, delete=False);
    gz_file = GzipFile(filename, mode='wb', fileobj=tempf, mtime=mtime);
    self.serialize_v1(gz_file);
    gz_file.close();
    tempf.flush();
    os.fsync(tempf.fileno());
    tempf.close();
    os.chmod(tempf.name, 0o644);
    os.rename(tempf.name, filename);
}
*/

