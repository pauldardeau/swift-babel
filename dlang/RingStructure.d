import std.conv;
import std.json;

import StorageDevice;


class RingStructure {

public:
    StorageDevice[] devs;
    int part_shift;
    int[][] replica2part2dev_id;


    static short shortFromNetBytes(byte[] b) {
        short s = 0;
        s += b[0] << 8;
        s += b[1];
        return s;
    }

    static int intFromNetBytes(byte[] b) {
        int i = 0;
        i += b[0] << 24;
        i += b[1] << 16;
        i += b[2] << 8;
        i += b[3];
        return i;
    }

    static byte[] toNetBytes(short s) {
        byte[] b = new byte[2];
        b[0] = cast(byte)(s & 0xff);
        b[1] = cast(byte)((s >> 8) & 0xff);
        return b;
    }

    static byte[] toNetBytes(int i) {
        byte[] b = new byte[4];
        b[0] = cast(byte)(i & 0xff);
        b[1] = cast(byte)((i >> 8) & 0xff);
        b[2] = cast(byte)((i >> 16) & 0xff);
        b[3] = cast(byte)((i >> 24) & 0x0ff);
        return b;
    }

    static string toString(int[] part2dev_id) {
        return ""; //TODO: PJD
    }

    void serialize_v1(string filename) {
        GZIPOutputStream gz_file = null;

        try {
            // Write out new-style serialization magic and version:
            gz_file.write("R1NG".getBytes(), 0, 4);
            ushort ring_version = 1;
            byte[] version_bytes = RingStructure.toNetBytes(ring_version);
            gz_file.write(version_bytes, 0, 2);

            JSONObject json_encoder = new JSONObject();
            json_encoder.put("devs", devs);
            json_encoder.put("part_shift", part_shift);
            json_encoder.put("replica_count",
                             replica2part2dev_id.length);
            string json_text = json_encoder.toString();
            int json_length = cast(int)(json_text.length);
            byte[] json_len_bytes = RingStructure.toNetBytes(json_length);
            gz_file.write(json_len_bytes, 0, 4);
            gz_file.write(json_text.getBytes(), 0, json_length);

            foreach (part2dev_id; replica2part2dev_id) {
                string s = RingStructure.toString(part2dev_id);
                gz_file.write(s.getBytes(), 0, s.length());
            }
        } finally {
            if (gz_file != null) {
                gz_file.close();
            }
        }
    }

    static RingStructure deserialize_v1(string filename) {
        return RingStructure.deserialize_v1(filename, false);
    }

    static RingStructure deserialize_v1(string filename,
                                        bool metadata_only) {

        GZIPInputStream gz_file = null;
        RingStructure ring_structure = null;

        try {
            gz_file =
                new GZIPInputStream(new FileInputStream(filename));

            // See if the file is in the new format
            string magic = gz_file.read(4);
            if (magic == "R1NG") {
                byte[] format_version_bytes = new byte[2];
                int format_version = 0;

                gz_file.read(format_version_bytes, 0, 2);
                format_version = intFromNetBytes(format_version_bytes);
                if (format_version == 1) {
                    ring_structure =
                        RingStructure.deserialize_v1(gz_file, metadata_only);
                } else {
                    throw new Exception("Unknown ring format version " ~
                                    to!string(format_version));
                }
            }
        } finally {
            if (gz_file != null) {
                gz_file.close();
            }
        }

        return ring_structure;
    }
}

