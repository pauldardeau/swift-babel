import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.json.JSONObject;


public class RingStructure {

    public ArrayList<StorageDevice> devs;
    public int part_shift;
    public int[][] replica2part2dev_id;


    public static byte[] toBytes(short s) {
        byte[] b = new byte[2];
        b[0] = (byte)(s & 0xff);
        b[1] = (byte)((s >> 8) & 0xff);
        return b;
    }

    public static byte[] toBytes(int i) {
        byte[] b = new byte[4];
        b[0] = (byte)(i & 0xff);
        b[1] = (byte)((i >> 8) & 0xff);
        b[2] = (byte)((i >> 16) & 0xff);
        b[3] = (byte)((i >> 24) & 0x0ff);
        return b;
    }

    public static String toString(int[] part2dev_id) {
        return ""; //TODO: PJD
    }

    public void serialize_v1(String filename) {
        GZIPOutputStream gz_file = null;

        try {
            // Write out new-style serialization magic and version:
            gz_file.write("R1NG".getBytes(), 0, 4);
            short version = 1;
            byte[] version_bytes = RingStructure.toBytes(version);
            gz_file.write(version_bytes, 0, 2);

            JSONObject json_encoder = new JSONObject();
            json_encoder.put("devs", devs);
            json_encoder.put("part_shift", part_shift);
            json_encoder.put("replica_count",
                             replica2part2dev_id.length);
            String json_text = json_encoder.toString();
            int json_length = json_text.length();
            byte[] json_len_bytes = RingStructure.toBytes(json_length);
            gz_file.write(json_len_bytes, 0, 4);
            gz_file.write(json_text.getBytes(), 0, json_length);

            for (int[] part2dev_id : replica2part2dev_id) {
                String s = RingStructure.toString(part2dev_id);
                gz_file.write(s.getBytes(), 0, s.length());
            }
        } finally {
            if (gz_file != null) {
                gz_file.close();
            }
        }
    }

    public static RingStructure deserialize_v1(String filename) {
        GZIPInputStream gz_file = null;
        RingStructure ring_structure = null;

        try {
            gz_file =
                new GZIPInputStream(new FileInputStream(filename));

            // See if the file is in the new format
            String magic = gz_file.read(4);
            if (magic.equals("R1NG")) {
                byte[] format_version_bytes = new byte[2];
                int format_version = 0;

                if (2 == gz_file.read(format_version_bytes, 0, 2)) {
                    format_version = struct.unpack("!H", gz_file.read(2));
                    if (format_version == 1) {
                        ring_structure =
                            RingStructure.deserialize_v1(gz_file, metadata_only);
                    } else {
                        throw new Exception("Unknown ring format version " +
                                        format_version);
                    }
                }
            }
        } finally {
            if (gz_file != null) {
                gz_file.close();
            }
        }

        if (!hasattr(ring_data, "devs")) {
            ring_data = new RingData(ring_data["replica2part2dev_id"],
                                     ring_data["devs"],
                                     ring_data["part_shift"]);
        }

        return ring_structure;
    }
}

