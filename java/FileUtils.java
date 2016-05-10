import java.io.File;

public class FileUtils {
    public static void delete(String path) {
        File f = new File(path);
        f.delete();
    }
}

