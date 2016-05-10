import java.io.File;

class FileUtils {
    def static delete(path: String) {
        File f = new File(path);
        f.delete();
    }
}

