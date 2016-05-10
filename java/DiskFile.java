import java.util.ArrayList;


public class DiskFile extends BaseDiskFile {

    //reader_cls = DiskFileReader
    //writer_cls = DiskFileWriter

    public void _get_ondisk_files(ArrayList<String> files) {
        this._ondisk_info =
            this.manager.get_ondisk_files(files, this._datadir);
        return this._ondisk_info;
    }
}

