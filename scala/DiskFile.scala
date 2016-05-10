import java.util.ArrayList;


class DiskFile extends BaseDiskFile {

    //reader_cls = DiskFileReader
    //writer_cls = DiskFileWriter

    def _get_ondisk_files(files: List[String]) {
        this._ondisk_info =
            this.manager.get_ondisk_files(files, this._datadir);
        return this._ondisk_info;
    }
}

