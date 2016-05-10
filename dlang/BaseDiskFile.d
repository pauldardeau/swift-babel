module BaseDiskFile;

import DirUtils;
import DiskFileManager;
import DiskFileReader;
import DiskFileWriter;
import FileUtils;
import Logger;
import QuarantineHook;
import StoragePolicy;
import ThreadPool;


abstract class BaseDiskFile {
    /*
    Manage object files.

    This specific implementation manages object files on a disk formatted with
    a POSIX-compliant file system that supports extended attributes as
    metadata on a file or directory.

    .. note::

        The arguments to the constructor are considered implementation
        specific. The API does not define the constructor arguments.

        The following path format is used for data file locations:
        <devices_path/<device_dir>/<datadir>/<partdir>/<suffixdir>/<hashdir>/
        <datafile>.<ext>

    :param mgr: associated DiskFileManager instance
    :param device_path: path to the target device or drive
    :param threadpool: thread pool to use for blocking operations
    :param partition: partition on the device in which the object lives
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param _datadir: override the full datadir otherwise constructed here
    :param policy: the StoragePolicy instance
    :param use_splice: if true, use zero-copy splice() to send data
    :param pipe_size: size of pipe buffer used in zero-copy operations
    """
    */

    //reader_cls = null;  // must be set by subclasses
    //writer_cls = null;  // must be set by subclasses

/*
    public Set<String> DATAFILE_SYSTEM_META = null;

    static {
        DATAFILE_SYSTEM_META = new TreeSet();
        DATAFILE_SYSTEM_META.add("content-length");
        DATAFILE_SYSTEM_META.add("content-type");
        DATAFILE_SYSTEM_META.add("deleted");
        DATAFILE_SYSTEM_META.add("etag");
    }
    */

private:
    string _name;
    string _account;
    string _container;
    string _obj;
    string _datadir;
    string _tmpdir;
    bool _use_splice;
    int _pipe_size;
    StoragePolicy policy;
    Logger _logger;
    DiskFileManager _manager;
    string _device_path;
    int _disk_chunk_size;
    int _bytes_per_sync;
    HashMap _metadata;


public:
    this(DiskFileManager mgr,
                             string device_path,
                             ThreadPool threadpool,
                             int partition,
                             string account, //=null
                             string container, //=null
                             string obj, //=null
                             string _datadir, //=null
                             StoragePolicy policy, //=null
                             bool use_splice, //=false
                             int pipe_size) { //=null

        this._manager = mgr;
        this._device_path = device_path;
        if (threadpool != null) {
            this._threadpool = threadpool;
        } else {
            this._threadpool = new ThreadPool(0);
        }
        this._logger = mgr.logger;
        this._disk_chunk_size = mgr.disk_chunk_size;
        this._bytes_per_sync = mgr.bytes_per_sync;
        this._use_splice = use_splice;
        this._pipe_size = pipe_size;
        this.policy = policy;
        if (account != null && container != null && obj != null) {
            this._name = "/" + account + "/" + container + "/" + obj;
            this._account = account;
            this._container = container;
            this._obj = obj;
            name_hash = hash_path(account, container, obj);
            this._datadir = join(
                device_path, storage_directory(get_data_dir(policy),
                                               partition, name_hash));
        } else {
            // gets populated when we read the metadata
            this._name = null;
            this._account = null;
            this._container = null;
            this._obj = null;
            this._datadir = null;
        }
        this._tmpdir = join(device_path, get_tmp_dir(policy));
        this._ondisk_info = null;
        this._metadata = null;
        this._datafile_metadata = null;
        this._metafile_metadata = null;
        this._data_file = null;
        this._fp = null;
        this._quarantined_dir = null;
        this._content_length = null;
        if (_datadir != null) {
            this._datadir = _datadir;
        } else {
            name_hash = hash_path(account, container, obj);
            this._datadir = join(
                device_path, storage_directory(get_data_dir(policy),
                                               partition, name_hash));
        }
    }

    @property void manager() {
        return this._manager;
    }

    @property string account() {
        return this._account;
    }

    @property string container() {
        return this._container;
    }

    @property string obj() {
        return this._obj;
    }

    @property void content_length() {
        if (this._metadata == null) {
            throw DiskFileNotOpen();
        }
        return this._content_length;
    }

    @property Date timestamp() {
        if (this._metadata == null) {
            throw DiskFileNotOpen();
        }
        return new Date(this._metadata.get("X-Timestamp"));
    }

    @property Date data_timestamp() {
        if (this._datafile_metadata == null) {
            throw DiskFileNotOpen();
        }
        return new Date(this._datafile_metadata.get("X-Timestamp"));
    }

    /**
    Provides the timestamp of the newest data file found in the object
    directory.

    :return: A Timestamp instance, or None if no data file was found.
    :raises DiskFileNotOpen: if the open() method has not been previously
                             called on this instance.
    */
    @property Date durable_timestamp() {
        if (this._ondisk_info == null) {
            throw DiskFileNotOpen();
        }

        if (this._datafile_metadata) {
            return new Date(this._datafile_metadata.get("X-Timestamp"));
        }

        return null;
    }

    @property void fragments() {
        return null;
    }

    @classmethod
    void from_hash_dir(Class cls,
                              DiskFileManager mgr,
                              string hash_dir_path,
                              string device_path,
                              int partition,
                              StoragePolicy policy) {
        return cls(mgr,
                   device_path,
                   null,
                   partition,
                   hash_dir_path, //_datadir
                   policy);
    }

    void open() {
        /*
        Open the object.

        This implementation opens the data file representing the object, reads
        the associated metadata in the extended attributes, additionally
        combining metadata from fast-POST `.meta` files.

        .. note::

            An implementation is allowed to raise any of the following
            exceptions, but is only required to raise `DiskFileNotExist` when
            the object representation does not exist.

        :raises DiskFileCollision: on name mis-match with metadata
        :raises DiskFileNotExist: if the object does not exist
        :raises DiskFileDeleted: if the object was previously deleted
        :raises DiskFileQuarantined: if while reading metadata of the file
                                     some data did pass cross checks
        :returns: itself for use as a context manager
        */

        string[] files;

        // First figure out if the data directory exists
        try {
            files = OSUtils.listdir(this._datadir);
        } catch (OSError err) {
            if (err.errno == errno.ENOTDIR) {
                // If there's a file here instead of a directory, quarantine
                // it; something's gone wrong somewhere.
                throw this._quarantine(
                    // hack: quarantine_renamer actually renames the directory
                    // enclosing the filename you give it, but here we just
                    // want this one file and not its parent.
                    OSUtils.path_join(this._datadir, "made-up-filename"),
                    "Expected directory, found file at " + this._datadir);
            } else if (err.errno != errno.ENOENT) {
                throw DiskFileError(
                    "Error listing directory " + this._datadir + ": " + err);
            }
            // The data directory does not exist, so the object cannot exist.
            files = new string[100]; //PJD: size???
        }
// gather info about the valid files to use to open the DiskFile
        file_info = this._get_ondisk_files(files);

        this._data_file = file_info.get("data_file");
        if (!this._data_file) {
            throw this._construct_exception_from_ts_file(file_info);
        }
        this._fp = this._construct_from_data_file(file_info);
        // This method must populate the internal _metadata attribute.
        if (null == this._metadata) {
            this._metadata = new HashMap();
        }
    }

    void __enter__() {
        /*
        Context enter.

        .. note::

            An implementation shall raise `DiskFileNotOpen` when has not
            previously invoked the :func:`swift.obj.diskfile.DiskFile.open`
            method.
        */
        if (this._metadata == null) {
            throw DiskFileNotOpen();
        }
        return this;
    }

    void __exit__() {
        /*
        Context exit.

        .. note::

            This method will be invoked by the object server while servicing
            the REST API *before* the object has actually been read. It is the
            responsibility of the implementation to properly handle that.
        */
        if (this._fp != null) {
            fp = this._fp;
            this._fp = null;
            fp.close();
        }
    }

    /**
    Quarantine a file; responsible for incrementing the associated logger's
    count of quarantines.

    :param data_file: full path of data file to quarantine
    :param msg: reason for quarantining to be included in the exception
    :returns: DiskFileQuarantined exception object
    */
    Exception _quarantine(string data_file, string msg) {
        this._quarantined_dir =
            this._threadpool.run_in_thread(this.manager.quarantine_renamer,
                                           this._device_path, data_file);
        this._logger.warning("Quarantined object " ~ data_file ~ ": " ~ msg);
        this._logger.increment("quarantines");
        return new DiskFileQuarantined(msg);
    }

    abstract void _get_ondisk_files(string[] files);
        /*
        Determine the on-disk files to use.

        :param files: a list of files in the object's dir
        :returns: dict of files to use having keys 'data_file', 'ts_file',
                 'meta_file'
        */

    Exception _construct_exception_from_ts_file(string ts_file) {
        /*
        If a tombstone is present it means the object is considered
        deleted. We just need to pull the metadata from the tombstone file
        which has the timestamp to construct the deleted exception. If there
        was no tombstone, just report it does not exist.

        :param ts_file: the tombstone file name found on disk
        :returns: DiskFileDeleted if the ts_file was provided, else
                  DiskFileNotExist
        */

        Exception exc;

        if (ts_file == null || ts_file.length() == 0) {
            exc = new DiskFileNotExist();
        } else {
            bool exception_raised = true;
            try {
                metadata = this._failsafe_read_metadata(ts_file, ts_file);
                exception_raised = false;
            } catch (DiskFileQuarantined dfq) {
                // If the tombstone's corrupted, quarantine it and pretend it
                // wasn't there
                exc = new DiskFileNotExist();
            }

            if (!exception_raised) {
                // All well and good that we have found a tombstone file, but
                // we don't have a data file so we are just going to raise an
                // exception that we could not find the object, providing the
                // tombstone's timestamp.
                exc = new DiskFileDeleted(metadata);
            }
        }
        return exc;
    }

    void _verify_name_matches_hash(string data_file) {
        string hash_from_fs = os.path.basename(this._datadir);
        string hash_from_name = hash_path(this._name.lstrip("/"));
        if (!hash_from_fs.equals(hash_from_name)) {
            throw this._quarantine(
                data_file,
                "Hash of name in metadata does not match directory name");
        }
    }

    void _verify_data_file(string data_file, Object fp) {
        /*
        Verify the metadata's name value matches what we think the object is
        named.

        :param data_file: data file name being consider, used when quarantines
                          occur
        :param fp: open file pointer so that we can `fstat()` the file to
                   verify the on-disk size with Content-Length metadata value
        :raises DiskFileCollision: if the metadata stored name does not match
                                   the referenced name of the file
        :raises DiskFileExpired: if the object has expired
        :raises DiskFileQuarantined: if data inconsistencies were detected
                                     between the metadata and the file-system
                                     metadata
        */

        string mname;
        bool exception_raised = true;

        try {
            mname = this._metadata["name"];
            exception_raised = false;
        } catch (KeyError ke) {
            throw this._quarantine(data_file, "missing name metadata");
        }

        if (!exception_raised) {
            if (!mname.equals(this._name)) {
                this._logger.error(
                    "Client path " + this._name + " does not match " +
                      "path stored in object metadata " + mname);
                throw DiskFileCollision("Client path does not match path " +
                                        "stored in object metadata");
            }
        }

        int x_delete_at;
        exception_raised = true;

        try {
            x_delete_at = to!int(this._metadata["X-Delete-At"]);
            exception_raised = false;
        } catch (KeyError ke) {

        } catch (ValueError ve) {
            // Quarantine, the x-delete-at key is present but not an
            // integer.
            throw this._quarantine(
                data_file, "bad metadata x-delete-at value " +
                    this._metadata["X-Delete-At"]);
        }

        if (!exception_raised) {
            if (x_delete_at <= time.time()) {
                throw DiskFileExpired(metadata=this._metadata);
            }
        }

        int metadata_size;

        try {
            metadata_size = to!int(this._metadata["Content-Length"]);
        } catch (KeyError ke) {
            throw this._quarantine(
                data_file, "missing content-length in metadata");
        } catch (ValueError ve) {
            // Quarantine, the content-length key is present but not an
            // integer.
            throw this._quarantine(
                data_file, "bad metadata content-length value " +
                    this._metadata["Content-Length"]);
        }

        fd = fp.fileno();
        exception_raised = true;

        try {
            statbuf = os.fstat(fd);
            exception_raised = false;
        } catch (OSError err) {
            // Quarantine, we can't successfully stat the file.
            throw this._quarantine(data_file, "not stat-able: " + err);
        }

        if (!exception_raised) {
            obj_size = statbuf.st_size;
        }

        if (obj_size != metadata_size) {
            throw this._quarantine(
                data_file, "metadata content-length " + metadata_size +
                " does not match actual object size " + statbuf.st_size);
        }
        this._content_length = obj_size;
        return obj_size;
    }

    void _failsafe_read_metadata(string source,
                                        string quarantine_filename) { //=null
        // Takes source and filename separately so we can read from an open
        // file if we have one
        try {
            return read_metadata(source);
        } catch (DiskFileXattrNotSupported e) { //, DiskFileNotExist):
            throw e;
        } catch (Exception err) {
            throw this._quarantine(
                quarantine_filename,
                "Exception reading metadata: " + err);
        }
    }

    void _construct_from_data_file(string data_file,
                                          string meta_file) {
        /*
        Open the `.data` file to fetch its metadata, and fetch the metadata
        from the fast-POST `.meta` file as well if it exists, merging them
        properly.

        :param data_file: on-disk `.data` file being considered
        :param meta_file: on-disk fast-POST `.meta` file being considered
        :returns: an opened data file pointer
        :raises DiskFileError: various exceptions from
                    :func:`swift.obj.diskfile.DiskFile._verify_data_file`
        */
        fp = open(data_file, "rb");
        this._datafile_metadata = this._failsafe_read_metadata(fp, data_file);
        this._metadata = new HashMap();
        if (meta_file != null) {
            this._metafile_metadata = this._failsafe_read_metadata(
                meta_file, meta_file);
            /*
            sys_metadata = dict(
                [(key, val) for key, val in this._datafile_metadata.items()
                 if (DATAFILE_SYSTEM_META.contains(key.lower()) ||
                 is_sys_meta("object", key)]);
                 */
            this._metadata.update(this._metafile_metadata);
            this._metadata.update(sys_metadata);
            // diskfile writer added 'name' to metafile, so remove it here
            this._metafile_metadata.pop("name", null);
        } else {
            this._metadata.update(this._datafile_metadata);
        }
        if (this._name == null) {
            // If we don't know our name, we were just given a hash dir at
            // instantiation, so we'd better validate that the name hashes back
            // to us
            this._name = this._metadata["name"];
            this._verify_name_matches_hash(data_file);
        }
        this._verify_data_file(data_file, fp);
        return fp;
    }

    void get_metafile_metadata() {
        /*
        Provide the metafile metadata for a previously opened object as a
        dictionary. This is metadata that was written by a POST and does not
        include any persistent metadata that was set by the original PUT.

        :returns: object's .meta file metadata dictionary, or None if there is
                  no .meta file
        :raises DiskFileNotOpen: if the
            :func:`swift.obj.diskfile.DiskFile.open` method was not previously
            invoked
        */
        if (this._metadata == null) {
            throw DiskFileNotOpen();
        }
        return this._metafile_metadata;
    }

    void get_datafile_metadata() {
        /*
        Provide the datafile metadata for a previously opened object as a
        dictionary. This is metadata that was included when the object was
        first PUT, and does not include metadata set by any subsequent POST.

        :returns: object's datafile metadata dictionary
        :raises DiskFileNotOpen: if the
            :func:`swift.obj.diskfile.DiskFile.open` method was not previously
            invoked
        */
        if (this._datafile_metadata == null) {
            throw DiskFileNotOpen();
        }
        return this._datafile_metadata;
    }

    void get_metadata() {
        /*
        Provide the metadata for a previously opened object as a dictionary.

        :returns: object's metadata dictionary
        :raises DiskFileNotOpen: if the
            :func:`swift.obj.diskfile.DiskFile.open` method was not previously
            invoked
        */
        if (this._metadata == null) {
            throw new DiskFileNotOpen();
        }
        return this._metadata;
    }

    /**
    Return the metadata for an object without requiring the caller to open
    the object first.

    :returns: metadata dictionary for an object
    :raises DiskFileError: this implementation will raise the same
                        errors as the `open()` method.
    */
    void read_metadata() {
        try {
            this.open();
            return this.get_metadata();
        } finally {
            this.__exit__();
        }
    }

    DiskFileReader reader(bool keep_cache, //=false,
               QuarantineHook quarantine_hook) { //=lambda m: null) {
        /*
        Return a :class:`swift.common.swob.Response` class compatible
        "`app_iter`" object as defined by
        :class:`swift.obj.diskfile.DiskFileReader`.

        For this implementation, the responsibility of closing the open file
        is passed to the :class:`swift.obj.diskfile.DiskFileReader` object.

        :param keep_cache: caller's preference for keeping data read in the
                           OS buffer cache
        :param _quarantine_hook: 1-arg callable called when obj quarantined;
                                 the arg is the reason for quarantine.
                                 Default is to ignore it.
                                 Not needed by the REST layer.
        :returns: a :class:`swift.obj.diskfile.DiskFileReader` object
        */

        DiskFileReader dr;

        dr = this.reader_cls(this._fp,
                             this._data_file,
                             to!int(this._metadata["Content-Length"]),
                             this._metadata["ETag"],
                             this._threadpool,
                             this._disk_chunk_size,
                             this._manager.keep_cache_size,
                             this._device_path,
                             this._logger,
                             this._use_splice,
                             quarantine_hook,
                             this._pipe_size,
                             this, //diskfile
                             keep_cache);
        // At this point the reader object is now responsible for closing
        // the file pointer.
        this._fp = null;
        return dr;
    }

    @contextmanager
    void create(int size) { //=null
        /*
        Context manager to create a file. We create a temporary file first, and
        then return a DiskFileWriter object to encapsulate the state.

        .. note::

            An implementation is not required to perform on-disk
            preallocations even if the parameter is specified. But if it does
            and it fails, it must raise a `DiskFileNoSpace` exception.

        :param size: optional initial size of file to explicitly allocate on
                     disk
        :raises DiskFileNoSpace: if a size is specified and allocation fails
        */
        if (!DirUtils.dirExists(this._tmpdir)) {
            DirUtils.mkdirs(this._tmpdir);
        }

        try {
            fd, tmppath = DirUtils.mkstemp(this._tmpdir);
        } catch (OSError err) {
            if (err.errno == errno.ENOSPC || err.errno == errno.EDQUOT) {
                // No more inodes in filesystem
                throw new DiskFileNoSpace();
            }
            throw err;
        }

        dfw = null;

        try {
            if (size != null && size > 0) {
                try {
                    fallocate(fd, size);
                } catch (OSError err) {
                    if (err.errno == errno.ENOSPC || err.errno == errno.EDQUOT) {
                        throw new DiskFileNoSpace();
                    }
                    throw err;
                }
            }

            DiskFileWriter dfw;

            dfw = this.writer_cls(this._name,
                                  this._datadir,
                                  fd,
                                  tmppath,
                                  this._bytes_per_sync,
                                  this._threadpool,
                                  this); //diskfile
            yield dfw;
        } finally {
            try {
                os.close(fd);
            } catch (OSError ose) {
            }

            if ((dfw == null) || (!dfw.put_succeeded)) {
                // Try removing the temp file only if put did NOT succeed.
                //
                // dfw.put_succeeded is set to True after renamer() succeeds in
                // DiskFileWriter._finalize_put()
                try {
                    FileUtils.delete_file(tmppath);
                } catch(OSError ose) {
                    this._logger.exception("Error removing tempfile: " ~ tmppath);
                }
            }
        }
    }

    /**
    Write a block of metadata to an object without requiring the caller to
    create the object first. Supports fast-POST behavior semantics.

    :param metadata: dictionary of metadata to be associated with the
                     object
    :raises DiskFileError: this implementation will raise the same
                        errors as the `create()` method.
    */
    void write_metadata(HashMap metadata) {
        DiskFileWriter writer = null;
        try {
            writer = this.create();
            writer._extension = ".meta";
            writer.put(metadata);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
    Delete the object.

    This implementation creates a tombstone file using the given
    timestamp, and removes any older versions of the object file. Any
    file that has an older timestamp than timestamp will be deleted.

    .. note::

        An implementation is free to use or ignore the timestamp
        parameter.

    :param timestamp: timestamp to compare with each file
    :raises DiskFileError: this implementation will raise the same
                        errors as the `create()` method.
    */
    void delete_object(string timestamp) {
        // this is dumb, only tests send in strings

        /*
        timestamp = Timestamp(timestamp);
        with this.create() as deleter:
            deleter._extension = ".ts";
            deleter.put({"X-Timestamp": timestamp.internal});
            */
    }
}

