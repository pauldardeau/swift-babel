import BaseDiskFileManager


type
    BaseDiskFile* = ref object
        name: string
        account: string
        container: string
        obj: string
        datadir: string
        tmpdir: string
        user_splice: bool
        pipe_size: int
        #policy: StoragePolicy
        #logger: Logger
        #manager: DiskFileManager
        device_path: string
        disk_chunk_size: int
        bytes_per_sync: int
        #metadata: HashMap

    #[
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
    ]#


    #reader_cls = nil  # must be set by subclasses
    #writer_cls = nil  # must be set by subclasses


method initBaseDiskFile*(self: BaseDiskFile, mgr: BaseDiskFileManager, device_path: string, threadpool, partition,
                 account: string, container: string, obj: string, datadir: string,
                 policy, use_splice: bool, pipe_size: int) =
        self.manager = mgr
        self.device_path = device_path
        #self.threadpool = threadpool or ThreadPool(nthreads=0)
        self.logger = mgr.logger
        self.disk_chunk_size = mgr.disk_chunk_size
        self.bytes_per_sync = mgr.bytes_per_sync
        self.use_splice = use_splice
        self.pipe_size = pipe_size
        self.policy = policy
        if account and container and obj:
            self.name = "/" + "/".join((account, container, obj))
            self.account = account
            self.container = container
            self.obj = obj
            #let name_hash = hash_path(account, container, obj)
            #self.datadir = join(
            #    device_path, storage_directory(get_data_dir(policy),
            #                                   partition, name_hash))
        else:
            # gets populated when we read the metadata
            self.name = nil
            self.account = nil
            self.container = nil
            self.obj = nil
            self.datadir = nil
        #self.tmpdir = join(device_path, get_tmp_dir(policy))
        self.ondisk_info = nil
        self.metadata = nil
        self.datafile_metadata = nil
        self.metafile_metadata = nil
        self.data_file = nil
        self.fp = nil
        self.quarantined_dir = nil
        self.content_length = nil
        if datadir:
            self.datadir = datadir
        else:
            #PJD: pass
            let x = 5
            #let name_hash = hash_path(account, container, obj)
            #self.datadir = join(
            #    device_path, storage_directory(get_data_dir(policy),
            #                                   partition, name_hash))

method manager*(self: BaseDiskFile): BaseDiskFileManager =
        return self.manager

method account*(self: BaseDiskFile): string =
        return self.account

method container*(self: BaseDiskFile): string =
        return self.container

method obj*(self: BaseDiskFile): string =
        return self.obj

method content_length*(self: BaseDiskFile): int =
        if self.metadata is nil:
            raise DiskFileNotOpen()
        return self.content_length

method timestamp*(self: BaseDiskFile): Timestamp =
        if self.metadata is nil:
            raise DiskFileNotOpen()
        return Timestamp(self.metadata.get("X-Timestamp"))

method data_timestamp*(self: BaseDiskFile): Timestamp =
        if self.datafile_metadata is nil:
            raise DiskFileNotOpen()
        return Timestamp(self.datafile_metadata.get("X-Timestamp"))

method durable_timestamp*(self: BaseDiskFile): Timestamp =
        #[
        Provides the timestamp of the newest data file found in the object
        directory.

        :return: A Timestamp instance, or nil if no data file was found.
        :raises DiskFileNotOpen: if the open() method has not been previously
                                 called on this instance.
        ]#

        if self.ondisk_info is nil:
            raise DiskFileNotOpen()
        if self.datafile_metadata:
            return Timestamp(self.datafile_metadata.get("X-Timestamp"))
        return nil

method fragments*(self: BaseDiskFile) =
        return nil

@classmethod
proc from_hash_dir(cls, mgr, hash_dir_path, device_path, partition, policy) =
        return cls(mgr, device_path, nil, partition, datadir=hash_dir_path,
                   policy=policy)

method open*(self: BaseDiskFile) =
        #[
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
        ]#

        # First figure out if the data directory exists
        try:
            files = os.listdir(self.datadir)
        except OSError as err:
            if err.errno == errno.ENOTDIR:
                # If there's a file here instead of a directory, quarantine
                # it; something's gone wrong somewhere.
                raise self.quarantine(
                    # hack: quarantine_renamer actually renames the directory
                    # enclosing the filename you give it, but here we just
                    # want this one file and not its parent.
                    os.path.join(self.datadir, "made-up-filename"),
                    "Expected directory, found file at %s" % self.datadir)
            elif err.errno != errno.ENOENT:
                raise DiskFileError(
                    "Error listing directory %s: %s" % (self.datadir, err))
            # The data directory does not exist, so the object cannot exist.
            files = []

        # gather info about the valid files to use to open the DiskFile
        file_info = self.get_ondisk_files(files)

        self.data_file = file_info.get("data_file")
        if not self.data_file:
            raise self.construct_exception_from_ts_file(**file_info)
        self.fp = self.construct_from_data_file(**file_info)
        # This method must populate the internal _metadata attribute.
        self.metadata = self.metadata or {}
        return self

method enter*(self: BaseDiskFile) =
        #[
        Context enter.

        .. note::

            An implementation shall raise `DiskFileNotOpen` when has not
            previously invoked the :func:`swift.obj.diskfile.DiskFile.open`
            method.
        ]#

        if self.metadata is nil:
            raise DiskFileNotOpen()
        return self

method exit*(self: BaseDiskFile, t, v, tb) =
        #[
        Context exit.

        .. note::

            This method will be invoked by the object server while servicing
            the REST API *before* the object has actually been read. It is the
            responsibility of the implementation to properly handle that.
        ]#
        if self.fp is not nil:
            fp, self.fp = self.fp, nil
            fp.close()

method quarantine*(self: BaseDiskFile, data_file: string, msg: string) =
        #[
        Quarantine a file; responsible for incrementing the associated logger's
        count of quarantines.

        :param data_file: full path of data file to quarantine
        :param msg: reason for quarantining to be included in the exception
        :returns: DiskFileQuarantined exception object
        ]#

        self.quarantined_dir = self.threadpool.run_in_thread(
            self.manager.quarantine_renamer, self.device_path, data_file)
        self.logger.warning("Quarantined object %s: %s" % (
            data_file, msg))
        self.logger.increment("quarantines")
        return DiskFileQuarantined(msg)

method get_ondisk_files*(self: BaseDiskFile, files) =
        #[
        Determine the on-disk files to use.

        :param files: a list of files in the object's dir
        :returns: dict of files to use having keys 'data_file', 'ts_file',
                 'meta_file'
        ]#
        
        raise NotImplementedError

method construct_exception_from_ts_file*(self: BaseDiskFile, ts_file: string) =
        #[
        If a tombstone is present it means the object is considered
        deleted. We just need to pull the metadata from the tombstone file
        which has the timestamp to construct the deleted exception. If there
        was no tombstone, just report it does not exist.

        :param ts_file: the tombstone file name found on disk
        :returns: DiskFileDeleted if the ts_file was provided, else
                  DiskFileNotExist
        ]#

        var
            exception_raised: bool

        if not ts_file:
            exc = DiskFileNotExist()
        else:
            exception_raised = true
            try:
                metadata = self.failsafe_read_metadata(ts_file, ts_file)
                exception_raised = false
            except DiskFileQuarantined:
                # If the tombstone's corrupted, quarantine it and pretend it
                # wasn't there
                exc = DiskFileNotExist()

            if not exception_raised:
                # All well and good that we have found a tombstone file, but
                # we don't have a data file so we are just going to raise an
                # exception that we could not find the object, providing the
                # tombstone's timestamp.
                exc = DiskFileDeleted(metadata=metadata)
        return exc

method verify_name_matches_hash*(self: BaseDiskFile, data_file) =
        hash_from_fs = os.path.basename(self.datadir)
        hash_from_name = hash_path(self.name.lstrip("/"))
        if hash_from_fs != hash_from_name:
            raise self.quarantine(
                data_file,
                "Hash of name in metadata does not match directory name")

method verify_data_file*(self: BaseDiskFile, data_file, fp): int =
        #[
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
        ]#

        var
            exception_raised: bool

        exception_raised = true
        try:
            mname = self.metadata["name"]
            exception_raised = false
        except KeyError:
            raise self.quarantine(data_file, "missing name metadata")

        if not exception_raised:
            if mname != self.name:
                self.logger.error(
                    _("Client path %(client)s does not match "
                      "path stored in object metadata %(meta)s"),
                    {"client": self.name, "meta": mname})
                raise DiskFileCollision("Client path does not match path "
                                        "stored in object metadata")

        exception_raised = true
        try:
            x_delete_at = int(self.metadata["X-Delete-At"])
            exception_raised = false
        except KeyError:
            pass
        except ValueError:
            # Quarantine, the x-delete-at key is present but not an
            # integer.
            raise self.quarantine(
                data_file, "bad metadata x-delete-at value %s" % (
                    self.metadata["X-Delete-At"]))

        if not exception_raised:
            if x_delete_at <= time.time():
                raise DiskFileExpired(metadata=self.metadata)
        try:
            metadata_size = int(self.metadata["Content-Length"])
        except KeyError:
            raise self.quarantine(
                data_file, "missing content-length in metadata")
        except ValueError:
            # Quarantine, the content-length key is present but not an
            # integer.
            raise self.quarantine(
                data_file, "bad metadata content-length value %s" % (
                    self.metadata["Content-Length"]))
        fd = fp.fileno()
        exception_raised = true
        try:
            statbuf = os.fstat(fd)
            exception_raised = false
        except OSError as err:
            # Quarantine, we can't successfully stat the file.
            raise self.quarantine(data_file, "not stat-able: %s" % err)

        if not exception_raised:
            obj_size = statbuf.st_size
        if obj_size != metadata_size:
            raise self.quarantine(
                data_file, "metadata content-length %s does"
                " not match actual object size %s" % (
                    metadata_size, statbuf.st_size))
        self.content_length = obj_size
        return obj_size

method failsafe_read_metadata*(self: BaseDiskFile, source, quarantine_filename=nil) =
        # Takes source and filename separately so we can read from an open
        # file if we have one
        try:
            return read_metadata(source)
        except (DiskFileXattrNotSupported, DiskFileNotExist):
            raise
        except Exception as err:
            raise self.quarantine(
                quarantine_filename,
                "Exception reading metadata: %s" % err)

method construct_from_data_file*(self: BaseDiskFile, data_file, meta_file) =
        #[
        Open the `.data` file to fetch its metadata, and fetch the metadata
        from the fast-POST `.meta` file as well if it exists, merging them
        properly.

        :param data_file: on-disk `.data` file being considered
        :param meta_file: on-disk fast-POST `.meta` file being considered
        :returns: an opened data file pointer
        :raises DiskFileError: various exceptions from
                    :func:`swift.obj.diskfile.DiskFile._verify_data_file`
        ]#
        fp = open(data_file, "rb")
        self.datafile_metadata = self.failsafe_read_metadata(fp, data_file)
        self.metadata = {}
        if meta_file:
            self.metafile_metadata = self.failsafe_read_metadata(
                meta_file, meta_file)
            sys_metadata = dict(
                [(key, val) for key, val in self.datafile_metadata.items()
                 if key.lower() in DATAFILE_SYSTEM_META
                 or is_sys_meta("object", key)])
            self.metadata.update(self.metafile_metadata)
            self.metadata.update(sys_metadata)
            # diskfile writer added 'name' to metafile, so remove it here
            self.metafile_metadata.pop("name", nil)
        else:
            self.metadata.update(self.datafile_metadata)
        if self.name is nil:
            # If we don't know our name, we were just given a hash dir at
            # instantiation, so we'd better validate that the name hashes back
            # to us
            self.name = self.metadata["name"]
            self.verify_name_matches_hash(data_file)
        self.verify_data_file(data_file, fp)
        return fp

method get_metafile_metadata*(self: BaseDiskFile) =
        #[
        Provide the metafile metadata for a previously opened object as a
        dictionary. This is metadata that was written by a POST and does not
        include any persistent metadata that was set by the original PUT.

        :returns: object's .meta file metadata dictionary, or nil if there is
                  no .meta file
        :raises DiskFileNotOpen: if the
            :func:`swift.obj.diskfile.DiskFile.open` method was not previously
            invoked
        ]#

        if self.metadata is nil:
            raise DiskFileNotOpen()
        return self.metafile_metadata

method get_datafile_metadata*(self: BaseDiskFile) =
        #[
        Provide the datafile metadata for a previously opened object as a
        dictionary. This is metadata that was included when the object was
        first PUT, and does not include metadata set by any subsequent POST.

        :returns: object's datafile metadata dictionary
        :raises DiskFileNotOpen: if the
            :func:`swift.obj.diskfile.DiskFile.open` method was not previously
            invoked
        #]

        if self.datafile_metadata is nil:
            raise DiskFileNotOpen()
        return self.datafile_metadata

method get_metadata*(self: BaseDiskFile) =
        #[
        Provide the metadata for a previously opened object as a dictionary.

        :returns: object's metadata dictionary
        :raises DiskFileNotOpen: if the
            :func:`swift.obj.diskfile.DiskFile.open` method was not previously
            invoked
        ]#

        if self.metadata is nil:
            raise DiskFileNotOpen()
        return self.metadata

method read_metadata*(self: BaseDiskFile) =
        #[
        Return the metadata for an object without requiring the caller to open
        the object first.

        :returns: metadata dictionary for an object
        :raises DiskFileError: this implementation will raise the same
                            errors as the `open()` method.
        ]#

        with self.open():
            return self.get_metadata()

method reader*(self: BaseDiskFile, keep_cache: bool=false,
               quarantine_hook=lambda m: nil) =
        #[
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
        ]#

        dr = self.reader_cls(
            self.fp, self.data_file, int(self.metadata["Content-Length"]),
            self.metadata["ETag"], self.threadpool, self.disk_chunk_size,
            self.manager.keep_cache_size, self.device_path, self.logger,
            use_splice=self.use_splice, quarantine_hook=_quarantine_hook,
            pipe_size=self.pipe_size, diskfile=self, keep_cache=keep_cache)
        # At this point the reader object is now responsible for closing
        # the file pointer.
        self.fp = nil
        return dr

    @contextmanager
iterator create*(self: BaseDiskFile, size: int=nil) =
        #[
        Context manager to create a file. We create a temporary file first, and
        then return a DiskFileWriter object to encapsulate the state.

        .. note::

            An implementation is not required to perform on-disk
            preallocations even if the parameter is specified. But if it does
            and it fails, it must raise a `DiskFileNoSpace` exception.

        :param size: optional initial size of file to explicitly allocate on
                     disk
        :raises DiskFileNoSpace: if a size is specified and allocation fails
        ]#

        if not exists(self.tmpdir):
            mkdirs(self.tmpdir)
        try:
            fd, tmppath = mkstemp(dir=self.tmpdir)
        except OSError as err:
            if err.errno in (errno.ENOSPC, errno.EDQUOT):
                # No more inodes in filesystem
                raise DiskFileNoSpace()
            raise
        dfw = nil
        try:
            if size is not nil and size > 0:
                try:
                    fallocate(fd, size)
                except OSError as err:
                    if err.errno in (errno.ENOSPC, errno.EDQUOT):
                        raise DiskFileNoSpace()
                    raise
            dfw = self.writer_cls(self.name, self.datadir, fd, tmppath,
                                  bytes_per_sync=self.bytes_per_sync,
                                  threadpool=self.threadpool,
                                  diskfile=self)
            yield dfw
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            if (dfw is nil) or (not dfw.put_succeeded):
                # Try removing the temp file only if put did NOT succeed.
                #
                # dfw.put_succeeded is set to True after renamer() succeeds in
                # DiskFileWriter._finalize_put()
                try:
                    os.unlink(tmppath)
                except OSError:
                    self.logger.exception("Error removing tempfile: %s" %
                                           tmppath)

method write_metadata*(self: BaseDiskFile, metadata) =
        #[
        Write a block of metadata to an object without requiring the caller to
        create the object first. Supports fast-POST behavior semantics.

        :param metadata: dictionary of metadata to be associated with the
                         object
        :raises DiskFileError: this implementation will raise the same
                            errors as the `create()` method.
        ]#

        with self.create() as writer:
            writer.extension = ".meta"
            writer.put(metadata)

method delete_object*(self: BaseDiskFile, timestamp: Timestamp) =
        #[
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
        ]#

        # this is dumb, only tests send in strings
        timestamp = Timestamp(timestamp)
        with self.create() as deleter:
            deleter.extension = ".ts"
            deleter.put({"X-Timestamp": timestamp.internal})

