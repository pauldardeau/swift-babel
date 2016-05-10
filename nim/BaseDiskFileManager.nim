import Logger;

type
    BaseDiskFileManager* = ref object
        devices*: string
        use_splice*: bool


    #[
    Management class for devices, providing common place for shared parameters
    and methods not provided by the DiskFile class (which primarily services
    the object server REST API layer).

    The `get_diskfile()` method is how this implementation creates a `DiskFile`
    object.

    .. note::

        This class is reference implementation specific and not part of the
        pluggable on-disk backend API.

    .. note::

        TODO(portante): Not sure what the right name to recommend here, as
        "manager" seemed generic enough, though suggestions are welcome.

    :param conf: caller provided configuration object
    :param logger: caller provided logger
    ]#

    #diskfile_cls = nil  # must be set by subclasses

    #invalidate_hash = strip_self(invalidate_hash)
    #quarantine_renamer = strip_self(quarantine_renamer)

method init*(self: BaseDiskFileManager, conf, logger: Logger) =
        var
            exception_raised: bool

        self.logger = logger
        self.devices = conf.get("devices", "/srv/node")
        self.disk_chunk_size = int(conf.get("disk_chunk_size", 65536))
        self.keep_cache_size = int(conf.get("keep_cache_size", 5242880))
        self.bytes_per_sync = int(conf.get("mb_per_sync", 512)) * 1024 * 1024
        self.mount_check = config_true_value(conf.get("mount_check", "true"))
        self.reclaim_age = int(conf.get("reclaim_age", ONE_WEEK))
        self.replication_one_per_device = config_true_value(
            conf.get("replication_one_per_device", "true"))
        self.replication_lock_timeout = int(conf.get(
            "replication_lock_timeout", 15))
        threads_per_disk = int(conf.get("threads_per_disk", "0"))
        self.threadpools = defaultdict(
            lambda: ThreadPool(nthreads=threads_per_disk))

        self.use_splice = false
        self.pipe_size = nil

        conf_wants_splice = config_true_value(conf.get("splice", "no"))
        # If the operator wants zero-copy with splice() but we don't have the
        # requisite kernel support, complain so they can go fix it.
        if conf_wants_splice and not splice.available:
            self.logger.warning(
                "Use of splice() requested (config says \"splice = %s\"), " ~
                "but the system does not support it. " ~
                "splice() will not be used." % conf.get("splice"))
        elif conf_wants_splice and splice.available:
            exception_raised = true
            try:
                sockfd = get_md5_socket()
                os.close(sockfd)
                exception_raised = false
            except IOError as err:
                # AF_ALG socket support was introduced in kernel 2.6.38; on
                # systems with older kernels (or custom-built kernels lacking
                # AF_ALG support), we can't use zero-copy.
                if err.errno != errno.EAFNOSUPPORT:
                    raise
                self.logger.warning("MD5 sockets not supported. " ~
                                    "splice() will not be used.")

            if not exception_raised:
                self.use_splice = true
                with open("/proc/sys/fs/pipe-max-size") as f:
                    max_pipe_size = int(f.read())
                self.pipe_size = min(max_pipe_size, self.disk_chunk_size)

method parse_on_disk_filename*(self: BaseDiskFileManager, filename: string) =
        #[
        Parse an on disk file name.

        :param filename: the data file name including extension
        :returns: a dict, with keys for timestamp, and ext:

            * timestamp is a :class:`~swift.common.utils.Timestamp`
            * ext is a string, the file extension including the leading dot or
              the empty string if the filename has no extension.

           Subclases may add further keys to the returned dict.

        :raises DiskFileError: if any part of the filename is not able to be
                               validated.
        ]#

        raise NotImplementedError

method process_ondisk_files*(self: BaseDiskFileManager, exts, results, **kwargs) =
        #[
        Called by get_ondisk_files(). Should be over-ridden to implement
        subclass specific handling of files.

        :param exts: dict of lists of file info, keyed by extension
        :param results: a dict that may be updated with results
        ]#

        raise NotImplementedError

method verify_ondisk_files*(self: BaseDiskFileManager, results, **kwargs): bool =
        #[
        Verify that the final combination of on disk files complies with the
        diskfile contract.

        :param results: files that have been found and accepted
        :returns: true if the file combination is compliant, false otherwise
        ]#

        data_file, meta_file, ts_file = tuple(
            [results[key]
             for key in ("data_file", "meta_file", "ts_file")])

        return ((data_file is nil and meta_file is nil and ts_file is nil)
                or (ts_file is not nil and data_file is nil
                    and meta_file is nil)
                or (data_file is not nil and ts_file is nil))

method split_list*(self: BaseDiskFileManager, original_list, condition) =
        #[
        Split a list into two lists. The first list contains the first N items
        of the original list, in their original order,  where 0 < N <=
        len(original list). The second list contains the remaining items of the
        original list, in their original order.

        The index, N, at which the original list is split is the index of the
        first item in the list that does not satisfy the given condition. Note
        that the original list should be appropriately sorted if the second
        list is to contain no items that satisfy the given condition.

        :param original_list: the list to be split.
        :param condition: a single argument function that will be used to test
                          for the list item to split on.
        :return: a tuple of two lists.
        ]#

        for i, item in enumerate(original_list):
            if not condition(item):
                return original_list[:i], original_list[i:]
        return original_list, []

method split_gt_timestamp*(self: BaseDiskFileManager, file_info_list, timestamp) =
        #[
        Given a list of file info dicts, reverse sorted by timestamp, split the
        list into two: items newer than timestamp, and items at same time or
        older than timestamp.

        :param file_info_list: a list of file_info dicts.
        :param timestamp: a Timestamp.
        :return: a tuple of two lists.
        ]#

        return self.split_list(
            file_info_list, lambda x: x["timestamp"] > timestamp)

method split_gte_timestamp*(self: BaseDiskFileManager, file_info_list, timestamp) =
        #[
        Given a list of file info dicts, reverse sorted by timestamp, split the
        list into two: items newer than or at same time as the timestamp, and
        items older than timestamp.

        :param file_info_list: a list of file_info dicts.
        :param timestamp: a Timestamp.
        :return: a tuple of two lists.
        ]#

        return self.split_list(
            file_info_list, lambda x: x["timestamp"] >= timestamp)

method get_ondisk_files*(self: BaseDiskFileManager, files: seq[string], datadir: string, verify=true: bool, **kwargs) =
        #[
        Given a simple list of files names, determine the files that constitute
        a valid fileset i.e. a set of files that defines the state of an
        object, and determine the files that are obsolete and could be deleted.
        Note that some files may fall into neither category.

        If a file is considered part of a valid fileset then its info dict will
        be added to the results dict, keyed by <extension>_info. Any files that
        are no longer required will have their info dicts added to a list
        stored under the key 'obsolete'.

        The results dict will always contain entries with keys 'ts_file',
        'data_file' and 'meta_file'. Their values will be the fully qualified
        path to a file of the corresponding type if there is such a file in the
        valid fileset, or nil.

        :param files: a list of file names.
        :param datadir: directory name files are from.
        :param verify: if true verify that the ondisk file contract has not
                       been violated, otherwise do not verify.
        :returns: a dict that will contain keys:
                    ts_file   -> path to a .ts file or nil
                    data_file -> path to a .data file or nil
                    meta_file -> path to a .meta file or nil
                  and may contain keys:
                    ts_info   -> a file info dict for a .ts file
                    data_info -> a file info dict for a .data file
                    meta_info -> a file info dict for a .meta file
                    obsolete  -> a list of file info dicts for obsolete files
        ]#

        # Build the exts data structure:
        # exts is a dict that maps file extensions to a list of file_info
        # dicts for the files having that extension. The file_info dicts are of
        # the form returned by parse_on_disk_filename, with the filename added.
        # Each list is sorted in reverse timestamp order.
        #
        # The exts dict will be modified during subsequent processing as files
        # are removed to be discarded or ignored.
        exts = defaultdict(list)
        for afile in files:
            # Categorize files by extension
            try:
                file_info = self.parse_on_disk_filename(afile)
                file_info["filename"] = afile
                exts[file_info["ext"]].append(file_info)
            except DiskFileError as e:
                self.logger.warning("Unexpected file %s: %s" %
                                    (os.path.join(datadir or "", afile), e))
        for ext in exts:
            # For each extension sort files into reverse chronological order.
            exts[ext] = sorted(
                exts[ext], key=lambda info: info["timestamp"], reverse=true)

        # the results dict is used to collect results of file filtering
        results = {}

        # non-tombstones older than or equal to latest tombstone are obsolete
        if exts.get(".ts"):
            for ext in filter(lambda ext: ext != ".ts", exts.keys()):
                exts[ext], older = self.split_gt_timestamp(
                    exts[ext], exts[".ts"][0]["timestamp"])
                results.setdefault("obsolete", []).extend(older)

        # all but most recent .meta and .ts are obsolete
        for ext in (".meta", ".ts"):
            if ext in exts:
                results.setdefault("obsolete", []).extend(exts[ext][1:])
                exts[ext] = exts[ext][:1]

        # delegate to subclass handler
        self.process_ondisk_files(exts, results, **kwargs)

        # set final choice of files
        if exts.get(".ts"):
            results["ts_info"] = exts[".ts"][0]
        if "data_info" in results and exts.get(".meta"):
            # only report a meta file if there is a data file
            results["meta_info"] = exts[".meta"][0]

        # set ts_file, data_file and meta_file with path to chosen file or nil
        for info_key in ("data_info", "meta_info", "ts_info"):
            info = results.get(info_key)
            key = info_key[:-5] + "_file"
            results[key] = join(datadir, info["filename"]) if info else nil

        if verify:
            assert self.verify_ondisk_files(
                results, **kwargs), \
                "On-disk file search algorithm contract is broken: %s" \
                % str(results)

        return results

method cleanup_ondisk_files*(self: BaseDiskFileManager, hsh_path, reclaim_age=ONE_WEEK, **kwargs) =
        #[
        Clean up on-disk files that are obsolete and gather the set of valid
        on-disk files for an object.

        :param hsh_path: object hash path
        :param reclaim_age: age in seconds at which to remove tombstones
        :param frag_index: if set, search for a specific fragment index .data
                           file, otherwise accept the first valid .data file
        :returns: a dict that may contain: valid on disk files keyed by their
                  filename extension; a list of obsolete files stored under the
                  key 'obsolete'; a list of files remaining in the directory,
                  reverse sorted, stored under the key 'files'.
        ]#

        proc is_reclaimable(timestamp): bool =
            return (time.time() - float(timestamp)) > reclaim_age

        files = listdir(hsh_path)
        files.sort(reverse=true)
        results = self.get_ondisk_files(
            files, hsh_path, verify=false, **kwargs)
        if "ts_info" in results and is_reclaimable(
                results["ts_info"]["timestamp"]):
            remove_file(join(hsh_path, results["ts_info"]["filename"]))
            files.remove(results.pop("ts_info")["filename"])
        for file_info in results.get("possible_reclaim", []):
            # stray fragments are not deleted until reclaim-age
            if is_reclaimable(file_info["timestamp"]):
                results.setdefault("obsolete", []).append(file_info)
        for file_info in results.get("obsolete", []):
            remove_file(join(hsh_path, file_info["filename"]))
            files.remove(file_info["filename"])
        results["files"] = files
        return results

method hash_cleanup_listdir*(self: BaseDiskFileManager, hsh_path, reclaim_age=ONE_WEEK) =
        #[
        List contents of a hash directory and clean up any old files.
        For EC policy, delete files older than a .durable or .ts file.

        :param hsh_path: object hash path
        :param reclaim_age: age in seconds at which to remove tombstones
        :returns: list of files remaining in the directory, reverse sorted
        ]#

        # maintain compatibility with 'legacy' hash_cleanup_listdir
        # return value
        return self.cleanup_ondisk_files(
            hsh_path, reclaim_age=reclaim_age)["files"]

method hash_suffix_dir*(self: BaseDiskFileManager, path, mapper, reclaim_age: int) =
        var
            exception_raised: bool

        hashes = defaultdict(hashlib.md5)
        try:
            path_contents = sorted(os.listdir(path))
        except OSError as err:
            if err.errno in (errno.ENOTDIR, errno.ENOENT):
                raise PathNotDir()
            raise
        for hsh in path_contents:
            hsh_path = join(path, hsh)
            try:
                files = self.hash_cleanup_listdir(hsh_path, reclaim_age)
            except OSError as err:
                if err.errno == errno.ENOTDIR:
                    partition_path = dirname(path)
                    objects_path = dirname(partition_path)
                    device_path = dirname(objects_path)
                    quar_path = quarantine_renamer(device_path, hsh_path)
                    logging.exception(
                        _("Quarantined %(hsh_path)s to %(quar_path)s because "
                          "it is not a directory"), {"hsh_path": hsh_path,
                                                     "quar_path": quar_path})
                    continue
                raise
            if not files:
                try:
                    os.rmdir(hsh_path)
                except OSError:
                    pass
            for filename in files:
                key, value = mapper(filename)
                hashes[key].update(value)

        exception_raised = true
        try:
            os.rmdir(path)
            exception_raised = false
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise PathNotDir()

        if not exception_raised:
            # if we remove it, pretend like it wasn't there to begin with so
            # that the suffix key gets removed
            raise PathNotDir()
        return hashes

method hash_suffix*(self: BaseDiskFileManager, path, reclaim_age: int) =
        #[
        Performs reclamation and returns an md5 of all (remaining) files.

        :param reclaim_age: age in seconds at which to remove tombstones
        :raises PathNotDir: if given path is not a valid directory
        :raises OSError: for non-ENOTDIR errors
        ]#

        raise NotImplementedError

method get_hashes*(self: BaseDiskFileManager, partition_path: string, recalculate=nil, do_listdir=false: bool,
                    reclaim_age=nil) =
        #[
        Get a list of hashes for the suffix dir.  do_listdir causes it to
        mistrust the hash cache for suffix existence at the (unexpectedly high)
        cost of a listdir.  reclaim_age is just passed on to hash_suffix.

        :param partition_path: absolute path of partition to get hashes for
        :param recalculate: list of suffixes which should be recalculated when
                            got
        :param do_listdir: force existence check for all hashes in the
                           partition
        :param reclaim_age: age at which to remove tombstones

        :returns: tuple of (number of suffix dirs hashed, dictionary of hashes)
        ]#

        reclaim_age = reclaim_age or self.reclaim_age
        hashed = 0
        hashes_file = join(partition_path, HASH_FILE)
        modified = false
        force_rewrite = false
        hashes = {}
        mtime = -1

        if recalculate is nil:
            recalculate = []

        try:
            with open(hashes_file, "rb") as fp:
                hashes = pickle.load(fp)
            mtime = getmtime(hashes_file)
        except Exception:
            do_listdir = true
            force_rewrite = true
        if do_listdir:
            for suff in os.listdir(partition_path):
                if len(suff) == 3:
                    hashes.setdefault(suff, nil)
            modified = true
        hashes.update((suffix, nil) for suffix in recalculate)
        for suffix, hash_ in hashes.items():
            if not hash_:
                suffix_dir = join(partition_path, suffix)
                try:
                    hashes[suffix] = self.hash_suffix(suffix_dir, reclaim_age)
                    hashed += 1
                except PathNotDir:
                    del hashes[suffix]
                except OSError:
                    logging.exception(_("Error hashing suffix"))
                modified = true
        if modified:
            with lock_path(partition_path):
                if force_rewrite or not exists(hashes_file) or \
                        getmtime(hashes_file) == mtime:
                    write_pickle(
                        hashes, hashes_file, partition_path, PICKLE_PROTOCOL)
                    return hashed, hashes
            return self.get_hashes(partition_path, recalculate, do_listdir,
                                    reclaim_age)
        else:
            return hashed, hashes

method construct_dev_path*(self: BaseDiskFileManager, device: string): string =
        #[
        Construct the path to a device without checking if it is mounted.

        :param device: name of target device
        :returns: full path to the device
        ]#

        return os.path.join(self.devices, device)

method get_dev_path*(self: BaseDiskFileManager, device: string, mount_check=nil: bool) =
        #[
        Return the path to a device, first checking to see if either it
        is a proper mount point, or at least a directory depending on
        the mount_check configuration option.

        :param device: name of target device
        :param mount_check: whether or not to check mountedness of device.
                            Defaults to bool(self.mount_check).
        :returns: full path to the device, nil if the path to the device is
                  not a proper mount point or directory.
        ]#

        # we'll do some kind of check unless explicitly forbidden
        if mount_check is not false:
            if mount_check or self.mount_check:
                check = check_mount
            else:
                check = check_dir
            if not check(self.devices, device):
                return nil
        return os.path.join(self.devices, device)

    @contextmanager
method replication_lock*(self: BaseDiskFileManager, device) =
        #[
        A context manager that will lock on the device given, if
        configured to do so.

        :raises ReplicationLockTimeout: If the lock on the device
            cannot be granted within the configured timeout.
        ]#

        if self.replication_one_per_device:
            dev_path = self.get_dev_path(device)
            with lock_path(
                    dev_path,
                    timeout=self.replication_lock_timeout,
                    timeout_class=ReplicationLockTimeout):
                yield true
        else:
            yield true

method pickle_async_update*(self: BaseDiskFileManager, device, account, container, obj: string, data,
                            timestamp, policy) =
        device_path = self.construct_dev_path(device)
        async_dir = os.path.join(device_path, get_async_dir(policy))
        ohash = hash_path(account, container, obj)
        self.threadpools[device].run_in_thread(
            write_pickle,
            data,
            os.path.join(async_dir, ohash[-3:], ohash + "-" +
                         Timestamp(timestamp).internal),
            os.path.join(device_path, get_tmp_dir(policy)))
        self.logger.increment("async_pendings")

method get_diskfile*(self: BaseDiskFileManager, device, partition, account, container, obj: string,
                     policy, **kwargs) =
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        return self.diskfile_cls(self, dev_path, self.threadpools[device],
                                 partition, account, container, obj,
                                 policy=policy, use_splice=self.use_splice,
                                 pipe_size=self.pipe_size, **kwargs)

method object_audit_location_generator*(self: BaseDiskFileManager, device_dirs=nil) =
        return object_audit_location_generator(self.devices, self.mount_check,
                                               self.logger, device_dirs)

method get_diskfile_from_audit_location*(self: BaseDiskFileManager, audit_location) =
        dev_path = self.get_dev_path(audit_location.device, mount_check=false)
        return self.diskfile_cls.from_hash_dir(
            self, audit_location.path, dev_path,
            audit_location.partition, policy=audit_location.policy)

method get_diskfile_from_hash*(self: BaseDiskFileManager, device, partition, object_hash: string,
                               policy, **kwargs) =
        #[
        Returns a DiskFile instance for an object at the given
        object_hash. Just in case someone thinks of refactoring, be
        sure DiskFileDeleted is *not* raised, but the DiskFile
        instance representing the tombstoned object is returned
        instead.

        :raises DiskFileNotExist: if the object does not exist
        ]#

        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        object_path = os.path.join(
            dev_path, get_data_dir(policy), str(partition), object_hash[-3:],
            object_hash)
        try:
            filenames = self.hash_cleanup_listdir(object_path,
                                                  self.reclaim_age)
        except OSError as err:
            if err.errno == errno.ENOTDIR:
                quar_path = self.quarantine_renamer(dev_path, object_path)
                logging.exception(
                    _("Quarantined %(object_path)s to %(quar_path)s because "
                      "it is not a directory"), {"object_path": object_path,
                                                 "quar_path": quar_path})
                raise DiskFileNotExist()
            if err.errno != errno.ENOENT:
                raise
            raise DiskFileNotExist()
        if not filenames:
            raise DiskFileNotExist()
        try:
            metadata = read_metadata(os.path.join(object_path, filenames[-1]))
        except EOFError:
            raise DiskFileNotExist()
        try:
            account, container, obj = split_path(
                metadata.get("name", ""), 3, 3, true)
        except ValueError:
            raise DiskFileNotExist()
        return self.diskfile_cls(self, dev_path, self.threadpools[device],
                                 partition, account, container, obj,
                                 policy=policy, **kwargs)

method get_hashes*(self: BaseDiskFileManager, device, partition, suffixes, policy) =
        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        let partition_path = os.path.join(dev_path, get_data_dir(policy),
                                      partition)
        if not os.path.exists(partition_path):
            mkdirs(partition_path)
        _junk, hashes = self.threadpools[device].force_run_in_thread(
            self.get_hashes, partition_path, recalculate=suffixes)
        return hashes

method listdir*(self: BaseDiskFileManager, path: string): seq[string] =
        try:
            return os.listdir(path)
        except OSError as err:
            if err.errno != errno.ENOENT:
                self.logger.error(
                    "ERROR: Skipping %r due to error with listdir attempt: %s",
                    path, err)
        return []

iterator yield_suffixes*(self: BaseDiskFileManager, device, partition, policy) =
        #[
        Yields tuples of (full_path, suffix_only) for suffixes stored
        on the given device and partition.
        ]#

        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        partition_path = os.path.join(dev_path, get_data_dir(policy),
                                      partition)
        for suffix in self.listdir(partition_path):
            if len(suffix) != 3:
                continue
            try:
                int(suffix, 16)
            except ValueError:
                continue
            yield (os.path.join(partition_path, suffix), suffix)

iterator yield_hashes*(self: BaseDiskFileManager, device, partition, policy,
                     suffixes=nil, **kwargs) =
        #[
        Yields tuples of (full_path, hash_only, timestamps) for object
        information stored for the given device, partition, and
        (optionally) suffixes. If suffixes is nil, all stored
        suffixes will be searched for object hashes. Note that if
        suffixes is not nil but empty, such as [], then nothing will
        be yielded.

        timestamps is a dict which may contain items mapping:

            ts_data -> timestamp of data or tombstone file,
            ts_meta -> timestamp of meta file, if one exists

        where timestamps are instances of
        :class:`~swift.common.utils.Timestamp`
        ]#

        dev_path = self.get_dev_path(device)
        if not dev_path:
            raise DiskFileDeviceUnavailable()
        if suffixes is nil:
            suffixes = self.yield_suffixes(device, partition, policy)
        else:
            partition_path = os.path.join(dev_path,
                                          get_data_dir(policy),
                                          str(partition))
            suffixes = (
                (os.path.join(partition_path, suffix), suffix)
                for suffix in suffixes)
        key_preference = (
            ("ts_meta", "meta_info"),
            ("ts_data", "data_info"),
            ("ts_data", "ts_info"),
        )
        for suffix_path, suffix in suffixes:
            for object_hash in self.listdir(suffix_path):
                object_path = os.path.join(suffix_path, object_hash)
                try:
                    results = self.cleanup_ondisk_files(
                        object_path, self.reclaim_age, **kwargs)
                    timestamps = {}
                    for ts_key, info_key in key_preference:
                        if info_key not in results:
                            continue
                        timestamps[ts_key] = results[info_key]["timestamp"]
                    if "ts_data" not in timestamps:
                        # file sets that do not include a .data or .ts
                        # file cannot be opened and therefore cannot
                        # be ssync'd
                        continue
                    yield (object_path, object_hash, timestamps)
                except AssertionError as err:
                    self.logger.debug("Invalid file set in %s (%s)" % (
                        object_path, err))
                except DiskFileError as err:
                    self.logger.debug(
                        "Invalid diskfile filename in %r (%s)" % (
                            object_path, err))

