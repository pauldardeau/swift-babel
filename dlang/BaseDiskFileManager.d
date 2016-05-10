module BaseDiskFileManager;

import std.typecons;

import AuditLocation;
import Config;
import Logger;
import StoragePolicy;
import ThreadPool;


class BaseDiskFileManager {
    /*
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
    */

/*
    static {
        diskfile_cls = null;  // must be set by subclasses

        invalidate_hash = strip_self(invalidate_hash)
        quarantine_renamer = strip_self(quarantine_renamer)
    }
    */

private:
    Logger logger;
    string devices;
    int disk_chunk_size;
    int keep_cache_size;
    int bytes_per_sync;
    bool mount_check;
    int reclaim_age;
    bool replication_one_per_device;
    int replication_lock_timeout;
    int threads_per_disk;
    bool use_splice;


public:
    this(Config conf, Logger logger) {
        this.logger = logger;
        this.devices = conf.get("devices", "/srv/node");
        this.disk_chunk_size = to!int(conf.get("disk_chunk_size", "65536"));
        this.keep_cache_size = to!int(conf.get("keep_cache_size", "5242880"));
        this.bytes_per_sync = to!int(conf.get("mb_per_sync", "512")) * 1024 * 1024;
        this.mount_check = config_true_value(conf.get("mount_check", "true"));
        this.reclaim_age = to!int(conf.get("reclaim_age", ONE_WEEK));
        this.replication_one_per_device = config_true_value(
            conf.get("replication_one_per_device", "true"));
        this.replication_lock_timeout = to!int(conf.get(
            "replication_lock_timeout", "15"));
        int threads_per_disk = to!int(conf.get("threads_per_disk", "0"));
        this.threadpools = defaultdict(
            lambda: ThreadPool(nthreads=threads_per_disk));

        this.use_splice = false;
        this.pipe_size = null;

        bool conf_wants_splice = config_true_value(conf.get("splice", "no"));
        // If the operator wants zero-copy with splice() but we don't have the
        // requisite kernel support, complain so they can go fix it.
        if (conf_wants_splice && !splice.available) {
            this.logger.warning(
                "Use of splice() requested (config says \"splice = " ~
                conf.get("splice") ~ "\"), " ~
                "but the system does not support it. " ~
                "splice() will not be used.");
        } else if (conf_wants_splice && splice.available) {
            bool exception_raised = true;
            try {
                sockfd = get_md5_socket();
                os.close(sockfd);
                exception_raised = false;
            } catch (IOError err) {
                // AF_ALG socket support was introduced in kernel 2.6.38; on
                // systems with older kernels (or custom-built kernels lacking
                // AF_ALG support), we can't use zero-copy.
                if (err.errno != errno.EAFNOSUPPORT) {
                    throw err;
                }

                this.logger.warning("MD5 sockets not supported. "
                                    "splice() will not be used.");
            }

            if (!exception_raised) {
                this.use_splice = true;
                f = open("/proc/sys/fs/pipe-max-size");
                if (f != null) {
                    max_pipe_size = to!int(f.read());
                    f.close();
                }
                this.pipe_size = min(max_pipe_size, this.disk_chunk_size);
            }
        }
    }

    /**
    Parse an on disk file name.

    :param filename: the data file name including extension
    :returns: a dict, with keys for timestamp, and ext:

        * timestamp is a :class:`~swift.common.utils.Timestamp`
        * ext is a string, the file extension including the leading dot or
          the empty string if the filename has no extension.

       Subclases may add further keys to the returned dict.

    :raises DiskFileError: if any part of the filename is not able to be
                           validated.
    */
    abstract void parse_on_disk_filename(string filename);

    /**
    Called by get_ondisk_files(). Should be over-ridden to implement
    subclass specific handling of files.

    :param exts: dict of lists of file info, keyed by extension
    :param results: a dict that may be updated with results
    */
    abstract void _process_ondisk_files(exts, results);

    /**
    Verify that the final combination of on disk files complies with the
    diskfile contract.

    :param results: files that have been found and accepted
    :returns: true if the file combination is compliant, false otherwise
    */
    bool _verify_ondisk_files(results) {
        Tuple!() data_file, meta_file, ts_file = tuple(
            [results[key]
             for key in ["data_file", "meta_file", "ts_file"]]);

        return ((data_file == null && meta_file == null && ts_file == null)
                || (ts_file != null && data_file == null
                    && meta_file == null)
                || (data_file != null && ts_file == null));
    }

    void _split_list(original_list, condition) {
        /*
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
        */
        foreach (i, item; enumerate(original_list)) {
            if (!condition(item)) {
                return tuple(original_list[:i], original_list[i:]);
            }
        }
        return tuple(original_list, []);
    }

    /**
    Given a list of file info dicts, reverse sorted by timestamp, split the
    list into two: items newer than timestamp, and items at same time or
    older than timestamp.

    :param file_info_list: a list of file_info dicts.
    :param timestamp: a Timestamp.
    :return: a tuple of two lists.
    */
    void _split_gt_timestamp(file_info_list, timestamp) {
        return this._split_list(
            file_info_list, lambda x: x["timestamp"] > timestamp);
    }

    /**
    Given a list of file info dicts, reverse sorted by timestamp, split the
    list into two: items newer than or at same time as the timestamp, and
    items older than timestamp.

    :param file_info_list: a list of file_info dicts.
    :param timestamp: a Timestamp.
    :return: a tuple of two lists.
    */
    void _split_gte_timestamp(file_info_list, timestamp) {
        return this._split_list(
            file_info_list, lambda x: x["timestamp"] >= timestamp);
    }

    void get_ondisk_files(string[] files,
                          string datadir,
                          bool verify=true) {
        /*
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
        valid fileset, or None.

        :param files: a list of file names.
        :param datadir: directory name files are from.
        :param verify: if true verify that the ondisk file contract has not
                       been violated, otherwise do not verify.
        :returns: a dict that will contain keys:
                    ts_file   -> path to a .ts file or None
                    data_file -> path to a .data file or None
                    meta_file -> path to a .meta file or None
                  and may contain keys:
                    ts_info   -> a file info dict for a .ts file
                    data_info -> a file info dict for a .data file
                    meta_info -> a file info dict for a .meta file
                    obsolete  -> a list of file info dicts for obsolete files
        */
        // Build the exts data structure:
        // exts is a dict that maps file extensions to a list of file_info
        // dicts for the files having that extension. The file_info dicts are of
        // the form returned by parse_on_disk_filename, with the filename added.
        // Each list is sorted in reverse timestamp order.
        //
        // The exts dict will be modified during subsequent processing as files
        // are removed to be discarded or ignored.
        exts = defaultdict(list);
        foreach (afile; files) {
            // Categorize files by extension
            try {
                file_info = this.parse_on_disk_filename(afile);
                file_info["filename"] = afile;
                exts[file_info["ext"]].append(file_info);
            } catch (DiskFileError e) {
                this.logger.warning("Unexpected file %s: %s" %
                                    (OSUtils.path_join(datadir || "", afile), e));
            }
        }

        foreach (ext; exts) {
            // For each extension sort files into reverse chronological order.
            exts[ext] = sorted(
                exts[ext], key=lambda info: info["timestamp"], reverse=true);
        }

        // the results dict is used to collect results of file filtering
        results = {};

        // non-tombstones older than or equal to latest tombstone are obsolete
        if (exts.get(".ts")) {
            for (ext in filter(lambda ext: ext != ".ts", exts.keys())) {
                exts[ext], older = this._split_gt_timestamp(
                    exts[ext], exts[".ts"][0]["timestamp"])
                results.setdefault("obsolete", []).extend(older)
            }
        }

        // all but most recent .meta and .ts are obsolete
        foreach (ext; (".meta", ".ts")) {
            if (ext in exts) {
                results.setdefault("obsolete", []).extend(exts[ext][1:])
                exts[ext] = exts[ext][:1]
            }
        }

        // delegate to subclass handler
        this._process_ondisk_files(exts, results);

        // set final choice of files
        if (exts.get(".ts")) {
            results["ts_info"] = exts[".ts"][0];
        }

        if ("data_info" in results && exts.get(".meta")) {
            // only report a meta file if there is a data file
            results["meta_info"] = exts[".meta"][0];
        }

        // set ts_file, data_file and meta_file with path to chosen file or None
        foreach (info_key; ("data_info", "meta_info", "ts_info")) {
            HashMap info = results.get(info_key);
            key = info_key[:-5] ~ "_file";
            results[key] = join(datadir, info["filename"]) if info else null;
        }

        if (verify) {
            assert this._verify_ondisk_files(
                results),
                "On-disk file search algorithm contract is broken: %s"
                % str(results)
        }

        return results;
    }

    HashMap cleanup_ondisk_files(hsh_path,
                                 int reclaim_age=ONE_WEEK) {
        /*
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
        */
        def is_reclaimable(timestamp):
            return (time.time() - float(timestamp)) > reclaim_age

        string[] files = listdir(hsh_path);
        files.sort(reverse=true);
        results = this.get_ondisk_files(
            files, hsh_path, verify=false);
        if "ts_info" in results && is_reclaimable(
                results["ts_info"]["timestamp"]):
            remove_file(join(hsh_path, results["ts_info"]["filename"]))
            files.remove(results.pop("ts_info")["filename"])
        foreach (file_info; results.get("possible_reclaim", [])) {
            // stray fragments are not deleted until reclaim-age
            if (is_reclaimable(file_info["timestamp"])) {
                results.setdefault("obsolete", []).append(file_info);
            }
        }
        foreach (file_info; results.get("obsolete", [])) {
            remove_file(join(hsh_path, file_info["filename"]));
            files.remove(file_info["filename"]);
        }
        results["files"] = files;
        return results;
    }

    /**
    List contents of a hash directory and clean up any old files.
    For EC policy, delete files older than a .durable or .ts file.

    :param hsh_path: object hash path
    :param reclaim_age: age in seconds at which to remove tombstones
    :returns: list of files remaining in the directory, reverse sorted
    */
    void hash_cleanup_listdir(hsh_path, int reclaim_age=ONE_WEEK) {
        // maintain compatibility with 'legacy' hash_cleanup_listdir
        // return value
        return this.cleanup_ondisk_files(
            hsh_path, reclaim_age=reclaim_age)["files"]
    }

    void _hash_suffix_dir(string path, mapper, int reclaim_age) {
        hashes = defaultdict(hashlib.md5);
        string[] path_contents;

        try {
            path_contents = OSUtils.listdir(path);
            sort(path_contents);
        } catch (OSError err) {
            if (err.errno in (errno.ENOTDIR, errno.ENOENT)) {
                throw new PathNotDir();
            }
            throw err;
        }

        foreach (hsh; path_contents) {
            string hsh_path = join(path, hsh);
            string[] files = null;

            try {
                files = this.hash_cleanup_listdir(hsh_path, reclaim_age);
            } catch (OSError err) {
                if (err.errno == errno.ENOTDIR) {
                    string partition_path = dirname(path);
                    string objects_path = dirname(partition_path);
                    string device_path = dirname(objects_path);
                    quar_path = quarantine_renamer(device_path, hsh_path);
                    logging.exception(
                        "Quarantined %(hsh_path)s to %(quar_path)s because "
                          "it is not a directory", {"hsh_path": hsh_path,
                                                     "quar_path": quar_path});
                    continue;
                }
                throw err;
            }

            if (files == null) {
                try {
                    os.rmdir(hsh_path);
                } catch (OSError e) {
                    //pass
                }
            }

            foreach (filename; files) {
                key, value = mapper(filename)
                hashes[key].update(value)
            }
        }

        bool exception_raised = true;

        try {
            os.rmdir(path);
            exception_raised = false;
        } catch (OSError e) {
            if (e.errno == errno.ENOENT) {
                throw new PathNotDir();
            }
        }

        if (!exception_raised) {
            // if we remove it, pretend like it wasn't there to begin with so
            // that the suffix key gets removed
            throw new PathNotDir();
        }

        return hashes;
    }

    /** PJD - no doc for path param? */
    void _hash_suffix(path, int reclaim_age) {
        /*
        Performs reclamation and returns an md5 of all (remaining) files.

        :param reclaim_age: age in seconds at which to remove tombstones
        :raises PathNotDir: if given path is not a valid directory
        :raises OSError: for non-ENOTDIR errors
        */
        throw NotImplementedError;
    }

    Tuple!(int,string[string]) _get_hashes(string partition_path,
                            recalculate=null,
                            bool do_listdir=false,
                            int reclaim_age=null) {
        /*
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
        */
        reclaim_age = reclaim_age || this.reclaim_age;
        int hashed = 0;
        string hashes_file = join(partition_path, HASH_FILE)
        bool modified = false;
        bool force_rewrite = false;
        hashes = {}
        int mtime = -1;

        if (recalculate == null) {
            recalculate = [];
        }

        try {
            with (open(hashes_file, "rb") as fp) {
                hashes = pickle.load(fp);
            }
            mtime = getmtime(hashes_file);
        } catch (Exception e) {
            do_listdir = true;
            force_rewrite = true;
        }

        if (do_listdir) {
            foreach (suff; OSUtils.listdir(partition_path)) {
                if (len(suff) == 3) {
                    hashes.setdefault(suff, null);
                }
            }
            modified = true;
        }

        hashes.update((suffix, null) for suffix in recalculate);

        foreach (suffix, hash_; hashes.items()) {
            if (!hash_) {
                suffix_dir = join(partition_path, suffix);
                try {
                    hashes[suffix] = this._hash_suffix(suffix_dir, reclaim_age);
                    hashed += 1;
                } catch (PathNotDir e) {
                    del hashes[suffix];
                } catch (OSError e) {
                    logging.exception("Error hashing suffix");
                }
                modified = true;
            }
        }

        if (modified) {
            with (lock_path(partition_path)) {
                if (force_rewrite || !exists(hashes_file) ||
                        getmtime(hashes_file) == mtime) {
                    write_pickle(
                        hashes, hashes_file, partition_path, PICKLE_PROTOCOL);
                    return tuple(hashed, hashes);
                }
            }
            return this._get_hashes(partition_path, recalculate, do_listdir,
                                    reclaim_age);
        } else {
            return tuple(hashed, hashes);
        }
    }

    /**
    Construct the path to a device without checking if it is mounted.

    :param device: name of target device
    :returns: full path to the device
    */
    string construct_dev_path(string device) {
        return OSUtils.path_join(this.devices, device);
    }

    string get_dev_path(string device) {
        return get_dev_path(device, false);
    }

    /**
    Return the path to a device, first checking to see if either it
    is a proper mount point, or at least a directory depending on
    the mount_check configuration option.

    :param device: name of target device
    :param mount_check: whether or not to check mountedness of device.
                        Defaults to bool(self.mount_check).
    :returns: full path to the device, None if the path to the device is
              not a proper mount point or directory.
    */
    string get_dev_path(string device, bool mount_check) {
        // we'll do some kind of check unless explicitly forbidden
        if (mount_check is not false) {
            bool check_result = false;
            if (mount_check || this.mount_check) {
                check_result = check_mount(this.devices, device);
            } else {
                check_result = check_dir(this.devices, device);
            }

            if (!check_result) {
                return null;
            }
        }

        return OSUtils.path_join(this.devices, device);
    }

    /** PJD: no documentation of device argument? */
    @contextmanager
    void replication_lock(string device) {
        /*
        A context manager that will lock on the device given, if
        configured to do so.

        :raises ReplicationLockTimeout: If the lock on the device
            cannot be granted within the configured timeout.
        */
        if (this.replication_one_per_device) {
            string dev_path = this.get_dev_path(device);
            with (lock_path(
                    dev_path,
                    timeout=this.replication_lock_timeout,
                    timeout_class=ReplicationLockTimeout)) {
                yield true;
            }
        } else {
            yield true;
        }
    }

    void pickle_async_update(string device,
                                    account,
                                    container,
                                    obj,
                                    data,
                                    timestamp,
                                    StoragePolicy policy) {
        string device_path = this.construct_dev_path(device);
        string async_dir = OSUtils.path_join(device_path, get_async_dir(policy));
        string ohash = hash_path(account, container, obj);
        this.threadpools[device].run_in_thread(
            write_pickle,
            data,
            OSUtils.path_join(async_dir, ohash[-3:], ohash ~ "-" ~
                         Timestamp(timestamp).internal),
            OSUtils.path_join(device_path, get_tmp_dir(policy)));
        this.logger.increment("async_pendings");
    }

    void get_diskfile(string device,
                             partition,
                             account,
                             container,
                             obj,
                             StoragePolicy policy,
                             kwargs) {
        string dev_path = this.get_dev_path(device);
        if (dev_path == null || dev_path.length() == 0) {
            throw new DiskFileDeviceUnavailable();
        }
        return this.diskfile_cls(this, dev_path, this.threadpools[device],
                                 partition, account, container, obj,
                                 policy=policy, use_splice=this.use_splice,
                                 pipe_size=this.pipe_size, kwargs);
    }

    void object_audit_location_generator(device_dirs=null) {
        return object_audit_location_generator(this.devices,
                                               this.mount_check,
                                               this.logger,
                                               device_dirs);
    }

    void get_diskfile_from_audit_location(AuditLocation audit_location) {
        string dev_path = this.get_dev_path(audit_location.device,
                                            false); //mount_check
        return this.diskfile_cls.from_hash_dir(
            this, audit_location.path, dev_path,
            audit_location.partition, policy=audit_location.policy);
    }

    /**
    Returns a DiskFile instance for an object at the given
    object_hash. Just in case someone thinks of refactoring, be
    sure DiskFileDeleted is *not* raised, but the DiskFile
    instance representing the tombstoned object is returned
    instead.

    :raises DiskFileNotExist: if the object does not exist
    */
    void get_diskfile_from_hash(string device,
                                       partition,
                                       object_hash,
                                       StoragePolicy policy,
                                       kwargs) {
        string dev_path = this.get_dev_path(device);
        if (dev_path == null || dev_path.length() == 0) {
            throw new DiskFileDeviceUnavailable();
        }
        string object_path = OSUtils.path_join(
            dev_path, get_data_dir(policy), str(partition), object_hash[-3:],
            object_hash);
        try {
            filenames = this.hash_cleanup_listdir(object_path,
                                                  this.reclaim_age);
        } catch (OSError err) {
            if (err.errno == errno.ENOTDIR) {
                quar_path = this.quarantine_renamer(dev_path, object_path);
                logging.exception(
                    "Quarantined %(object_path)s to %(quar_path)s because "
                      "it is not a directory", {"object_path": object_path,
                                                 "quar_path": quar_path});
                throw new DiskFileNotExist();
            }

            if (err.errno != errno.ENOENT) {
                throw err;
            }

            throw new DiskFileNotExist();
        }

        if (!filenames) {
            throw new DiskFileNotExist();
        }

        try {
            metadata = read_metadata(OSUtils.path_join(object_path, filenames[-1]));
        } catch (EOFError e) {
            throw new DiskFileNotExist();
        }

        try {
            account, container, obj = split_path(
                metadata.get("name", ""), 3, 3, true);
        } catch (ValueError e) {
            throw new DiskFileNotExist();
        }

        return this.diskfile_cls(this, dev_path, this.threadpools[device],
                                 partition, account, container, obj,
                                 policy=policy, kwargs);
    }

    void get_hashes(string device,
                           partition,
                           suffixes,
                           StoragePolicy policy) {
        string dev_path = this.get_dev_path(device);
        if (dev_path == null || dev_path.length() == 0) {
            throw new DiskFileDeviceUnavailable();
        }
        string partition_path = OSUtils.path_join(dev_path, get_data_dir(policy),
                                      partition);
        if (!os.path.exists(partition_path)) {
            mkdirs(partition_path);
        }
        _junk, hashes = this.threadpools[device].force_run_in_thread(
            this._get_hashes, partition_path, recalculate=suffixes);
        return hashes;
    }

    string[] _listdir(string path) {
        try {
            return OSUtils.listdir(path);
        } catch (OSError err) {
            if (err.errno != errno.ENOENT) {
                this.logger.error(
                    "ERROR: Skipping %r due to error with listdir attempt: %s",
                    path, err);
            }
        }
        return [];
    }

    void yield_suffixes(string device, partition, StoragePolicy policy) {
        /*
        Yields tuples of (full_path, suffix_only) for suffixes stored
        on the given device and partition.
        */
        string dev_path = this.get_dev_path(device);
        if (dev_path == null || dev_path.length() == 0) {
            throw new DiskFileDeviceUnavailable();
        }
        string partition_path = OSUtils.path_join(dev_path, get_data_dir(policy),
                                      partition);
        foreach (suffix; this._listdir(partition_path)) {
            if (len(suffix) != 3) {
                continue;
            }

            try {
                int(suffix, 16);
            } catch (ValueError ignored) {
                continue;
            }
            yield tuple(OSUtils.path_join(partition_path, suffix), suffix);
        }
    }

    void yield_hashes(string device,
                        partition,
                        StoragePolicy policy,
                        suffixes=null,
                        kwargs) {
        /*
        Yields tuples of (full_path, hash_only, timestamps) for object
        information stored for the given device, partition, and
        (optionally) suffixes. If suffixes is None, all stored
        suffixes will be searched for object hashes. Note that if
        suffixes is not None but empty, such as [], then nothing will
        be yielded.

        timestamps is a dict which may contain items mapping:

            ts_data -> timestamp of data or tombstone file,
            ts_meta -> timestamp of meta file, if one exists

        where timestamps are instances of
        :class:`~swift.common.utils.Timestamp`
        */
        string dev_path = this.get_dev_path(device);
        if (dev_path == null || dev_path.length() == 0) {
            throw new DiskFileDeviceUnavailable();
        }

        if (suffixes == null) {
            suffixes = this.yield_suffixes(device, partition, policy);
        } else {
            string partition_path = OSUtils.path_join(dev_path,
                                          get_data_dir(policy),
                                          str(partition));
            suffixes = (
                (OSUtils.path_join(partition_path, suffix), suffix)
                for suffix in suffixes);
        }

        key_preference = (
            ("ts_meta", "meta_info"),
            ("ts_data", "data_info"),
            ("ts_data", "ts_info"),
        );

        foreach (suffix_path, suffix; suffixes) {
            foreach (object_hash; this._listdir(suffix_path) {
                string object_path = OSUtils.path_join(suffix_path, object_hash);
                try {
                    results = this.cleanup_ondisk_files(;
                        object_path, this.reclaim_age, kwargs)
                    timestamps = {};
                    foreach (ts_key, info_key; key_preference) {
                        if (info_key not in results) {
                            continue;
                        }
                        timestamps[ts_key] = results[info_key]["timestamp"];
                    }

                    if ("ts_data" not in timestamps) {
                        // file sets that do not include a .data or .ts
                        // file cannot be opened and therefore cannot
                        // be ssync'd
                        continue;
                    }

                    yield tuple(object_path, object_hash, timestamps);
                } catch (AssertionError err) {
                    this.logger.debug("Invalid file set in " ~ object_path ~
                                      " (" ~ err ~ ")";
                } catch (DiskFileError err) {
                    this.logger.debug(
                        "Invalid diskfile filename in %r (%s)" % (
                            object_path, err));
                }
            }
        }
    }
}

