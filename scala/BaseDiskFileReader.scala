

class BaseDiskFileReader {

    private Boolean _keep_cache;
    private Boolean _use_splice;
    private Boolean _started_at_0;
    private Boolean _read_to_eof;
    private Object _fp;
    private String _data_file;
    private Int _obj_size;
    private Int _pipe_size;
    private Object _etag;
    private QuarantineHook _quarantine_hook;
    private Int _bytes_read;
    private ThreadPool _threadpool;
    private Logger _logger;

    /**
    Encapsulation of the WSGI read context for servicing GET REST API
    requests. Serves as the context manager object for the
    :class:`swift.obj.diskfile.DiskFile` class's
    :func:`swift.obj.diskfile.DiskFile.reader` method.

    .. note::

        The quarantining behavior of this method is considered implementation
        specific, and is not required of the API.

    .. note::

        The arguments to the constructor are considered implementation
        specific. The API does not define the constructor arguments.

    @param fp open file object pointer reference
    @param data_file on-disk data file name for the object
    @param obj_size verified on-disk size of the object
    @param etag expected metadata etag value for entire file
    @param threadpool thread pool to use for read operations
    @param disk_chunk_size size of reads from disk in bytes
    @param keep_cache_size maximum object size that will be kept in cache
    @param device_path on-disk device path, used when quarantining an obj
    @param logger logger caller wants this object to use
    @param quarantine_hook 1-arg callable called w/reason when quarantined
    @param use_splice if true, use zero-copy splice() to send data
    @param pipe_size size of pipe buffer used in zero-copy operations
    @param diskfile the diskfile creating this DiskFileReader instance
    @param keep_cache should resulting reads be kept in the buffer cache
    */
    def BaseDiskFileReader(fp: Object,
                              data_file: String,
                              obj_size: Int,
                              etag: Object,
                              threadpool: ThreadPool,
                              disk_chunk_size: Int,
                              keep_cache_size: Boolean,
                              device_path: String,
                              logger: Logger,
                              quarantine_hook: QuarantineHook,
                              use_splice: Boolean,
                              pipe_size: Int,
                              diskfile: DiskFile,
                              keep_cache: Boolean) { //=false
        // Parameter tracking
        this._fp = fp;
        this._data_file = data_file;
        this._obj_size = obj_size;
        this._etag = etag;
        this._threadpool = threadpool;
        this._diskfile = diskfile;
        this._disk_chunk_size = disk_chunk_size;
        this._device_path = device_path;
        this._logger = logger;
        this._quarantine_hook = quarantine_hook;
        this._use_splice = use_splice;
        this._pipe_size = pipe_size;
        if (keep_cache) {
            // Caller suggests we keep this in cache, only do it if the
            // object's size is less than the maximum.
            this._keep_cache = (obj_size < keep_cache_size);
        } else {
            this._keep_cache = false;
        }

        // Internal Attributes
        this._iter_etag = null; 
        this._bytes_read = 0;
        this._started_at_0 = false;
        this._read_to_eof = false;
        this._md5_of_sent_bytes = null;
        this._suppress_file_closing = false;
        this._quarantined_dir = null;
    }

    @property
    def manager() {
        return this._diskfile.manager;
    }

    /** Returns an iterator over the data file. */
    def __iter__() {
        try {
            int dropped_cache = 0;
            this._bytes_read = 0;
            this._started_at_0 = false;
            this._read_to_eof = false;
            if (this._fp.tell() == 0) {
                this._started_at_0 = true;
                this._iter_etag = hashlib.md5();
            }

            while (true) {
                byte[] chunk = this._threadpool.run_in_thread(
                    this._fp.read, this._disk_chunk_size);
                if (chunk) {
                    if (this._iter_etag) {
                        this._iter_etag.update(chunk);
                    }
                    this._bytes_read += chunk.length;
                    if (this._bytes_read - dropped_cache > DROP_CACHE_WINDOW) {
                        this._drop_cache(this._fp.fileno(), dropped_cache,
                                         this._bytes_read - dropped_cache);
                        dropped_cache = this._bytes_read;
                    }
                    yield chunk;
                } else {
                    this._read_to_eof = true;
                    this._drop_cache(this._fp.fileno(), dropped_cache,
                                     this._bytes_read - dropped_cache);
                    break;
                }
            }
        } finally {
            if (!this._suppress_file_closing) {
                this.close();
            }
        }
    }

    def can_zero_copy_send() : Boolean = {
        return this._use_splice;
    }

    /**
    Does some magic with splice() and tee() to move stuff from disk to
    network without ever touching userspace.

    @param wsockfd file descriptor (integer) of the socket out which to
                   send data
    */
    def zero_copy_send(wsockfd: Int) {
        // Note: if we ever add support for zero-copy ranged GET responses,
        // we'll have to make this conditional.
        this._started_at_0 = true;

        rfd = this._fp.fileno();
        client_rpipe, client_wpipe = os.pipe();
        hash_rpipe, hash_wpipe = os.pipe();
        md5_sockfd = get_md5_socket();

        // The actual amount allocated to the pipe may be rounded up to the
        // nearest multiple of the page size. If we have the memory allocated,
        // we may as well use it.
        //
        // Note: this will raise IOError on failure, so we don't bother
        // checking the return value.
        int pipe_size = fcntl.fcntl(client_rpipe, F_SETPIPE_SZ, this._pipe_size);
        fcntl.fcntl(hash_rpipe, F_SETPIPE_SZ, pipe_size);

        int dropped_cache = 0;
        this._bytes_read = 0;
        int bytes_copied;

        try {
            while (true) {
                // Read data from disk to pipe
                (bytes_in_pipe, _1, _2) = this._threadpool.run_in_thread(
                    splice, rfd, null, client_wpipe, null, pipe_size, 0);
                if (bytes_in_pipe == 0) {
                    this._read_to_eof = true;
                    this._drop_cache(rfd, dropped_cache,
                                     this._bytes_read - dropped_cache);
                    break;
                }
                this._bytes_read += bytes_in_pipe;

                // "Copy" data from pipe A to pipe B (really just some pointer
                // manipulation in the kernel, not actual copying).
                bytes_copied = tee(client_rpipe, hash_wpipe, bytes_in_pipe, 0);
                if (bytes_copied != bytes_in_pipe) {
                    // We teed data between two pipes of equal size, and the
                    // destination pipe was empty. If, somehow, the destination
                    // pipe was full before all the data was teed, we should
                    // fail here. If we don't raise an exception, then we will
                    // have the incorrect MD5 hash once the object has been
                    // sent out, causing a false-positive quarantine.
                    throw new Exception("tee() failed: tried to move " +
                                        bytes_in_pipe +
                                        " bytes, but only moved " +
                                        bytes_copied);
                }
                // Take the data and feed it into an in-kernel MD5 socket. The
                // MD5 socket hashes data that is written to it. Reading from
                // it yields the MD5 checksum of the written data.
                //
                // Note that we don't have to worry about splice() returning
                // null here (which happens on EWOULDBLOCK); we're splicing
                // $bytes_in_pipe bytes from a pipe with exactly that many
                // bytes in it, so read won't block, and we're splicing it into
                // an MD5 socket, which synchronously hashes any data sent to
                // it, so writing won't block either.
                (hashed, _1, _2) = splice(hash_rpipe, null, md5_sockfd, null,
                                          bytes_in_pipe, splice.SPLICE_F_MORE);
                if (hashed != bytes_in_pipe) {
                    throw new Exception("md5 socket didn't take all the data? " +
                                    "(tried to write " + bytes_in_pipe +
                                    ", but wrote " + hashed + ")");
                }

                while (bytes_in_pipe > 0) {
                    try {
                        res = splice(client_rpipe, null, wsockfd, null,
                                     bytes_in_pipe, 0);
                        bytes_in_pipe -= res[0];
                    } catch (IOError exc) {
                        if (exc.errno == errno.EWOULDBLOCK) {
                            trampoline(wsockfd, true); //write
                        } else {
                            throw exc;
                        }
                    }
                }

                if (this._bytes_read - dropped_cache > DROP_CACHE_WINDOW) {
                    this._drop_cache(rfd, dropped_cache,
                                     this._bytes_read - dropped_cache);
                    dropped_cache = this._bytes_read;
                }
            }
        } finally {
            // Linux MD5 sockets return '00000000000000000000000000000000' for
            // the checksum if you didn't write any bytes to them, instead of
            // returning the correct value.
            if (this._bytes_read > 0) {
                bin_checksum = os.read(md5_sockfd, 16);
                hex_checksum = "".join("%02x" % ord(c) for c in bin_checksum);
            } else {
                hex_checksum = MD5_OF_EMPTY_STRING;
            }
            this._md5_of_sent_bytes = hex_checksum;

            os.close(client_rpipe);
            os.close(client_wpipe);
            os.close(hash_rpipe);
            os.close(hash_wpipe);
            os.close(md5_sockfd);
            this.close();
        }
    }

    /**Returns an iterator over the data file for range (start, stop)*/
    def app_iter_range(start: Int, stop: Int) {
        if (start > -1) {
            this._fp.seek(start);
        }

        if (stop > 0) {
            length = stop - start;
        } else {
            length = null;
        }

        try {
            for (byte[] chunk : this) {
                if (length != null) {
                    length -= chunk.length;
                    if (length < 0) {
                        // Chop off the extra:
                        yield chunk[:length];
                        break;
                    }
                }
                yield chunk;
            }
        } finally {
            if (!this._suppress_file_closing) {
                this.close();
            }
        }
    }

    /**Returns an iterator over the data file for a set of ranges*/
    def app_iter_ranges(ranges, content_type, boundary, size) {
        if (!ranges) {
            yield "";
        } else {
            try {
                this._suppress_file_closing = true;
                for (byte[] chunk : multi_range_iterator(
                        ranges, content_type, boundary, size,
                        this.app_iter_range)) {
                    yield chunk;
                }
            } finally {
                this._suppress_file_closing = false;
                this.close();
            }
        }
    }

    /**Method for no-oping buffer cache drop method.*/
    def _drop_cache(fd: Int, offset: Int, length: Int) {
        if (!this._keep_cache) {
            drop_buffer_cache(fd, offset, length);
        }
    }

    def _quarantine(msg: String) {
        this._quarantined_dir = this._threadpool.run_in_thread(
            this.manager.quarantine_renamer, this._device_path,
            this._data_file);
        this._logger.warning("Quarantined object " +
                             this._data_file + ": " + msg);
        this._logger.increment("quarantines");
        if (this._quarantine_hook != null) {
            this._quarantine_hook.onQuarantine(msg);
        }
    }

    /**Check if file needs to be quarantined*/
    def _handle_close_quarantine() {
        if (this._iter_etag && !this._md5_of_sent_bytes) {
            this._md5_of_sent_bytes = this._iter_etag.hexdigest();
        }

        if (this._bytes_read != this._obj_size) {
            this._quarantine(
                "Bytes read: " + this._bytes_read +
                ", does not match metadata: " + this._obj_size);
        } else if (this._md5_of_sent_bytes &&
                   this._etag != this._md5_of_sent_bytes) {
            this._quarantine(
                "ETag " + this._etag + " and file's md5 " +
                this._md5_of_sent_bytes + " do not match");
        }
    }

    /**
    Close the open file handle if present.

    For this specific implementation, this method will handle quarantining
    the file if necessary.
    */
    def close() {
        if (this._fp) {
            try {
                if (this._started_at_0 && this._read_to_eof) {
                    this._handle_close_quarantine();
                }
            } catch (DiskFileQuarantined dfq) {
                throw dfq;
            } catch (Exception e) { //, Timeout) as e:
            /*
                this._logger.error(_(
                    'ERROR DiskFile %(data_file)s'
                    ' close failure: %(exc)s : %(stack)s'),
                    {'exc': e, 'stack': "".join(traceback.format_stack()),
                     'data_file': this._data_file});
                */
            } finally {
                fp, this._fp = this._fp, null;
                fp.close();
            }
        }
    }
}

