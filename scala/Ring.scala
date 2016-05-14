import java.util.TreeSet;


class Ring {

    serialized_path: String
    reload_time: Int
    _rtime: Int
    _mtime: Int
    //private Object _devs;
    _num_regions: Int
    _num_zones: Int
    _num_devs: Int
    _num_ips: Int
    _replica2part2dev_id: Array[Array[Int]]
    _part_shift: Int
    //private Object tier2devs;
    //private Object tiers_by_length;


    public Ring(String serialized_path) {
        this(serialized_path, 15);
    }

    public Ring(String serialized_path,
                int reload_time) {
        this(serialized_path, reload_time, null);
    }

    /**
    Partitioned consistent hashing ring.

    @param serialized_path path to serialized RingData instance
    @param reload_time time interval in seconds to check for a ring change
    @param ring_name
    */
    public Ring(String serialized_path,
                int reload_time,
                String ring_name) {
        // can't use the ring unless HASH_PATH_SUFFIX is set
        validate_configuration();
        if (ring_name != null) {
            this.serialized_path = os.path.join(serialized_path,
                                                ring_name + ".ring.gz");
        } else {
            this.serialized_path = os.path.join(serialized_path);
        }
        this.reload_time = reload_time;
        this._reload(true);
    }

    protected void _reload() {
        _reload(false);
    }

    protected void _reload(boolean force) {
        this._rtime = time() + this.reload_time;
        if (force || this.has_changed()) {
            RingData ring_data = RingData.load(this.serialized_path);
            this._mtime = getmtime(this.serialized_path);
            this._devs = ring_data.devs;
            // NOTE(akscram): Replication parameters like replication_ip
            //                and replication_port are required for
            //                replication process. An old replication
            //                ring doesn't contain this parameters into
            //                device. Old-style pickled rings won't have
            //                region information.
            for (dev : this._devs) {
                if (dev) {
                    dev.setdefault("region", 1);
                    if ("ip" in dev) {
                        dev.setdefault("replication_ip", dev["ip"]);
                    }
                    if ("port" in dev) {
                        dev.setdefault("replication_port", dev["port"]);
                    }
                }
            }

            this._replica2part2dev_id = ring_data._replica2part2dev_id;
            this._part_shift = ring_data._part_shift;
            this._rebuild_tier_data();

            // Do this now, when we know the data has changed, rather than
            // doing it on every call to get_more_nodes().
            //
            // Since this is to speed up the finding of handoffs, we only
            // consider devices with at least one partition assigned. This
            // way, a region, zone, or server with no partitions assigned
            // does not count toward our totals, thereby keeping the early
            // bailouts in get_more_nodes() working.
            SortedSet<Integer> dev_ids_with_parts = new TreeSet<>();
            for (part2dev_id in this._replica2part2dev_id) {
                for (int dev_id : part2dev_id) {
                    dev_ids_with_parts.add(dev_id);
                }
            }

            SortedSet<Integer> regions = new TreeSet<>();
            zones = set();
            ips = set();
            this._num_devs = 0;
            for (dev in this._devs) {
                if (dev && dev["id"] in dev_ids_with_parts) {
                    regions.add(dev["region"]);
                    zones.add((dev["region"], dev["zone"]));
                    ips.add((dev["region"], dev["zone"], dev["ip"]));
                    this._num_devs += 1;
                }
            }
            this._num_regions = regions.size();
            this._num_zones = len(zones);
            this._num_ips = len(ips);
        }
    }

    protected void _rebuild_tier_data() {
        this.tier2devs = defaultdict(list);
        for (dev in this._devs) {
            if (not dev) {
                continue;
            }
            for (tier in tiers_for_dev(dev)) {
                this.tier2devs[tier].append(dev);
            }
        }

        tiers_by_length = defaultdict(list);
        for (tier in this.tier2devs) {
            tiers_by_length[len(tier)].append(tier);
        }
        this.tiers_by_length = sorted(tiers_by_length.values(),
                                      key=lambda x: len(x[0]));
        for (tiers in this.tiers_by_length) {
            tiers.sort();
        }
    }

    //@property
    /**
    Number of replicas (full or partial) used in the ring.
    */
    public int replica_count() {
        return len(this._replica2part2dev_id);
    }

    //@property
    /**
    Number of partitions in the ring.
    */
    public int partition_count() {
        return len(this._replica2part2dev_id[0]);
    }

    //@property
    /**
    * devices in the ring
    */
    public void devs() {
        if (time() > this._rtime) {
            this._reload();
        }
        return this._devs;
    }

    /**
    Check to see if the ring on disk is different than the current one in
    memory.

    @return true if the ring on disk has changed, false otherwise
    */
    public boolean has_changed() {
        return getmtime(this.serialized_path) != this._mtime;
    }

    /**
     *
     * @param part
     */
    public void _get_part_nodes(int part) {
        part_nodes = [];
        SortedSet<Integer> seen_ids = new TreeSet<>();
        for (int[] r2p2d : this._replica2part2dev_id) {
            if (part < r2p2d.length) {
                int dev_id = r2p2d[part];
                if (!seen_ids.contains(dev_id)) {
                    part_nodes.add(this.devs[dev_id]);
                    seen_ids.add(dev_id);
                }
            }
        }
        return [dict(node, index=i) for i, node in enumerate(part_nodes)];
    }

    /**
    Get the partition for an account/container/object.

    @param account account name
    @param container container name
    @param obj object name
    @return the partition number
    */
    public int get_part(String account,
                        String container=null,
                        String obj=null) {
        key = hash_path(account, container, obj, raw_digest=true);
        if (time() > this._rtime) {
            this._reload();
        }
        int part = struct.unpack_from(">I", key)[0] >> this._part_shift;
        return part;
    }

    /**
    Get the nodes that are responsible for the partition. If one
    node is responsible for more than one replica of the same
    partition, it will only appear in the output once.

    @param part partition to get nodes for
    @return list of node dicts

    See :func:`get_nodes` for a description of the node dicts.
    */
    public void get_part_nodes(int part) {
        if (time() > this._rtime) {
            this._reload();
        }
        return this._get_part_nodes(part);
    }

    /**
    Get the partition and nodes for an account/container/object.
    If a node is responsible for more than one replica, it will
    only appear in the output once.

    @param account account name
    @param container container name
    @param obj object name
    @return a tuple of (partition, list of node dicts)

    Each node dict will have at least the following keys:

    ======  ===============================================================
    id      unique integer identifier amongst devices
    index   offset into the primary node list for the partition
    weight  a float of the relative weight of this device as compared to
            others; this indicates how many partitions the builder will try
            to assign to this device
    zone    integer indicating which zone the device is in; a given
            partition will not be assigned to multiple devices within the
            same zone
    ip      the ip address of the device
    port    the tcp port of the device
    device  the device's name on disk (sdb1, for example)
    meta    general use 'extra' field; for example: the online date, the
            hardware description
    ======  ===============================================================
    */
    public void get_nodes(String account,
                          String container=null,
                          String obj=null) {
        int part = this.get_part(account, container, obj);
        return part, this._get_part_nodes(part);
    }

    /**
    Generator to get extra nodes for a partition for hinted handoff.

    The handoff nodes will try to be in zones other than the
    primary zones, will take into account the device weights, and
    will usually keep the same sequences of handoffs even with
    ring changes.

    @param part partition to get handoff nodes for
    @return generator of node dicts

    See :func:`get_nodes` for a description of the node dicts.
    */
    public void get_more_nodes(int part) {
        if (time() > this._rtime) {
            this._reload();
        }
        primary_nodes = this._get_part_nodes(part);

        SortedSet<Integer> used = new TreeSet<>();
        used = set(d["id"] for d in primary_nodes);
        same_regions = set(d["region"] for d in primary_nodes);
        same_zones = set((d["region"], d["zone"]) for d in primary_nodes);
        same_ips = set(
            (d["region"], d["zone"], d["ip"]) for d in primary_nodes);

        int parts = len(this._replica2part2dev_id[0]);
        start = struct.unpack_from(
            ">I", md5(str(part)).digest())[0] >> this._part_shift;
        inc = int(parts / 65536) or 1;
        // Multiple loops for execution speed; the checks and bookkeeping get
        // simpler as you go along
        boolean hit_all_regions = len(same_regions) == this._num_regions;
        for (handoff_part in chain(range(start, parts, inc),
                                  range(inc - ((parts - start) % inc),
                                        start, inc))) {
            if (hit_all_regions) {
                // At this point, there are no regions left untouched, so we
                // can stop looking.
                break;
            }

            for (part2dev_id in this._replica2part2dev_id) {
                if (handoff_part < len(part2dev_id)) {
                    dev_id = part2dev_id[handoff_part];
                    dev = this._devs[dev_id];
                    region = dev["region"];
                    if (dev_id not in used && region not in same_regions) {
                        yield dev;
                        used.add(dev_id);
                        same_regions.add(region);
                        zone = dev["zone"];
                        ip = (region, zone, dev["ip"]);
                        same_zones.add((region, zone));
                        same_ips.add(ip);
                        if (len(same_regions) == this._num_regions) {
                            hit_all_regions = true;
                            break;
                        }
                    }
                }
            }
        }

        boolean hit_all_zones = len(same_zones) == this._num_zones;
        for (handoff_part in chain(range(start, parts, inc),
                                  range(inc - ((parts - start) % inc),
                                        start, inc))) {
            if (hit_all_zones) {
                // Much like we stopped looking for fresh regions before, we
                // can now stop looking for fresh zones; there are no more.
                break;
            }

            for (part2dev_id in this._replica2part2dev_id) {
                if (handoff_part < len(part2dev_id)) {
                    dev_id = part2dev_id[handoff_part];
                    dev = this._devs[dev_id];
                    zone = (dev["region"], dev["zone"]);
                    if (dev_id not in used && zone not in same_zones) {
                        yield dev;
                        used.add(dev_id);
                        same_zones.add(zone);
                        ip = zone + (dev["ip"],);
                        same_ips.add(ip);
                        if (len(same_zones) == this._num_zones) {
                            hit_all_zones = true;
                            break;
                        }
                    }
                }
            }
        }

        boolean hit_all_ips = len(same_ips) == this._num_ips;
        for (handoff_part in chain(range(start, parts, inc),
                                  range(inc - ((parts - start) % inc),
                                        start, inc))) {
            if (hit_all_ips) {
                // We've exhausted the pool of unused backends, so stop
                // looking.
                break;
            }

            for (part2dev_id in this._replica2part2dev_id) {
                if (handoff_part < len(part2dev_id)) {
                    dev_id = part2dev_id[handoff_part];
                    dev = this._devs[dev_id];
                    ip = (dev["region"], dev["zone"], dev["ip"]);
                    if (dev_id not in used && ip not in same_ips) {
                        yield dev;
                        used.add(dev_id);
                        same_ips.add(ip);
                        if (len(same_ips) == this._num_ips) {
                            hit_all_ips = true;
                            break;
                        }
                    }
                }
            }
        }

        boolean hit_all_devs = used.size() == this._num_devs;
        for (handoff_part in chain(range(start, parts, inc),
                                  range(inc - ((parts - start) % inc),
                                        start, inc))) {
            if (hit_all_devs) {
                // We've used every device we have, so let's stop looking for
                // unused devices now.
                break;
            }

            for (part2dev_id in this._replica2part2dev_id) {
                if (handoff_part < len(part2dev_id)) {
                    int dev_id = part2dev_id[handoff_part];
                    if (!used.contains(dev_id)) {
                        yield this._devs[dev_id];
                        used.add(dev_id);
                        if (used.size() == this._num_devs) {
                            hit_all_devs = true;
                            break;
                        }
                    }
                }
            }
        }
    }
}
