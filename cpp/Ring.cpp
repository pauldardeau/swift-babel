#include "Ring.h"
#include "RingData.h"
#include "OSUtils.h"


using namespace std;


void Ring::_reload() {
}

void Ring::_reload(bool force) {
    this->_rtime = time() + this->reload_time;
    if (force or this->has_changed()) {
        RingData ring_data = RingData::load(this->serialized_path);
        this->_mtime = getmtime(this->serialized_path);
        this->_devs = ring_data.devs;
        // NOTE(akscram): Replication parameters like replication_ip
        //                and replication_port are required for
        //                replication process. An old replication
        //                ring doesn't contain this parameters into
        //                device. Old-style pickled rings won't have
        //                region information.
        for (dev in this->_devs) {
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

        this->_replica2part2dev_id = ring_data._replica2part2dev_id;
        this->_part_shift = ring_data._part_shift;
        this->_rebuild_tier_data();

        // Do this now, when we know the data has changed, rather than
        // doing it on every call to get_more_nodes().
        //
        // Since this is to speed up the finding of handoffs, we only
        // consider devices with at least one partition assigned. This
        // way, a region, zone, or server with no partitions assigned
        // does not count toward our totals, thereby keeping the early
        // bailouts in get_more_nodes() working.
        dev_ids_with_parts = set();
        for (part2dev_id in this->_replica2part2dev_id) {
            for (dev_id in part2dev_id) {
                dev_ids_with_parts.add(dev_id);
            }
        }

        regions = set();
        zones = set();
        ips = set();
        this->_num_devs = 0;
        for (dev in this->_devs) {
            if (dev and dev["id"] in dev_ids_with_parts) {
                regions.add(dev["region"]);
                zones.add((dev["region"], dev["zone"]));
                ips.add((dev["region"], dev["zone"], dev["ip"]));
                this->_num_devs += 1;
            }
        }
        this->_num_regions = len(regions);
        this->_num_zones = len(zones);
        this->_num_ips = len(ips);
    }
}

void Ring::_rebuild_tier_data() {
    this->tier2devs = defaultdict(list);
    for (dev in this->_devs) {
        if (not dev) {
            continue;
        }
        for (tier in tiers_for_dev(dev)) {
            this->tier2devs[tier].append(dev);
        }
    }

    tiers_by_length = defaultdict(list);
    for (tier in this->tier2devs) {
        tiers_by_length[len(tier)].append(tier);
    }
    this->tiers_by_length = sorted(tiers_by_length.values(),
                                  key=lambda x: len(x[0]));
    for (tiers in this->tiers_by_length) {
        tiers.sort();
    }
}

Ring::Ring(const string& serialized_path) {
}

Ring::Ring(const string& serialized_path,
           int reload_time) {
}

Ring::Ring(const string& serialized_path,
           const string& ring_name,
           int reload_time) {
    // can't use the ring unless HASH_PATH_SUFFIX is set
    validate_configuration();
    if (ring_name) {
        this->serialized_path = OSUtils::path_join(serialized_path,
                                            ring_name + ".ring.gz");
    } else {
        this->serialized_path = OSUtils::path_join(serialized_path);
    }
    this->reload_time = reload_time;
    this->_reload(force=true);
}

int Ring::replica_count() {
    return len(this->_replica2part2dev_id);
}

int Ring::partition_count() {
    return len(this->_replica2part2dev_id[0]);
}

void Ring::devs() {
    if (time() > this->_rtime) {
        this->_reload();
    }
    return this->_devs;
}

bool Ring::has_changed() {
    return getmtime(this->serialized_path) != this->_mtime;
}

void Ring::_get_part_nodes(int part) {
    part_nodes = [];
    seen_ids = set();
    for (r2p2d in this->_replica2part2dev_id) {
        if (part < len(r2p2d)) {
            dev_id = r2p2d[part];
            if (dev_id not in seen_ids) {
                part_nodes.append(this->devs[dev_id]);
                seen_ids.add(dev_id);
            }
        }
    }
    return [dict(node, index=i) for i, node in enumerate(part_nodes)];
}

int Ring::get_part(const string& account,
                   const string& container,
                   const string& obj) {
    key = SwiftUtils::hash_path(account, container, obj, true); // raw_digest
    if (time() > this->_rtime) {
        this->_reload();
    }
    part = struct.unpack_from(">I", key)[0] >> this->_part_shift;
    return part;
}

void Ring::get_part_nodes(int part) {
    if (time() > this->_rtime) {
        this->_reload();
    }
    return this->_get_part_nodes(part);
}

void Ring::get_nodes(const string& account,
                     const string& container,
                     const string& obj) {
    part = this->get_part(account, container, obj);
    return part, this->_get_part_nodes(part);
}

void Ring::get_more_nodes(int part) {
    if (time() > this->_rtime) {
        this->_reload();
    }

    primary_nodes = this->_get_part_nodes(part);

    used = set(d["id"] for d in primary_nodes);
    same_regions = set(d["region"] for d in primary_nodes);
    same_zones = set((d["region"], d["zone"]) for d in primary_nodes);
    same_ips = set(
        (d["region"], d["zone"], d["ip"]) for d in primary_nodes);

    int parts = len(this->_replica2part2dev_id[0]);
    start = struct.unpack_from(
        ">I", md5(str(part)).digest())[0] >> this->_part_shift;
    int inc = max(parts / 65536, 1);
    // Multiple loops for execution speed; the checks and bookkeeping get
    // simpler as you go along
    bool hit_all_regions = (len(same_regions) == this->_num_regions);
    for (handoff_part in chain(range(start, parts, inc),
                              range(inc - ((parts - start) % inc),
                                    start, inc))) {
        if (hit_all_regions) {
            // At this point, there are no regions left untouched, so we
            // can stop looking.
            break;
        }

        for (part2dev_id in this->_replica2part2dev_id) {
            if (handoff_part < len(part2dev_id)) {
                dev_id = part2dev_id[handoff_part];
                dev = this->_devs[dev_id];
                region = dev["region"];
                if (dev_id not in used and region not in same_regions) {
                    yield dev;
                    used.add(dev_id);
                    same_regions.add(region);
                    zone = dev["zone"];
                    ip = (region, zone, dev["ip"]);
                    same_zones.add((region, zone));
                    same_ips.add(ip);
                    if (len(same_regions) == this->_num_regions) {
                        hit_all_regions = true;
                        break;
                    }
                }
            }
        }
    }

    bool hit_all_zones = len(same_zones) == this->_num_zones;
    for (handoff_part in chain(range(start, parts, inc),
                              range(inc - ((parts - start) % inc),
                                    start, inc))) {
        if (hit_all_zones) {
            // Much like we stopped looking for fresh regions before, we
            // can now stop looking for fresh zones; there are no more.
            break;
        }

        for (part2dev_id in this->_replica2part2dev_id) {
            if (handoff_part < len(part2dev_id)) {
                dev_id = part2dev_id[handoff_part];
                dev = this->_devs[dev_id];
                zone = (dev["region"], dev["zone"]);
                if (dev_id not in used and zone not in same_zones) {
                    yield dev;
                    used.add(dev_id);
                    same_zones.add(zone);
                    ip = zone + (dev["ip"],);
                    same_ips.add(ip);
                    if (len(same_zones) == this->_num_zones) {
                        hit_all_zones = true;
                        break;
                    }
                }
            }
        }
    }

    bool hit_all_ips = len(same_ips) == this->_num_ips;
    for (handoff_part in chain(range(start, parts, inc),
                              range(inc - ((parts - start) % inc),
                                    start, inc))) {
        if (hit_all_ips) {
            // We've exhausted the pool of unused backends, so stop
            // looking.
            break;
        }
        for (part2dev_id in this->_replica2part2dev_id) {
            if (handoff_part < len(part2dev_id)) {
                dev_id = part2dev_id[handoff_part];
                dev = this->_devs[dev_id];
                ip = (dev["region"], dev["zone"], dev["ip"]);
                if (dev_id not in used and ip not in same_ips) {
                    yield dev;
                    used.add(dev_id);
                    same_ips.add(ip);
                    if (len(same_ips) == this->_num_ips) {
                        hit_all_ips = true;
                        break;
                    }
                }
            }
        }
    }

    bool hit_all_devs = len(used) == this->_num_devs;
    for (handoff_part in chain(range(start, parts, inc),
                              range(inc - ((parts - start) % inc),
                                    start, inc))) {
        if (hit_all_devs) {
            // We've used every device we have, so let's stop looking for
            // unused devices now.
            break;
        }

        for (part2dev_id in this->_replica2part2dev_id) {
            if (handoff_part < len(part2dev_id)) {
                dev_id = part2dev_id[handoff_part];
                if (dev_id not in used) {
                    yield this->_devs[dev_id];
                    used.add(dev_id);
                    if (len(used) == this->_num_devs) {
                        hit_all_devs = true;
                        break;
                    }
                }
            }
        }
    }
}


