#ifndef RING_H
#define RING_H


#include <string>

#include "RingStructure.h"


class Ring {

private:
    std::string serialized_path;
    int reload_time;
    int _rtime;
    int _mtime;
    //Object _devs;
    int _num_regions;
    int _num_zones;
    int _num_devs;
    int _num_ips;
    RingStructure ring_structure;
    //int[][] _replica2part2dev_id;
    //int _part_shift;
    //Object tier2devs;
    //Object tiers_by_length;


protected:
    void _reload();
    void _reload(bool force);
    void _rebuild_tier_data();


public:
    Ring(const std::string& serialized_path);
    Ring(const std::string& serialized_path,
         int reload_time);
    Ring(const std::string& serialized_path,
         const std::string& ring_name,
         int reload_time=-1); //TODO: correct default value?

    int replica_count();
    int partition_count();
    void devs();
    bool has_changed();
    void _get_part_nodes(int part);
    int get_part(const std::string& account,
                 const std::string& container,
                 const std::string& obj);
    void get_part_nodes(int part);
    void get_nodes(const std::string& account,
                   const std::string& container,
                   const std::string& obj);

    void get_more_nodes(int part);

};


#endif

