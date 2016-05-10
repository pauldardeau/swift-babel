#ifndef STORAGEPOLICYCOLLECTION_H
#define STORAGEPOLICYCOLLECTION_H


#include <string>
#include <map>
#include <vector>

class Ring;
class StoragePolicy;


class StoragePolicyCollection {

private:
    StoragePolicy* default_policy;
    std::map<std::string,StoragePolicy*> by_name;
    std::map<int,StoragePolicy*> by_index;


public:
    static std::string LEGACY_POLICY_NAME;


    StoragePolicyCollection(const std::vector<StoragePolicy*>& pols);

    void _add_policy(StoragePolicy* policy);

    int length() const;

    StoragePolicy* getitem(int key);

    void iter();

    void _validate_policies(const std::vector<StoragePolicy*>& policies);

    StoragePolicy* get_by_name(const std::string& name);

    StoragePolicy* get_by_index(int index);

    StoragePolicy* legacy();

    Ring* get_object_ring(int policy_idx,
                         const std::string& swift_dir);

    void add_policy_alias(int policy_index,
                          const std::vector<std::string>& aliases);

    void remove_policy_alias(const std::vector<std::string>& aliases);

    void change_policy_primary_name(int policy_index,
                                    const std::string& new_name);
};

#endif

