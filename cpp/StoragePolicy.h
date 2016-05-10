#ifndef STORAGEPOLICY_H
#define STORAGEPOLICY_H

#include <string>
#include <vector>
#include <algorithm>

#include "PolicyError.h"
#include "Ring.h"
#include "StoragePolicyCollection.h"
#include "StrUtils.h"


class StoragePolicy {

private:
    // disallow copies
    StoragePolicy(const StoragePolicy&);
    StoragePolicy& operator=(const StoragePolicy&);


public:
    static const std::string LEGACY_POLICY_NAME;
    static const std::string UPPER_LEGACY_POLICY_NAME;

    std::string _class_name;
    int _idx;
    bool _is_default;
    bool _is_deprecated;
    std::string _ring_name;
    std::string _policy_type;
    std::vector<std::string> _alias_list;
    Ring* _object_ring;


    StoragePolicy(int idx, const std::string& name) :
        _idx(idx),
        _is_default(false),
        _is_deprecated(false),
        _object_ring(NULL) {
        _alias_list.push_back(name);

        if (idx < 0) {
            throw PolicyError("Invalid index", idx);
        }
    }

    StoragePolicy(const std::string& class_name,
                  int idx,
                  const std::string& name) :
        _class_name(class_name),
        _idx(idx),
        _is_default(false),
        _is_deprecated(false),
        _object_ring(NULL) {
        _alias_list.push_back(name);

        if (idx < 0) {
            throw PolicyError("Invalid index", idx);
        }
    }

    virtual ~StoragePolicy() {}

    bool operator==(const StoragePolicy& compare) const {
        return (this->name() == compare.name());
    }

    bool operator!=(const StoragePolicy& compare) const {
        return (this->name() != compare.name());
    }

    /**
     * Hook, called with the ring is loaded. Can be used to
     * validate the ring against the StoragePolicy configuration.
     */
    virtual void _validate_ring() {
        //pass
    }

    //PJD: not sure why the bool return type since we raise exception
    // on error
    bool _validate_policy_name(const std::string& name) const;

    bool is_default() const {
        return _is_default;
    }

    void make_default() {
        _is_default = true;
    }

    const std::string& name() const {
        return _alias_list[0];
    }

    void set_name(const std::string& name) {
        _validate_policy_name(name);
        _alias_list[0] = name;
    }

    void add_name(const std::string& name) {
        if (_validate_policy_name(name)) {
            _alias_list.push_back(name);
        }
    }

    void remove_name(const std::string& name) {
        std::vector<std::string>::iterator itAlias =
            std::find(this->_alias_list.begin(),
                      this->_alias_list.end(),
                      name);

        if (this->_alias_list.end() == itAlias) {
            throw PolicyError(name +
                              " is not a name assigned to policy " +
                              StrUtils::toString(this->_idx));
        }

        if (this->_alias_list.size() == 1) {
            throw PolicyError(std::string("Cannot remove only name ") +
                              name +
                              " from policy " +
                              StrUtils::toString(this->_idx) +
                              ". Policies must have at least one name.");
        } else {
            this->_alias_list.erase(itAlias);
        }
    }

    void change_primary_name(const std::string& name) {
        if (name == this->name()) {
            return;
        } else {
            if (std::find(_alias_list.begin(), _alias_list.end(), name) !=
                _alias_list.end()) {
                this->remove_name(name);
            } else {
                this->_validate_policy_name(name);
            }
        }

        this->_alias_list.insert(_alias_list.begin(), name);
    }

    void load_ring(const std::string& swift_dir) {
        if (NULL != _object_ring) {
            return;
        }

        _object_ring = new Ring(swift_dir, _ring_name);

        // Validate ring to make sure it conforms to policy requirements
        _validate_ring();
    }

    std::string toString() const {
        std::string s(this->_class_name);
        s += "(";
        s += StrUtils::toString(this->_idx);
        s += ", ";
        s += this->alias_list_to_string();
        s += ", is_default=";
        s += StrUtils::toString(this->_is_default);
        s += ", is_deprecated=";
        s += StrUtils::toString(this->_is_deprecated);
        s += ", policy_type=";
        s += this->_policy_type;
        s += ")"; 
        return s;
    }

    std::string alias_list_to_string() const {
        //TODO: implement alias_list_to_string
        return "";
    }

    static int extract_policy(const std::string& path) {
        //TODO: implement extract_policy
        return -1;
    }

    int intValue() const {
        return this->_idx;
    }
};


#endif

