#include "StoragePolicyCollection.h"
#include "StoragePolicy.h"
#include "PolicyError.h"
#include "StrUtils.h"


using namespace std;


StoragePolicyCollection::StoragePolicyCollection(const vector<StoragePolicy*>& pols) {
    this->_validate_policies(pols);
}

void StoragePolicyCollection::_add_policy(StoragePolicy* policy) {
    const vector<string>::const_iterator itAliasEnd =
        policy->_alias_list.end();
    vector<string>::const_iterator itAlias = policy->_alias_list.begin();

    for (; itAlias != itAliasEnd; ++itAlias) {
        const string& name = *itAlias;
        this->by_name[StrUtils::upper(name)] = policy;
    }
    this->by_index[policy->intValue()] = policy;
}

int StoragePolicyCollection::length() const {
    return this->by_index.size();
}

/*
    def __repr__(self):
        return (textwrap.dedent("""
    StoragePolicyCollection([
        %s
    ])
    """) % ',\n    '.join(repr(p) for p in self)).strip()

    def __getitem__(self, key):
        return self.by_index[key]

    def __iter__(self):
        return iter(self.by_index.values())
*/

void StoragePolicyCollection::_validate_policies(const vector<StoragePolicy*>& policies) {

    const vector<StoragePolicy*>::const_iterator itPoliciesEnd = policies.end();
    vector<StoragePolicy*>::const_iterator itPolicies = policies.begin();

    for (; itPolicies != itPoliciesEnd; ++itPolicies) {
        StoragePolicy* policy = *itPolicies;
        if (this->by_index.find(policy->intValue()) !=
            this->by_index.end()) {
            throw PolicyError(string("Duplicate index ") +
                              policy->toString() +
                              " conflicts with " +
                              this->get_by_index(policy->intValue())->toString());
        }

        const vector<string>::const_iterator itAliasEnd =
            policy->_alias_list.end();
        vector<string>::iterator itAlias = policy->_alias_list.begin();

        for (; itAlias != itAliasEnd; ++itAlias) {
            const string& name = *itAlias;
            const string upper_name = StrUtils::upper(name);
            
            if (this->by_name.find(upper_name) != this->by_name.end()) {
                throw PolicyError(string("Duplicate name ") +
                    policy->toString() +
                    " conflicts with " +
                    this->get_by_name(name)->toString());
            }
        }

        if (policy->_is_default) {
            if (NULL == this->default_policy) {
                this->default_policy = policy;
            } else {
                throw PolicyError(
                    string("Duplicate default " +
                           policy->toString() +
                           " conflicts with " +
                           this->default_policy->toString()));
            }
        }

        this->_add_policy(policy);
    }

    // If a 0 policy wasn't explicitly given, or nothing was
    // provided, create the 0 policy now
    if (this->by_index.find(0) == this->by_index.end()) {
        if (this->length() != 0) {
            throw PolicyError("You must specify a storage policy "
                              "section for policy index 0 in order "
                              "to define multiple policies");
        }
        this->_add_policy(new StoragePolicy(0,
                              StoragePolicy::LEGACY_POLICY_NAME));
    }

    // at least one policy must be enabled
    bool enabled_policies = false;
    for (int i = 0; i < by_index.size(); ++i) {
        StoragePolicy* p = by_index[i];
        if (!p->_is_deprecated) {
            enabled_policies = true;
            break;
        }
    }

    if (!enabled_policies) {
        throw PolicyError("Unable to find policy that's not deprecated!");
    }

    // if needed, specify default
    if (NULL == this->default_policy) {
        if (this->length() > 1) {
            throw PolicyError("Unable to find default policy");
        }
        this->default_policy = this->get_by_index(0);
        this->default_policy->_is_default = true;
    }
}

StoragePolicy* StoragePolicyCollection::get_by_name(const string& name) {
    /**
    Find a storage policy by its name.
    :param name: name of the policy
    :returns: storage policy, or None
    */
    return this->by_name[StrUtils::upper(name)];
}

StoragePolicy* StoragePolicyCollection::get_by_index(int index) {
    /**
    Find a storage policy by its index.
    An index of None will be treated as 0.
    :param index: numeric index of the storage policy
    :returns: storage policy, or None if no such policy
    */
    // makes it easier for callers to just pass in a header value
    if (index < 0) {
        index = 0;
    }
    return this->by_index[index];
}

StoragePolicy* StoragePolicyCollection::legacy() {
    return this->get_by_index(-1);
}

Ring* StoragePolicyCollection::get_object_ring(int policy_idx,
                                               const string& swift_dir) {
    /**
    Get the ring object to use to handle a request based on its policy.
    An index of None will be treated as 0.
    :param policy_idx: policy index as defined in swift.conf
    :param swift_dir: swift_dir used by the caller
    :returns: appropriate ring object
    */
    StoragePolicy* policy = this->get_by_index(policy_idx);
    if (NULL == policy) {
        throw PolicyError(string("No policy with index ") +
                          StrUtils::toString(policy_idx));
    }

    if (NULL == policy->_object_ring) {
        policy->load_ring(swift_dir);
    }

    return policy->_object_ring;
}

/*
    def get_policy_info(self):
        """
        Build info about policies for the /info endpoint
        :returns: list of dicts containing relevant policy information
        """
        policy_info = []
        for pol in self:
            # delete from /info if deprecated
            if pol.is_deprecated:
                continue
            policy_entry = pol.get_info()
            policy_info.append(policy_entry)
        return policy_info
*/

/**
 Adds a new name or names to a policy
 @param policy_index index of a policy in this policy collection.
 @param aliases arbitrary number of string policy names to add.
*/
void StoragePolicyCollection::add_policy_alias(int policy_index,
                                               const vector<string>& aliases) {
    StoragePolicy* policy = this->get_by_index(policy_index);

    for (int i = 0; i < aliases.size(); ++i ) {
        const string& alias = aliases[i];
        const string alias_upper = StrUtils::upper(alias);
        if (by_name.end() != by_name.find(alias_upper)) {
            throw PolicyError(string("Duplicate name ") + alias +
                              " in use by policy " +
                              this->get_by_name(alias)->toString());
        }

        policy->add_name(alias);
        this->by_name[alias_upper] = policy;
    }
}

void StoragePolicyCollection::remove_policy_alias(const vector<string>& aliases) {
    const vector<string>::const_iterator itAliasEnd = aliases.end();
    vector<string>::const_iterator itAlias = aliases.begin();

    for (; itAlias != itAliasEnd; ++itAlias) {
        const string& alias = *itAlias;
        StoragePolicy* policy = this->get_by_name(alias);
        if (NULL == policy) {
            throw PolicyError(string("No policy with name ") +
                              alias + " exists.");
        }
        if (policy->_alias_list.size() == 1) {
            throw PolicyError(string("Policy ") +
                              policy->toString() +
                              " with name " +
                              alias +
                              " has only one name. " +
                              "Policies must have at least one name.");
        } else {
            policy->remove_name(alias);
            const string upper_alias = StrUtils::upper(alias);
            this->by_name.erase(this->by_name.find(upper_alias));
        }
    }
}

void StoragePolicyCollection::change_policy_primary_name(int policy_index,
                                                         const string& new_name) {
    StoragePolicy* policy = this->get_by_index(policy_index);
    StoragePolicy* name_taken = this->get_by_name(new_name);
    // if the name belongs to some other policy in the collection
    if (name_taken != NULL && *name_taken != *policy) {
        throw PolicyError(string("Other policy ") +
                          StrUtils::toString(name_taken->_idx) +
                          " with name " +
                          new_name +
                          " exists.");
    } else {
        policy->change_primary_name(new_name);
        this->by_name[StrUtils::upper(new_name)] = policy;
    }
}

