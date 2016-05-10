import PolicyError;
import Ring;
import BaseStoragePolicy;


class StoragePolicyCollection {
    /**
    This class represents the collection of valid storage policies for the
    cluster and is instantiated as :class:`StoragePolicy` objects are added to
    the collection when ``swift.conf`` is parsed by
    :func:`parse_storage_policies`.

    When a StoragePolicyCollection is created, the following validation
    is enforced:

    * If a policy with index 0 is not declared and no other policies defined,
      Swift will create one
    * The policy index must be a non-negative integer
    * If no policy is declared as the default and no other policies are
      defined, the policy with index 0 is set as the default
    * Policy indexes must be unique
    * Policy names are required
    * Policy names are case insensitive
    * Policy names must contain only letters, digits or a dash
    * Policy names must be unique
    * The policy name 'Policy-0' can only be used for the policy with index 0
    * If any policies are defined, exactly one policy must be declared default
    * Deprecated policies can not be declared the default

    */

private:
    BaseStoragePolicy default_policy;
    BaseStoragePolicy[string] by_name;
    BaseStoragePolicy[int] by_index;


public:
    static string LEGACY_POLICY_NAME = "Policy-0";

    this(BaseStoragePolicy[] pols) {
        this.default_policy = null;
        this.by_name = {};
        this.by_index = {};
        this._validate_policies(pols);
    }

    void _add_policy(BaseStoragePolicy policy) {
        /**
        Add pre-validated policies to internal indexes.
        */
        foreach (name, policy.alias_list) {
            this.by_name[name.upper()] = policy;
        }
        this.by_index[to!int(policy)] = policy;
    }

    /**
    override string toString() {
        return (textwrap.dedent("""
    StoragePolicyCollection([
        %s
    ])
    """) % ',\n    '.join(p.toString() for p in this)).strip();
    }
    */

    int length() {
        return len(this.by_index);
    }

    BaseStoragePolicy getitem(int key) {
        return this.by_index[key];
    }

    void iter() {
        return iter(this.by_index.values());
    }

    void _validate_policies(BaseStoragePolicy[] policies) {
        /**
        :param policies: list of policies
        */

        foreach (policy, policies) {
            if (to!int(policy) in this.by_index) {
                throw new PolicyError("Duplicate index %s conflicts with %s" % (
                    policy, this.get_by_index(to!int(policy))));
            }
            foreach (name, policy.alias_list) {
                if (name.upper() in this.by_name) {
                    throw new PolicyError("Duplicate name %s conflicts with %s" % (
                        policy, this.get_by_name(name)));
                }
            }
            if (policy.is_default) {
                if (null == this.default_policy) {
                    this.default_policy = policy;
                } else {
                    throw new PolicyError(
                        "Duplicate default %s conflicts with %s" % (
                            policy, this.default_policy));
                }
            }
            this._add_policy(policy);
        }

        // If a 0 policy wasn't explicitly given, or nothing was
        // provided, create the 0 policy now
        if (0 not in this.by_index) {
            if (this.length() != 0) {
                throw new PolicyError("You must specify a storage policy "
                                  "section for policy index 0 in order "
                                  "to define multiple policies");
            }
            this._add_policy(new StoragePolicy(0, name=LEGACY_POLICY_NAME));
        }

        // at least one policy must be enabled
        enabled_policies = [p for p in this if not p.is_deprecated]
        if (!enabled_policies) {
            throw new PolicyError("Unable to find policy that's not deprecated!");
        }

        // if needed, specify default
        if (null == this.default_policy) {
            if (this.length() > 1) {
                throw new PolicyError("Unable to find default policy");
            }
            this.default_policy = this.getitem(0);
            this.default.is_default = true;
        }
    }

    BaseStoragePolicy get_by_name(string name) {
        /**
        Find a storage policy by its name.

        :param name: name of the policy
        :returns: storage policy, or None
        */
        return this.by_name.get(name.upper());
    }

    BaseStoragePolicy get_by_index(int index) {
        /**
        Find a storage policy by its index.

        An index of None will be treated as 0.

        :param index: numeric index of the storage policy
        :returns: storage policy, or None if no such policy
        */
        // makes it easier for callers to just pass in a header value
        if (index in ('', None)) {
            index = 0;
        } else {
            try {
                index = to!int(index);
            } catch (ValueError e) {
                return None;
            }
        }
        return this.by_index.get(index);
    }

    @property
    void legacy():
        return this.get_by_index(None);

    Ring get_object_ring(int policy_idx, string swift_dir) {
        /**
        Get the ring object to use to handle a request based on its policy.

        An index of None will be treated as 0.

        :param policy_idx: policy index as defined in swift.conf
        :param swift_dir: swift_dir used by the caller
        :returns: appropriate ring object
        */
        BaseStoragePolicy policy = this.get_by_index(policy_idx);
        if (null == policy) {
            throw new PolicyError("No policy with index %s" % policy_idx);
        }
        if (null == policy.object_ring) {
            policy.load_ring(swift_dir);
        }
        return policy.object_ring;
    }

    /**
    Build info about policies for the /info endpoint

    :returns: list of dicts containing relevant policy information
    */
/*
    void get_policy_info() {
        policy_info = [];
        foreach (pol, this) {
            // delete from /info if deprecated
            if (pol.is_deprecated) {
                continue;
            }
            policy_entry = pol.get_info();
            policy_info.append(policy_entry);
        }
        return policy_info;
    }
*/
    void add_policy_alias(int policy_index, string[] aliases) {
        /**
        Adds a new name or names to a policy

        :param policy_index: index of a policy in this policy collection.
        :param aliases: arbitrary number of string policy names to add.
        */
        BaseStoragePolicy policy = this.get_by_index(policy_index);
        foreach (policy_alias, aliases) {
            if (policy_alias.upper() in this.by_name) {
                throw new PolicyError("Duplicate name %s in use "
                                  "by policy %s" % (policy_alias,
                                                    this.get_by_name(policy_alias)));
            } else {
                policy.add_name(policy_alias);
                this.by_name[policy_alias.upper()] = policy;
            }
        }
    }

    void remove_policy_alias(string[] aliases) {
        /**
        Removes a name or names from a policy. If the name removed is the
        primary name then the next available alias will be adopted
        as the new primary name.

        :param aliases: arbitrary number of existing policy names to remove.
        */
        foreach (policy_alias, aliases) {
            BaseStoragePolicy policy = this.get_by_name(policy_alias);
            if (null == policy) {
                throw new PolicyError("No policy with name %s exists." % alias)
            }
            if (len(policy.alias_list) == 1) {
                throw new PolicyError("Policy %s with name %s has only one name. "
                                  "Policies must have at least one name." % (
                                      policy, alias))
            } else {
                policy.remove_name(alias);
                del this.by_name[alias.upper()];
            }
        }
    }

    void change_policy_primary_name(int policy_index, string new_name) {
        /**
        Changes the primary or default name of a policy. The new primary
        name can be an alias that already belongs to the policy or a
        completely new name.

        :param policy_index: index of a policy in this policy collection.
        :param new_name: a string name to set as the new default name.
        */
        BaseStoragePolicy policy = this.get_by_index(policy_index);
        string name_taken = this.get_by_name(new_name);
        // if the name belongs to some other policy in the collection
        if (name_taken != null && name_taken != policy) {
            throw new PolicyError("Other policy %s with name %s exists." %
                              (this.get_by_name(new_name).idx, new_name));
        } else {
            policy.change_primary_name(new_name);
            this.by_name[new_name.upper()] = policy;
        }
    }
}
