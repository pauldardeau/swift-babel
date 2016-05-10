import java.util.HashMap;
import java.util.List;


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
class StoragePolicyCollection {

    private BaseStoragePolicy default_policy;
    private HashMap<String,BaseStoragePolicy> by_name;
    private HashMap<Integer,BaseStoragePolicy> by_index;


    public static String LEGACY_POLICY_NAME = "Policy-0";

    def StoragePolicyCollection(pols: BaseStoragePolicy[]) {
        this.default_policy = null;
        this.by_name = new HashMap<>();
        this.by_index = new HashMap<>();
        this._validate_policies(pols);
    }

    /**
    Add pre-validated policies to internal indexes.
    */
    def _add_policy(policy: BaseStoragePolicy) {
        for (String name : policy.alias_list) {
            this.by_name[name.toUpperCase()] = policy;
        }
        this.by_index[policy.toInt()] = policy;
    }

    /**
    override String toString() {
        return (textwrap.dedent("""
    StoragePolicyCollection([
        %s
    ])
    """) % ',\n    '.join(p.toString() for p in this)).strip();
    }
    */

    def length() : Int = {
        return this.by_index.size();
    }

    def getitem(key: Int) : BaseStoragePolicy = {
        return this.by_index[key];
    }

    def iter() {
        return iter(this.by_index.values());
    }

    /**
    @param policies list of policies
    */
    def _validate_policies(policies: BaseStoragePolicy[]) {

        for (BaseStoragePolicy policy : policies) {
            if (policy.toInt() in this.by_index) {
                throw new PolicyError("Duplicate index " +
                                      policy +
                                      " conflicts with " +
                                      this.get_by_index(policy.toInt()));
            }

            for (String name : policy.alias_list) {
                if (this.by_name.containsKey(name.toUpperCase())) {
                    throw new PolicyError("Duplicate name " +
                                          policy +
                                          " conflicts with " +
                                          this.get_by_name(name));
                }
            }

            if (policy.is_default) {
                if (null == this.default_policy) {
                    this.default_policy = policy;
                } else {
                    throw new PolicyError(
                        "Duplicate default " +
                        policy +
                        " conflicts with " +
                        this.default_policy);
                }
            }
            this._add_policy(policy);
        }

        // If a 0 policy wasn't explicitly given, or nothing was
        // provided, create the 0 policy now
        if (0 not in this.by_index) {
            if (this.length() != 0) {
                throw new PolicyError("You must specify a storage policy " +
                                  "section for policy index 0 in order " +
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
            this.default_policy.is_default = true;
        }
    }

    /**
    Find a storage policy by its name.

    @param name name of the policy
    @return storage policy, or null
    */
    def get_by_name(name: String) : BaseStoragePolicy = {
        return this.by_name.get(name.toUpperCase());
    }

    /**
    Find a storage policy by its index.

    An index of null will be treated as 0.

    @param index numeric index of the storage policy
    @return storage policy, or null if no such policy
    */
    def get_by_index(index: Int) : BaseStoragePolicy = {
        // makes it easier for callers to just pass in a header value
        if (index in ('', null)) {
            index = 0;
        } else {
            try {
                index = Integer.parseInt(index);
            } catch (ValueError e) {
                return null;
            }
        }
        return this.by_index.get(index);
    }

    @property
    def legacy() : BaseStoragePolicy = {
        return this.get_by_index(null);
    }

    /**
    Get the ring object to use to handle a request based on its policy.

    An index of null will be treated as 0.

    @param policy_idx policy index as defined in swift.conf
    @param swift_dir swift_dir used by the caller
    @return appropriate ring object
    */
    def get_object_ring(policy_idx: Int, swift_dir: String) : Ring = {
        val policy = this.get_by_index(policy_idx);
        if (null == policy) {
            throw new PolicyError("No policy with index " + policy_idx);
        }
        if (null == policy.object_ring) {
            policy.load_ring(swift_dir);
        }
        return policy.object_ring;
    }

    /**
    Build info about policies for the /info endpoint

    @return list of dicts containing relevant policy information
    */
/*
    def get_policy_info() {
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

    /**
    Adds a new name or names to a policy

    @param policy_index index of a policy in this policy collection.
    @param aliases arbitrary number of string policy names to add.
    */
    def add_policy_alias(policy_index: Int, aliases: String[]) {
        val policy = this.get_by_index(policy_index);
        for (String policy_alias : aliases) {
            if (this.by_name.containsKey(policy_alias.toUpperCase())) {
                throw new PolicyError("Duplicate name " +
                                      policy_alias +
                                      " in use by policy " +
                                      this.get_by_name(policy_alias));
            } else {
                policy.add_name(policy_alias);
                this.by_name[policy_alias.toUpperCase()] = policy;
            }
        }
    }

    /**
    Removes a name or names from a policy. If the name removed is the
    primary name then the next available alias will be adopted
    as the new primary name.

    @param aliases arbitrary number of existing policy names to remove.
    */
    def remove_policy_alias(aliases: List[String]) {
        for (String policy_alias : aliases) {
            BaseStoragePolicy policy = this.get_by_name(policy_alias);
            if (null == policy) {
                throw new PolicyError("No policy with name " +
                                      alias + " exists.");
            }
            if (policy.alias_list.size() == 1) {
                throw new PolicyError("Policy " +
                                      policy +
                                      " with name " +
                                      alias +
                                      " has only one name. " +
                                      "Policies must have at " +
                                      "least one name.");
            } else {
                policy.remove_name(alias);
                this.by_name.remove(alias.toUpperCase());
            }
        }
    }

    /**
    Changes the primary or default name of a policy. The new primary
    name can be an alias that already belongs to the policy or a
    completely new name.

    @param policy_index index of a policy in this policy collection.
    @param new_name a string name to set as the new default name.
    */
    def change_policy_primary_name(policy_index: Int, new_name: String) {
        val policy = this.get_by_index(policy_index);
        val name_taken = this.get_by_name(new_name);
        // if the name belongs to some other policy in the collection
        if (name_taken != null && name_taken != policy) {
            throw new PolicyError("Other policy " +
                                  this.get_by_name(new_name) +
                                  " with name " +
                                  new_name + " exists.");
        } else {
            policy.change_primary_name(new_name);
            this.by_name[new_name.toUpperCase()] = policy;
        }
    }
}
