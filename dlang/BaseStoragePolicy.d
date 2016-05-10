module BaseStoragePolicy;

import std.conv;
import std.string;
//import std.uni;

import PolicyError;
import Ring;
import Utils;


/**
Represents a storage policy.  Not meant to be instantiated directly;
implement a derived subclasses (e.g. StoragePolicy, ECStoragePolicy, etc)
or use :func:`~swift.common.storage_policy.reload_storage_policies` to
load POLICIES from ``swift.conf``.

The object_ring property is lazy loaded once the service's ``swift_dir``
is known via :meth:`~StoragePolicyCollection.get_object_ring`, but it may
be over-ridden via object_ring kwarg at create time for testing or
actively loaded with :meth:`~StoragePolicy.load_ring`.
*/
abstract class BaseStoragePolicy {

private:
    int idx;
    string ring_name;
    Ring object_ring;
    bool is_deprecated;
    bool is_default;

    static string VALID_CHARS = "abcdefghijklmnopqrstuvwxyz" ~
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" ~ "0123456789" ~ "-";


public:
    static string LEGACY_POLICY_NAME = "Policy-0";

    string[] alias_list;


    /*policy_type_to_policy_cls = {}*/


public:
    this(int idx,
         string name="",
         bool is_default=false,
         bool is_deprecated=false,
         Ring object_ring=null,
         string aliases="") {

        // do not allow BaseStoragePolicy class to be instantiated directly
        /*
        if (type(this) == BaseStoragePolicy) {
            throw new TypeError("Can't instantiate BaseStoragePolicy directly");
        }
        */

        // policy parameter validation
        try {
            this.idx = to!int(idx);
        } catch (ConvException ve) {
            throw new PolicyError("Invalid index", idx);
        }

        if (this.idx < 0) {
            throw new PolicyError("Invalid index", idx);
        }
        this.alias_list = [];
        if (!name || !this._validate_policy_name(name)) {
            throw new PolicyError("Invalid name " ~ name, idx);
        }
        this.alias_list ~= name;
        if (aliases) {
            string[] names_list = SwiftUtils.list_from_csv(aliases);
            foreach (name_alias; names_list) {
                if (name_alias == name) {
                    continue;
                }
                this._validate_policy_name(name_alias);
                this.alias_list ~= name_alias;
            }
        }
        this.is_deprecated = Utils.config_true_value(is_deprecated);
        this.is_default = Utils.config_true_value(is_default);
        if (this.policy_type !in BaseStoragePolicy.policy_type_to_policy_cls) {
            throw new PolicyError("Invalid type", this.policy_type);
        }
        if (this.is_deprecated && this.is_default) {
            throw new PolicyError("Deprecated policy can not be default.  " ~
                              "Invalid config", this.idx);
        }

        this.ring_name = _get_policy_string("object", this.idx);
        this.object_ring = object_ring;
    }

    @property string name() {
        return this.alias_list[0];
    }

    @property void name(string name) {
        this._validate_policy_name(name);
        this.alias_list[0] = name;
    }

    @property string aliases() {
        return ", ".join(this.alias_list);
    }

    public string[] upper_alias_list() {
        //TODO: implement upper_alias_list
        return [];
    }

    int __int__() {
        return this.idx;
    }

    int __cmp__(BaseStoragePolicy other) {
        return cmp(this.idx, to!int(other));
    }

    override string toString() {
        return ("%s(%d, %r, is_default=%s, " ~
                "is_deprecated=%s, policy_type=%r)") %
               (this.__class__.__name__, this.idx, this.alias_list,
                this.is_default, this.is_deprecated, this.policy_type);
    }

    /*
    void register_wrapper(policy_cls) {
        if (policy_type in cls.policy_type_to_policy_cls) {
            throw new PolicyError(
                "%r is already registered for the policy_type %r" % (
                    cls.policy_type_to_policy_cls[policy_type],
                    policy_type));
        }
        cls.policy_type_to_policy_cls[policy_type] = policy_cls;
        policy_cls.policy_type = policy_type;
        return policy_cls;
    }
    */

    /**
    Decorator for Storage Policy implementations to register
    their StoragePolicy class.  This will also set the policy_type
    attribute on the registered implementation.
    */

    //@classmethod
    /*
    static void register(cls, policy_type) {
        return register_wrapper;
    }
    */

    //@classmethod
    static string[string] _config_options_map() {
        /*
        Map config option name to StoragePolicy parameter name.
        */
        return [
            "name": "name",
            "aliases": "aliases",
            "policy_type": "policy_type",
            "default": "is_default",
            "deprecated": "is_deprecated",
        ]; 
    }

    /*
    @classmethod
    void from_config(cls, int policy_index, options) {
        config_to_policy_option_map = cls._config_options_map();
        policy_options = {};
        foreach (config_option, value; options.items()) {
            try {
                policy_option = config_to_policy_option_map[config_option];
            } catch (KeyError ke) {
                throw new PolicyError("Invalid option %r in " ~
                                  "storage-policy section" % config_option,
                                  index=policy_index);
            }
            policy_options[policy_option] = value;
        }
        return cls(policy_index, policy_options);
    }
    */

    /**
    Return the info dict and conf file options for this policy.

    :param config: boolean, if True all config options are returned
    */
/*
    string[string] get_info(bool config=false) {
        info = string[string];
        foreach (config_option, policy_attribute) {
                this._config_options_map().items()) {
            info[config_option] = getattr(this, policy_attribute);
            }
        }

        if (!config) {
            // remove some options for public consumption
            if (!this.is_default) {
                info.pop("default");
            }
            if (!this.is_deprecated) {
                info.pop("deprecated");
            }
            info.pop("policy_type");
        }

        return info;
    }
*/

    /**
    Helper function to determine the validity of a policy name. Used
    to check policy names before setting them.

    :param name: a name string for a single policy name.
    :returns: true if the name is valid.
    :raises: PolicyError if the policy name is invalid.
    */
    bool _validate_policy_name(string name) {
        // this is defensively restrictive, but could be expanded in the future
        string msg;

        if (-1 != name.indexOfNeither(VALID_CHARS)) {
        //if (!all(c in VALID_CHARS for c in name)) {
            throw new PolicyError("Names are used as HTTP headers, and can not " ~
                              "reliably contain any characters not in " ~ VALID_CHARS ~ ". " ~
                              "Invalid name " ~ name);
        }

        string upper_name = name.toUpper();

        if (upper_name == LEGACY_POLICY_NAME.toUpper() && this.idx != 0) {
            msg = "The name " ~ LEGACY_POLICY_NAME ~ " is reserved for policy index 0. " ~
                  "Invalid name " ~ name;
            throw new PolicyError(msg, this.idx);
        }

        if (upper_name in this.upper_alias_list()) {
            msg = "The name " ~ name ~ " is already assigned to this policy.";
            throw new PolicyError(msg, this.idx);
        }

        return true;
    }

    /*
    Adds an alias name to the storage policy. Shouldn't be called
    directly from the storage policy but instead through the
    storage policy collection class, so lookups by name resolve
    correctly.

    :param name: a new alias for the storage policy
     */
    void add_name(string name) {
        if (this._validate_policy_name(name)) {
            this.alias_list ~= name;
        }
    }

    /**
    Removes an alias name from the storage policy. Shouldn't be called
    directly from the storage policy but instead through the storage
    policy collection class, so lookups by name resolve correctly. If
    the name removed is the primary name then the next available alias
    will be adopted as the new primary name.

    :param name: a name assigned to the storage policy
    */
    void remove_name(string name) {
        if (name !in this.alias_list) {
            throw new PolicyError(name ~ " is not a name assigned to policy " ~ to!string(this.idx);
        }

        if (this.alias_list.length == 1) {
            string msg = "Cannot remove only name " ~
                         name ~
                         " from policy " ~
                         to!string(this.idx) ~
                         ". Policies must have at least one name.";
            throw new PolicyError(msg);
        } else {
            this.alias_list.remove(name);
        }
    }

    /**
    Changes the primary/default name of the policy to a specified name.

    :param name: a string name to replace the current primary name.
    */
    void change_primary_name(string name) {
        if (name == this.name) {
            return;
        } else if (name in this.alias_list) {
            this.remove_name(name);
        } else {
            this._validate_policy_name(name);
        }
        this.alias_list.insert(0, name);
    }

    /**
    Hook, called when the ring is loaded.  Can be used to
    validate the ring against the StoragePolicy configuration.
    */
    void _validate_ring() {
        //pass
    }

    /**
    Load the ring for this policy immediately.

    :param swift_dir: path to rings
    */
    void load_ring(string swift_dir) {
        if (this.object_ring != null) {
            this.object_ring = new Ring(swift_dir, this.ring_name);

            // Validate ring to make sure it conforms to policy requirements
            this._validate_ring();
        }
    }

    /**
    Number of successful backend requests needed for the proxy to
    consider the client request successful.
    */
    abstract @property int quorum();
}
