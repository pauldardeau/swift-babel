import java.util.ArrayList;
import java.util.List;


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

    private int idx;
    private ArrayList<String> alias_list;
    private Boolean is_deprecated;
    private Boolean is_default;
    private String ring_name;
    private Ring object_ring;


    /*policy_type_to_policy_cls = {}*/


    def BaseStoragePolicy(idx: Int,
                             name: String, //="",
                             is_default: Boolean, //=false,
                             is_deprecated: Boolean, //=false,
                             object_ring: Ring, //=null,
                             aliases: String) { //="") {
        // do not allow BaseStoragePolicy class to be instantiated directly
        if (type(this) == BaseStoragePolicy) {
            throw new TypeError("Can't instantiate BaseStoragePolicy directly");
        }
        // policy parameter validation
        if (this.idx < 0) {
            throw new PolicyError("Invalid index", idx);
        }
        this.alias_list = new ArrayList<String>();
        if (!name || !this._validate_policy_name(name)) {
            throw new PolicyError("Invalid name " + name, idx);
        }
        this.alias_list.append(name);
        if (aliases != null) {
            List<String> names_list = list_from_csv(aliases);
            for (String name_alias : names_list) {
                if (name_alias.equals(name)) {
                    continue;
                }
                this._validate_policy_name(name_alias);
                this.alias_list.append(name_alias);
            }
        }
        this.is_deprecated = config_true_value(is_deprecated);
        this.is_default = config_true_value(is_default);
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

    @property
    def name() : String = {
        return this.alias_list[0];
    }

    def name_setter(name: String) {
        this._validate_policy_name(name);
        this.alias_list[0] = name;
    }

    @property
    def aliases() : String = {
        return ", ".join(this.alias_list);
    }

    def toInt() : Int {
        return this.idx;
    }

    def __cmp__(BaseStoragePolicy other) : Int = {
        return cmp(this.idx, other.idx);
    }

    def toString() : String = {
        return ("%s(%d, %r, is_default=%s, " + 
                "is_deprecated=%s, policy_type=%r)") %
               (this.__class__.__name__, this.idx, this.alias_list,
                this.is_default, this.is_deprecated, this.policy_type);
    }

    def static register(policy_type) {
        /*
        Decorator for Storage Policy implementations to register
        their StoragePolicy class.  This will also set the policy_type
        attribute on the registered implementation.
        */

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

        return register_wrapper;
    }

    def static _config_options_map() {
        /*
        Map config option name to StoragePolicy parameter name.
        */
        return {
            "name": "name",
            "aliases": "aliases",
            "policy_type": "policy_type",
            "default": "is_default",
            "deprecated": "is_deprecated",
        }
    }

    def static from_config(policy_index: Int, options) {
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

    /**
    Return the info dict and conf file options for this policy.

    @param config Boolean, if True all config options are returned
    */
    def get_info(config: Boolean=false) {
        info = {};
        foreach (config_option, policy_attribute) {
                this._config_options_map().items() {
            info[config_option] = getattr(this, policy_attribute);
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

    /**
    Helper function to determine the validity of a policy name. Used
    to check policy names before setting them.

    @param name a name string for a single policy name.
    @return true if the name is valid.
    @throws PolicyError if the policy name is invalid.
    */
    def _validate_policy_name(name: String) : Boolean {
        // this is defensively restrictive, but could be expanded in the future
        String msg;

        if (!all(c in VALID_CHARS for c in name)) {
            throw new PolicyError("Names are used as HTTP headers, and can not " ~
                              "reliably contain any characters not in %r. " ~
                              "Invalid name %r" % (VALID_CHARS, name));
        }

        if (name.toUpperCase() == LEGACY_POLICY_NAME.toUpperCase() && this.idx != 0) {
            msg = "The name %s is reserved for policy index 0. " ~
                  "Invalid name %r" % (LEGACY_POLICY_NAME, name);
            throw new PolicyError(msg, this.idx);
        }

        if (name.toUpperCase() in (existing_name.toUpperCase() for existing_name
                            in this.alias_list)) {
            msg = "The name %s is already assigned to this policy." % name
            throw new PolicyError(msg, this.idx);
        }

        return true;
    }

    /**
    Adds an alias name to the storage policy. Shouldn't be called
    directly from the storage policy but instead through the
    storage policy collection class, so lookups by name resolve
    correctly.

    @param name a new alias for the storage policy
    */
    def add_name(name: String) {
        if (this._validate_policy_name(name)) {
            this.alias_list.add(name);
        }
    }

    /**
    Removes an alias name from the storage policy. Shouldn't be called
    directly from the storage policy but instead through the storage
    policy collection class, so lookups by name resolve correctly. If
    the name removed is the primary name then the next available alias
    will be adopted as the new primary name.

    @param name a name assigned to the storage policy
    */
    def remove_name(name: String) {
        if (name !in this.alias_list) {
            throw new PolicyError(name +
                                  " is not a name assigned to policy " +
                                  this.idx);
        }
        if (this.alias_list.length == 1) {
            throw new PolicyError("Cannot remove only name " +
                                  name +
                                  " from policy " +
                                  this.idx +
                                  ". Policies must have at least one name.");
        } else {
            this.alias_list.remove(name);
        }
    }

    /**
    Changes the primary/default name of the policy to a specified name.

    @param name a string name to replace the current primary name.
    */
    def change_primary_name(name: String) {
        if (name.equals(this.name)) {
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
    def _validate_ring() {
        //pass
    }

    /**
    Load the ring for this policy immediately.

    @param swift_dir path to rings
    */
    def load_ring(swift_dir: String) {
        if (this.object_ring == null) {
            this.object_ring = new Ring(swift_dir, this.ring_name);
            // Validate ring to make sure it conforms to policy requirements
            this._validate_ring();
        }
    }

    /**
     * Number of successful backend requests needed for the proxy to
     * consider the client request successful.
     */
    def abstract quorum() : Int;
}
