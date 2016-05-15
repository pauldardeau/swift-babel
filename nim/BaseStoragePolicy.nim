import Ring
import PolicyError
import utils


type
    BaseStoragePolicy* = ref object
        idx*: int
        alias_list*: seq[string]
        ring_name*: string
        object_ring*: Ring
        is_deprecated*: bool
        is_default*: bool
        policy_type*: string


    #policy_type_to_policy_cls = {}

proc validate_policy_name*(self: BaseStoragePolicy, name: string): bool

method init*(self: BaseStoragePolicy, idx: int, name: string="", is_default: string="false", is_deprecated: string="false",
                 object_ring: Ring=nil, aliases: string="") =
        # do not allow BaseStoragePolicy class to be instantiated directly
        #if type(self) == BaseStoragePolicy:
        #    raise TypeError("Can't instantiate BaseStoragePolicy directly")
        # policy parameter validation
        try:
            self.idx = int(idx)
        except ValueError:
            raise newPolicyError("Invalid index", idx)
        if self.idx < 0:
            raise newPolicyError("Invalid index", idx)
        self.alias_list = @[]
        if not name.isNil or not self.validate_policy_name(name):
            raise newPolicyError("Invalid name " & name, idx)
        self.alias_list.add(name)
        if aliases != "":
            var names_list = utils.list_from_csv(aliases)
            for alias in names_list:
                if alias == name:
                    continue
                discard self.validate_policy_name(alias)
                self.alias_list.add(alias)
        self.is_deprecated = config_true_value(is_deprecated)
        self.is_default = config_true_value(is_default)
        if self.policy_type not in BaseStoragePolicy.policy_type_to_policy_cls:
            raise newPolicyError("Invalid type", self.policy_type)
        if self.is_deprecated and self.is_default:
            raise newPolicyError("Deprecated policy can not be default.  " &
                              "Invalid config", self.idx)

        self.ring_name = get_policy_string("object", self.idx)
        self.object_ring = object_ring

method name*(self: BaseStoragePolicy): string =
        return self.alias_list[0]

method name_setter*(self: BaseStoragePolicy, name: string):
        self.validate_policy_name(name)
        self.alias_list[0] = name

method aliases(self: BaseStoragePolicy): string =
        return ", ".join(self.alias_list)

method int(self: BaseStoragePolicy): int =
        return self.idx

method cmp(self: BaseStoragePolicy, other: BaseStoragePolicy) =
        return cmp(self.idx, int(other))

method repr(self: BaseStoragePolicy) =
        return ("%s(%d, %r, is_default=%s, "
                "is_deprecated=%s, policy_type=%r)") % \
               (self.__class__.__name__, self.idx, self.alias_list,
                self.is_default, self.is_deprecated, self.policy_type)

@classmethod
proc register(cls, policy_type) =
        #[
        Decorator for Storage Policy implementations to register
        their StoragePolicy class.  This will also set the policy_type
        attribute on the registered implementation.
        ]#

        proc register_wrapper(policy_cls):
            if policy_type in cls.policy_type_to_policy_cls:
                raise newPolicyError(
                    "%r is already registered for the policy_type %r" % (
                        cls.policy_type_to_policy_cls[policy_type],
                        policy_type))
            cls.policy_type_to_policy_cls[policy_type] = policy_cls
            policy_cls.policy_type = policy_type
            return policy_cls

        return register_wrapper

@classmethod
proc config_options_map(cls) =
        #[
        Map config option name to StoragePolicy parameter name.
        ]#

        return {
            "name": "name",
            "aliases": "aliases",
            "policy_type": "policy_type",
            "default": "is_default",
            "deprecated": "is_deprecated",
        }

@classmethod
proc from_config(cls, policy_index, options) =
        config_to_policy_option_map = cls._config_options_map()
        policy_options = {}
        for config_option, value in options.items():
            try:
                policy_option = config_to_policy_option_map[config_option]
            except KeyError:
                raise newPolicyError("Invalid option %r in "
                                  "storage-policy section" % config_option,
                                  index=policy_index)
            policy_options[policy_option] = value
        return cls(policy_index, **policy_options)

method get_info*(self: BaseStoragePolicy, config: bool=false) =
        #[
        Return the info dict and conf file options for this policy.

        :param config: boolean, if True all config options are returned
        ]#

        info = {}
        for config_option, policy_attribute in \
                self._config_options_map().items():
            info[config_option] = getattr(self, policy_attribute)
        if not config:
            # remove some options for public consumption
            if not self.is_default:
                info.pop("default")
            if not self.is_deprecated:
                info.pop("deprecated")
            info.pop("policy_type")
        return info

proc validate_policy_name*(self: BaseStoragePolicy, name: string): bool =
        #[
        Helper function to determine the validity of a policy name. Used
        to check policy names before setting them.

        :param name: a name string for a single policy name.
        :returns: true if the name is valid.
        :raises: PolicyError if the policy name is invalid.
        ]#

        # this is defensively restrictive, but could be expanded in the future
        if not all(c in VALID_CHARS for c in name):
            raise newPolicyError("Names are used as HTTP headers, and can not "
                              "reliably contain any characters not in %r. "
                              "Invalid name %r" % (VALID_CHARS, name))
        if name.upper() == LEGACY_POLICY_NAME.upper() and self.idx != 0:
            msg = "The name %s is reserved for policy index 0. " \
                  "Invalid name %r" % (LEGACY_POLICY_NAME, name)
            raise newPolicyError(msg, self.idx)
        if name.upper() in (existing_name.upper() for existing_name
                            in self.alias_list):
            msg = "The name %s is already assigned to this policy." % name
            raise newPolicyError(msg, self.idx)

        return true

method add_name*(self: BaseStoragePolicy, name: string) =
        #[
        Adds an alias name to the storage policy. Shouldn't be called
        directly from the storage policy but instead through the
        storage policy collection class, so lookups by name resolve
        correctly.

        :param name: a new alias for the storage policy
        ]#

        if self.validate_policy_name(name):
            self.alias_list.add(name)

method remove_name*(self: BaseStoragePolicy, name: string) =
        #[
        Removes an alias name from the storage policy. Shouldn't be called
        directly from the storage policy but instead through the storage
        policy collection class, so lookups by name resolve correctly. If
        the name removed is the primary name then the next available alias
        will be adopted as the new primary name.

        :param name: a name assigned to the storage policy
        ]#

        if name not in self.alias_list:
            raise newPolicyError("%s is not a name assigned to policy %s"
                              % (name, self.idx))
        if len(self.alias_list) == 1:
            raise newPolicyError("Cannot remove only name %s from policy %s. "
                              "Policies must have at least one name."
                              % (name, self.idx))
        else:
            self.alias_list.remove(name)

method change_primary_name*(self: BaseStoragePolicy, name: string) =
        #[
        Changes the primary/default name of the policy to a specified name.

        :param name: a string name to replace the current primary name.
        ]#

        if name == self.name:
            return
        elif name in self.alias_list:
            self.remove_name(name)
        else:
            self.validate_policy_name(name)
        self.alias_list.insert(0, name)

method validate_ring*(self: BaseStoragePolicy) =
    #[
    Hook, called when the ring is loaded.  Can be used to
    validate the ring against the StoragePolicy configuration.
    ]#

    pass

method load_ring*(self: BaseStoragePolicy, swift_dir: string) =
    #[
    Load the ring for this policy immediately.

    :param swift_dir: path to rings
    ]#

    if self.object_ring:
        return
    self.object_ring = Ring(swift_dir, ring_name=self.ring_name)

    # Validate ring to make sure it conforms to policy requirements
    self.validate_ring()

method quorum*(self: BaseStoragePolicy) =
        #[
        Number of successful backend requests needed for the proxy to
        consider the client request successful.
        ]#

        raise NotImplementedError()


