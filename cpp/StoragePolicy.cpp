#include "StoragePolicy.h"
#include "StoragePolicyCollection.h"


using namespace std;


static const string NAME_VALID_CHARS =
    string("abcdefghijklmnopqrstuvwxyz") +
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
    "0123456789" +
    "-";
const string StoragePolicy::LEGACY_POLICY_NAME = "Policy-0";
const string StoragePolicy::UPPER_LEGACY_POLICY_NAME = "POLICY-0";


bool StoragePolicy::_validate_policy_name(const string& name) const {
    const string& valid_chars = NAME_VALID_CHARS;

    if (!StrUtils::contains_only_chars_in(name, valid_chars)) {
        string msg =
            string("Names are used as HTTP headers, and can not ") +
            "reliably contain any characters not in " +
            valid_chars +
            ". Invalid name " +
            name;
        throw PolicyError(msg);
    }

    const string upper_name = StrUtils::upper(name);

    if (upper_name == StoragePolicy::UPPER_LEGACY_POLICY_NAME &&
        this->_idx != 0) {
        string msg = string("The name ") +
                          StoragePolicyCollection::LEGACY_POLICY_NAME +
                          " is reserved for policy index 0. "
                          "Invalid name " +
                          name;
        throw PolicyError(msg, this->_idx);
    }

    const vector<string>::const_iterator itAliasEnd =
        this->_alias_list.end();
    vector<string>::const_iterator itAlias =
        this->_alias_list.begin();

    for (; itAlias != itAliasEnd; ++itAlias) {
        const string& existing_name = *itAlias;
        const string upper_existing_name = StrUtils::upper(existing_name);

        if (upper_name == upper_existing_name) {
            const string msg =
                string("The name ") +
                       name +
                       " is already assigned to this policy.";
            throw PolicyError(msg, this->_idx);
        }
    }

    return true;
}

