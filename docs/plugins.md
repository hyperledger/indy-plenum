# Plugins

Plugins allow for supporting new transactions without modifying/adding to the core parts of the system. 
Supporting any new transaction requires:
-   Defining new transaction types
-   Defining new message validation rules or updates to any message validation rules
-   Logic to handle new transactions, validation, making changes to state, etc (for more detail look at the doc on [request handling](request_handling.md)).
-   Storage of transactions in existing ledger or introducing new ledgers
-   Changes to existing state or introducing new states  
-   Defining new constants
-   Defining any new config variables


### Conventions that need to be followed when defining a new plugin:
#   Each plugin is a separate python package that lives under the package defined in `PLUGIN_ROOT` in `config.py`. It is currently set to `plenum.server.plugin`
#   The list of enabled plugins is defined in `ENABLED_PLUGINS` in `config.py`.
#   Each plugin has unique name, this is the name in the list `ENABLED_PLUGINS` and has to match the package name of the plugin.
#   The plugin package contains defines several items in the `__init__.py` file
    -   It contains a ledger id for each new ledger that the plugin introduces, see `LEDGER_IDS`
    -   For each new field that needs to be added in the `Request`, the plugin defines the name and its corresponding input validation function in a dict, see `CLIENT_REQUEST_FIELDS`
    -   Each write or query transaction type is defined in `AcceptableWriteTypes` and `AcceptableQueryTypes` respectively
#   Each plugin package contains a `main.py` that contains a method `integrate_plugin_in_node` to initialise and register any new ledgers, states, authenticators, 
    request handlers, etc.
#   If the plugin requires a new authenticator, it should be defined in `client_authn.py` in the plugin package.
#   New storage layers should be defined in `storage.py` in the package
#   New transactions need to be in `transactions.py` in the plugin package. To avoid conflicts in transaction type values, each plugin defines a unique prefix to be added before 
    the value of the transaction type, see `PREFIX`.
#   Any new config variables that have to be introduced are introduced in `get_config` in `config.py` in the plugin package.


### Plugin setup
When plenum initialises, initialises all enabled plugins; this is done through `setup_plugins` method defined in `plenum/__init__.py` Plenum defines 2 project level globals:
`PLUGIN_LEDGER_IDS` which would be the set of ids of all new ledgers defined by any enabled plugin and `PLUGIN_CLIENT_REQUEST_FIELDS` would be a dictionary of all new `Request` fields
defined by any plugin and its corresponding validator.
`setup_plugins` iterates over each of the enabled plugin packages and updates `PLUGIN_LEDGER_IDS` and `PLUGIN_CLIENT_REQUEST_FIELDS`. 
It then reloads some modules which defines input validation objects and schemas, this is done since set of valid ledger ids of request fields might have changed.


### Demo plugin
The Demo plugin serves as an example of how a plugin should be written, though it does some extra setup actions that an actual plugin will not do.
The reason for those extra actions is that by the time the plugin tests are loaded, the configuration has already been loaded, several other objects 
have already been initialised. The demo plugin is located at `plenum/test/plugin/demo_plugin`. The demo plugin models a very trivial auction functionality.
#   The plugin defines new transaction in `plenum/test/plugin/demo_plugin/transactions.py`
#   The plugin defines constants denoting transaction types in `plenum/test/plugin/demo_plugin/constants.py`
#   The demo plugin defines a new ledger for which it declares the ledger id `AUCTION_LEDGER_ID` in `plenum/test/plugin/demo_plugin/__init__.py`. 
    Also the plugin defines a new field in request called `fix_length_dummy` and corresponding input validator for that.
#   New ledger, hash store and state are defined in `plenum/test/plugin/demo_plugin/storage.py`
#   A request handler for the plugin introduced transactions are defined, `AuctionReqHandler` in `plenum/test/plugin/demo_plugin/auction_req_handler.py`. 
    The request handler defines methods for static validation, dynamic validation and state changes.
#   New config entries for ledger name, state name, storage type are defined in `plenum/test/plugin/demo_plugin/config.py`
#   An authenticator `AuctionAuthNr` is defined in `plenum/test/plugin/demo_plugin/client_authnr.py`, that specifies the query and write types.
#   The integration of this plugin happens in `integrate_plugin_in_node` in `plenum/test/plugin/demo_plugin/main.py` which takes the node as an argument. In this integration method:
    -   Update the node's config with the plugin's config
    -   Initialise the plusin's ledger, state and other storage components
    -   Register the storage components in the node.
    -   Initialise the authenticator and register it with the node
    -   Initialise and regsiter the request handler with the node.
#   Now the above method can be called on any initialised node. For the tests, the `TestNode`s are updated in the fixture `do_post_node_creation` in plenum/test/plugin/demo_plugin/conftest.py
