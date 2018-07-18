# Log processing utility

## Introduction

When analyzing logs from nodes it's often very hard to find out what's
going on because there are too many things logged. Since plenum is quite 
complex piece of software many things could potentially go bad, so it's 
very hard to reduce amount of logging and not lose valuable information 
in the process. Also it's not always possible to clearly define severity
levels (debug, info, warning, etc) since the very same event in different
circumstances could be either normal or totally unexpected. Sometimes these 
issues can be addressed by grepping through logs, but in order to do so 
one needs knowledge of what to look for, which is not always available.

One useful technique of dealing with this problem is to start filtering out
messages in which we are definitely not interested, look for leftovers and
then repeat process until either something interesting is found or nothing
is left. Another is gathering some statistics (like log writing intensity)
and presenting them graphically, so any anomalies can be easily spotted,
narrowing down search area. Log processor was started as a tool supporting
these techniques.

## Basic principles

Log processor consists of `process_log` command line utility and comon
config file `process_log.yml` placed next to it. Basic workflow consists 
of writing some custom config, calling `process_log custom_config.yml`,
looking at output, adding more rules to custom config as needed and
repeating this process until achieving desired results. Custom config can
use some common rules defined in `process_log.yml` (these configs have same 
format). Also there are some example configs provided in `example_*.yml` files 
which could be used as a starting point.

Configuration file consists of three main parts:
- input log gathering rules
- configuration of outputs (log, graphs or simple counter)
- processing rules (chains and matchers)

Log processor finds all relevant log files, parses each message and starts
applying rules from `main` chain until completion (which could be end of chain
or some early drop). Most common rules are jumping to another chain, checking
if message fits some matchers and logging to different outputs. There are also
rules for modifying messages like time shifting or attaching custom attributes.

## Configuration sections

### input_logs

Contains list of rules where to find logs and how to understand which node
they belong to. Options are:
- `path`: where to look for log files
- `recursive`: whether to descend into subdirectories recursively
- `pattern`: log file name regex pattern to look for
- `node_group`: regex group number that matches node identifier
- `only_timestamped`: whether to discard non-timestamped messages
- `min_timestamp`: minimum timestamp of accepted messages
- `max_timestamp`: maximum timestamp of accepted messages

### outputs

Contains subsections describing different outputs.

#### logs

Contains dictionary of output log files, each with following options:
- `filename`: output filename, possibly with `<node>` and/or `<replica>` 
  placeholders when logs from different nodes/replicas should be placed 
  in different files
- `pattern`: log line format, can contain the following placeholders:
  - `<timestamp>`: event timestamp
  - `<node>`: source node identifier
  - `<replica>`: source replica identifier
  - `<level>`: log level (DEBUG, INFO, WARNING, etc)
  - `<source>`: source file which emitted message
  - `<body>`: message body
  - `<user attr>`: any user-defined attribute, explained later in 
    *chains* section 

#### timelogs

Contains dictionary of output event intensity plots, each with following 
options:
- `interval`: sampling interval in seconds
- `filename`: output csv filename
- `graphs`: list of graphs on plot

#### counters

Contains dictionary of output event counters, each with simple option
`format` containing string, which will be output for each node when all
input logs are processed. This string can contain `<node>` placeholder,
as well as any `<subcounter>` placeholder which were used in `log count` 
commands, explained later in *chains* section

#### requests

Contains reporting options of request tracker:
- `report_stats`: whether to report statistics at all
- `report_lags`: whether to report request id's which took more than minute 
  to order
- `filename`: csv filename to output requests time to order

### matchers

Contains dictionary of custom matchers, each containing a list key-value
pairs describing sub-matchers (builtin or custom) to check. Key is type 
of matcher, value is parameter. For submatchers that don't have parameters
(for example, other custom matchers) value should be omitted. Custom matcher 
matches message if any of its sub-matchers matches message.

Matcher name resolution rules are:
- check if this is any of builtin matchers (listed below)
- check if this is a custom matcher
- consider this custom attribute matcher

Attribute matchers check if message has given attribute, and, when value is 
provided, if attribute contains given value. For example, matcher
```yaml
- is_request:
``` 
checks that message has attribute `is_request`, and matcher
```yaml
- reqId: xz 42
```
checks that message has attribute `reqId` containing value `xz 42`


#### Builtin matchers
- `timestamp`: checks if message timestamp is within defined limits. 
  Parameter for this matcher is dictionary with `min` and `max` values 
  containing lower and upper bounds for timestamp, for example:
  ```yaml
  - timestamp:
      min: 2018-03-15 10:30:12
      max: 2018-03-15 10:30:16
  ```
  Any of `min` or `max` can be omitted to skip check of lower or upper bound.
  Note that using `min/max_timestamp` attributes from `input_logs` section
  is preferred since they can skip processing whole files which greatly
  improves performance.
- `level`: checks if message severity level is within defined limits. 
  Parameters for this matcher is either dictionary with `min` and `max` 
  values containing lower and upper bounds for message severity (`DEBUG`, 
  `INFO`, `WARNING`, etc), or just a simple identifier, which is shorthand 
  for specifying it for both `min` and `max`, for example:
  ```yaml
  - level: WARNING
  ```
  As with timestamp any of `min` or `max` can be omitted.
- `message`: checks if body of message matches given regex, which is 
  parameter for this matcher, for example:
  ```yaml
  - message: has not prepared \((\d+), (\d+)\), will dequeue the COMMITs later
  ```
  This matcher is implemented using python `matcher.search` method, which has
  optimizations for simple substring search, so to improve performance it's
  generally better to prefer simple strings, like
  ```yaml
  - message: cannot send COMMIT since does not have prepare quorum for PREPARE
  ```
- `replica`: checks if message is from given replica, which could be a replica
  index or one of special strings:
  - `node`: message is actually from node, not replica
  - `master`: message is from master replica or from node
  - `backup`: message is from any of backup replicas
- `any`: check if message matches any submatcher, specified as a list:
  ```yaml
  - any:
    - message: sending message
    - message: appending to nodeInbox
  ```
- `all`: check if message matches all submatchers, specified as a list:
  ```yaml
  - all:
    - message: "VIEW CHANGE:"
    - message: CURRENT_STATE  
  ```
  
### chains

Contains dictionary of chains, each containing a list of key-value pairs
describing commands to execute. Key is type of command, value is parameter.
For commands not having parameters (like jumping to other chain) value 
should be omitted. If type of command is not any of builtin types it's
considered a name of custom chain to which jump should be performed.

Each message enters `main` chain, and is processed until it either reaches 
end of this chain or is dropped. When message jumps to some custom chain 
and reaches it's end it returns to source chain and continues processing
from command next to jump. For example, in this setup:
```yaml
- main:
  - do_a:
  - subchain:
  - do_b:
  
- subchain:
  - do_something_special:
  - do_something_exotic: 
```
each message is first processed by `do_a` command, then jumps to subchain 
`subchain` where it's processed by `do_something_special` and 
`do_something_exotic` commands, and then returns to main chain where it's
processed by `do_b` command. This allows for arbitrary complex processing 
rules to be split into manageable pieces, and for building library of
some common filters. 

There's also special `drop` chain, jumping to which stops processing of
message prematurely. For example, in this setup:
```yaml
- main:
  - do_a:
  - subchain:
  - do_b:
  
- subchain:
  - do_something_special:
  - drop:
  - do_something_exotic: 
```
each message is processed by `do_a` and `do_something_special` commands, but
then is dropped and never reaches neither `do_something_exotic` nor `do_b`
commands.

#### Builtin commands

- `log line`: adds message to given output log target, for example:
  ```yaml
  chains:
    main:
      - log line: fancy_log
    
  logs:
    fancy_log:
       ...
  ```
- `log time`: registers message to given graph of given timelog, for example:
  ```yaml
  chains:
    main:
      - log time: {monitoring: some_metric}
    
  timelogs:
    monitoring:
      graphs:
        some_metric: blue
  ```
- `log count`: registers message to given subcounter of given counter, 
  for example:
  ```yaml
  chains:
    main:
      - log count: {stats: total}
    
  counters:
    stats:
      format: <node> emitted <total> messages
  ```
- `timeshift`: shifts timestamp for some nodes by given amount of seconds,
  for example:
  ```yaml
  - timeshift:
      Node12: 300
      Node2: -3600
  ``` 
- `match`: checks if messages matches given matcher(s), specified as a list,
  and returns from chain or drops message altogether if needed. In fact there 
  are several match commands with following pattern:
  ```
  match [any|all] [(or|and) (return|drop)]
  ```
  When all optional parts are omitted match command is interpreted as 
  `match any or return`. Meaning of options is as follows:
  - `any`: consider message matched if any of specified matchers triggered
  - `all`: consider message matched if all of specified matchers triggered
  - `or`: perform action when message is not matched
  - `and`: perform action when message is matched
  - `return`: action to perform is to return from current chain to calling one
  - `drop`: action to perform is to drop message altogether
- `track_requests`: track requests, adding multiple attributes to relevant
  messages:
  - `reqId`: request identifier, composed from `identifier` and `reqId` 
    separated by space
  - `viewNo`: view number
  - `ppSeqNo`: pre-prepare sequence number 
  - TODO: list other attibutes
- `tag`: optionally checks if message matches some regex pattern and sets
  custom tags and/or attributes on it. Parameter for this command is dictionary
  with following keys:
  - `pattern`: pattern to match message body
  - `attributes`: dictionary of custom attributes to add to message on match,
    with keys being attribute names and values being one of:
    - empty: indicating tag-like attribute which doesn't have any value
    - arbitrary string: sets this string as attribute value
    - `group <n>`: sets matching regex group as attribute value
  For example:
  ```yaml
  - tag:
      pattern: sending (\w+), viewNo: (\d+)
      attributes:
        is_message:
        message_action: send
        message_type: group 1
        view_no: group 2
  ```
  will add following attributes to messages containing 
  `sending COMMIT, viewNo: 23`:
  ```
  is_message: []
  message_action: send
  message_type: COMMIT
  view_no: 23
  ```
  It's possible for multiple tagger to write to same attribute multiple
  times, in this case it will contain all written values.

## Standard (in process_logs.yml) matchers and chains

### Matchers

TODO

### Chains

TODO

## Things to consider in future

- implement in-place subchains
- allow custom formatting per message, not per log sink
- allow implicit timelog and counter configurations
- switch from YAML configuration to python
- use some RDBMS backend to allow faster queries on logs after preprocessing
- integrate with some dedicated solution like ElasticSearch+Kibana
