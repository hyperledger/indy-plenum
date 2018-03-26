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
use some common rules defined in common `process_log.yml` (these configs
have same format). Also there are some example configs provided in 
`example_*.yml` files which could be used as a starting point.

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

### logs

Contains dictionary of output log files, each with following options:
- `filename`: output filename, possibly with `<node>` placeholder when logs
  from different nodes should be placed to different files
- `merge_nodes`: whether to merge messages from different nodes to one 
  output or not
- `pattern`: log line format, can contain the following placeholders:
  - `<timestamp>`: event timestamp
  - `<node>`: source node identifier
  - `<level>`: log level (DEBUG, INFO, WARNING, etc)
  - `<source>`: source file which emitted message
  - `<func>`: function which emitted message
  - `<user attr>`: any user-defined attribute, explained later in 
    *chains* section 

### timelogs

Contains dictionary of output event intensity plots, each with following 
options:
- `interval`: sampling interval in seconds
- `graphs`: list of graphs on plot, each being a `name: color` key-value

### counters

Contains dictionary of output event counters, each with simple option
`format` containing string, which will be output for each node when all
input logs are processed. This string can contain `<node>` placeholder,
as well as any `<subcounter>` placeholder which were used in `log count` 
commands, explained later in *chains* section 

### matchers

Contains dictionary of custom matchers, each containing a list key-value
pairs describing sub-matchers (builtin or custom) to check. Key is type 
of matcher, value is parameter. For submatchers that don't have parameters
(for example, other custom matchers) value should be omitted. Custom matcher 
matches message if any of its sub-matchers matches message.

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
- `level`: checks if message severity level is within defined limits. 
  Parameters for this matcher is either dictionary with `min` and `max` 
  values containing lower and upper bounds for message severity (`DEBUG`, 
  `INFO`, `WARNING`, etc), or just a simple identifier, which is shorthand 
  for specifying it for both `min` and `max`, for example:
  ```yaml
  - level: WARNING
  ```
  As with timestamp any of `min` or `max` can be omitted.
- `func`: checks if function that emitted message matches given name, which
  is parameter of this matcher, for example:
  ```yaml
  - func: acquire
  ```
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
- `tag`: checks if message is tagged with specified tag (tagging explained 
  later in *chains* section), for example:
  ```yaml
  - tag: NETWORK
  ```
- `attribute`: check if message has custom attribute (explained later in 
  *chains* section) with matching name and value, specified as a dictionary 
  with single key-value pair, for example:
  ```yaml
  - attribute: {viewNo: 2}
  ```
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

TODO

## Things to consider in future

- implement in-place subchains
- allow custom formatting per message, not per log sink
- allow implicit timelog and counter configurations
- switch from YAML configuration to python
- use some RDBMS backend to allow faster queries on logs after preprocessing
- integrate with some dedicated solution like ElasticSearch+Kibana
