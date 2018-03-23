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


## Things to consider in future

- switch from YAML configuration to python
- allow custom formatting per message, not per log sink
- allow implicit timelog and counter configurations
- use some RDBMS backend to allow faster queries on logs after preprocessing
- integrate with some dedicated solution like ElasticSearch+Kibana
