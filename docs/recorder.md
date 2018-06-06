# Recorder
-   The recorder works by recording incoming, outgoing messages, the times at which nodes connected/disconnected, times at which itself stopped and started. 
-   Each node has 2 recorders, one for each stack. The recorder has a key value database which has the time as the key and value as the message. 
-   The message has a marker indicating the type of the message. The recorder has a can be queried to return messages in the order and with correct time apart between them.
eg. if node had 3 messages at 15 sec, 20 sec and 25 sec, the recorder's `get_next` method will return the first message the first time is called but 
it will not return the next message until 5 seconds have passed; it does not block, but just returns None.
-   The recorder records messages coming into `verifyAndAppend` for each stack.

-   During the replay, recorders are combined to for a `CombinedRecorder` is used for the replay. 
-   Also during replay, the times of the `Node` are 
patched to match the actual time of the Node run. 
-   Also the information about which PRE-PREPARE sent which requests at what time are extracted 
before the replay starts so that during the replay the node sends the same PRE-PREPARES. 
-   During the replay the node, stops at appropriate intervals at which the original node stopped. 
-   Also the node calls simulates disconnections from other nodes at the appropriate time.
-   During the replay the node sends no outgoing messages neither does it have the recorder attached.
-   The replay is supposed to be run as a standalone node, most probably in an IDE so it can be debugged.
-   The record and and replay functionality is controlled through the config flag `STACK_COMPANION`.
    It has 3 possible values
    -   0 for normal operation
    -   1 for recorder
    -   2 during replay
-   The node is run with the `STACK_COMPANION` set to 1 and the `recorder` directory is created under `data/<node name>`. 
    This directory is either copied or a script in indy-node is used to copy the directories.
    The directories are given to the developer who uses a script in indy-node to replay.  