# ZeroMQ features

ZeroMQ has several unclear features that affect consumer application.
This doc describes known features/issues/limitations of ZeroMQ that were
found during our testing and observation.

**NOTE: all following info is valid for the server side when ZMQ_ROUTER socket type is used.**

## There are no ZeroMQ API calls to stop accepting new connections

It means that there is no ability to limit the number of clients connections using ZeroMQ API.
External firewall is needed to implement such limits.

## There is no ability to track and drop clients connections using ZeroMQ API

To protect the server side from file descriptors exhaustion it is needed to have the ability
to track clients connections and drop those of them that are kept for a long time.
However when using ZMQ_ROUTER socket type there is no access to particular connection.

We even created a ticket regarding this issue:

https://github.com/zeromq/libzmq/issues/2877

Guys from ZeroMQ said that external firewall is needed to implement required functionality.

Despite ZeroMQ encapsulates all work with clients connection sockets we can retrieve a new 
connection socket file descriptor from *CONNECTED* event reported by ZeroMQ monitor socket.
So that we can close this file descriptor using an ordinary close() system call after certain
time from the *CONNECTED* event.

But the problem here is that ZeroMQ does not expect that its controlled sockets may be
closed externally. Internally ZeroMQ is the epoll-based network library and closing of sockets
does not trigger any epoll events. So this invalid descriptor continues to be kept in ZeroMQ's 
epoll and DISCONNECTED event is not triggered. This leads to ZeroMQ crash with "Bad file descriptor" 
error during ZeroMQ finalizing. Moreover, seems like newly open socket with the same file descriptor 
number leads to mess in inner ZeroMQ structures associated with externally closed file descriptor 
as they were not finalized.

More info here:

https://jira.hyperledger.org/browse/INDY-1417?focusedCommentId=46595&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-46595

## ZeroMQ creates a separate queue for each client connection

And also these inner queues are not cleaned if corresponding connection socket has been closed
from the client side (for example by time out), such queue is kept in memory until the last 
packet is read.

## ZeroMQ handles all inner queues associated with clients connections in Round-Robin manner

ZeroMQ reads from these queues in a Round-Robin manner when recv() for listener is called,
so these queues may be kept in memory for a long time in case of big number of connections
(note our receive quotas for client stack).

## You can not monitor ZeroMQ inner queues for incoming/outgoing messages

You can not:
* get the current/max length of the queue
* get the min/avg/max time that messages are kept in the queue
* get the number of drops from the queue (details in ZMQ_HWM section below)

Just weird!

## Do not rely on ZeroMQ high watermark

From the official ZeroMQ doc:

*The high water mark is a hard limit on the maximum number of outstanding messages ØMQ shall 
queue in memory for any single peer that the specified socket is communicating with.*

*If this limit has been reached the socket shall enter an exceptional state and depending on 
the socket type, ØMQ shall take appropriate action such as blocking or dropping sent messages.*

But setting the high watermark to *N* for the listener socket does not guarantee that
the server will receive *N* messages from the client and no more even if received messages are 
not read from the queue. Despite the official doc tells that for ZMQ_ROUTER socket type 
the ZMQ_HWM option action is drop, it is proven by our tests that actually messages are not dropped.

There is corresponding issue on github, and there is an answer by the member of ZMQ project which
tells that "HWM in zmq is pretty complicated, and can't really be used to control flow in a fine-grained way.": 

https://github.com/zeromq/pyzmq/issues/1100#issuecomment-339591427

Seems like its behaviour depends on messages sizes, but anyway it is not a strict limit you can rely on.
