Eventing
========
This is the Couchbase Eventing engine. It is intended to allow associating user code with any event that occurs
inside Couchbase Server. An event is a change in state of any element of the server. Initial focus is on data
state changes, but future versions will bring ability to handle non-data event.

Eventing is a MDS enabled service, and will run user supplied code on nodes designated with Eventing role. The
role supports linear scalability and online rebalance. The consistency model is same as the GSI model, which is
ability to do unbounded consistency, and at-or-after consistency.

We use [Google V8](https://developers.google.com/v8/) to run user supplied Javascript code. we do not support
full Javascript syntax, because the programming model we offer needs to automatically parallelize on multiple
nodes to handle the volume of events. For example, no global variables are accessible in event handlers.

We add a number of extensions to Javascript to make it easy to work with Couchbase. For example, Couchbase
Buckets appear as javascript maps, N1QL results can be iterated over using javascript iterators and a number
of added functions allow event handlers to send messages, raise more events etc.

