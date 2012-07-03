leo_storage
===========

Overview
--------

* "leo_storage" is one of the core component of [LeoFS](https://github.com/leo-project/leofs). Main roles are described below.
  * "leo_storage" is log structure file system for Object/BLOB. Also, It includes metadata-server which is powered by [bitcask](https://github.com/basho/bitcask).
  * "leo_storage" is master-less; It has NO-SPOF.
  * LeoFS's storage-cluster consists of a set of loosely connected nodes.

*  Detail document is [here](http://www.leofs.org/docs/).

* "leo_storage" uses the "rebar" build system. Makefile so that simply running "make" at the top level should work.
  * [rebar](https://github.com/basho/rebar)
* "leo_storage" requires Erlang R14B04 or later.
