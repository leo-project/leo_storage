leo_storage
===========

[![Build Status](https://secure.travis-ci.org/leo-project/leo_storage.png?branch=master)](http://travis-ci.org/leo-project/leo_storage)

Overview
--------

* "leo_storage" is one of the core component of [LeoFS](https://github.com/leo-project/leofs). Main roles are described below.
  * "leo_storage" is log structure file system for Object/BLOB. Also, It includes metadata-server which is powered by [bitcask](https://github.com/basho/bitcask).
  * "leo_storage" is master-less; It has NO-SPOF.
  * LeoFS's storage-cluster consists of a set of loosely connected nodes.
*  Detail document is [here](http://www.leofs.org/docs/).
* "leo_storage" uses [rebar](https://github.com/basho/rebar) build system. Makefile so that simply running "make" at the top level should work.
* "leo_storage" requires Erlang R15B03-1 or later.