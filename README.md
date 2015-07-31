# leo_storage

[![Build Status](https://secure.travis-ci.org/leo-project/leo_storage.png?branch=develop)](http://travis-ci.org/leo-project/leo_storage)

## Overview

* "leo_storage" is one of the core components of [LeoFS](https://github.com/leo-project/leofs). Main roles are described below.
  * "leo_storage" is log structure file system for Object/BLOB. Also, It includes metadata-server which is powered by [eleveldb](https://github.com/basho/eleveldb).
  * "leo_storage" is master-less; It has NO-SPOF.
  * LeoFS's storage-cluster consists of a set of loosely connected nodes.
*  Detail document is [here](http://leo-project.net/leofs/docs/).
* "leo_storage" uses [rebar](https://github.com/rebar/rebar) build system. Makefile so that simply running "make" at the top level should work.
* "leo_storage" requires Erlang R16B03-1 or later.

## Sponsors

LeoProject/LeoFS is sponsored by [Rakuten, Inc.](http://global.rakuten.com/corp/) and suppoerted by [Rakuten Institute of Technology](http://rit.rakuten.co.jp/).
