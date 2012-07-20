%%======================================================================
%%
%% LeoFS Storage
%%
%% Copyright (c) 2012
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------
%% LeoFS Storage - EUnit
%% @doc
%% @end
%%======================================================================
-module(leo_storage_handler_object_tests).

-author('Yosuke Hara').
-vsn('0.9.1').

-include("leo_storage.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(TEST_DIR_0,  "air/on/g/").
-define(TEST_KEY_0,  "air/on/g/string").
-define(TEST_KEY_1,  "air/on/g/bach/music").
-define(TEST_BIN,    <<"V">>).
-define(TEST_META_0, #metadata{key   = ?TEST_KEY_0,
                               dsize = byte_size(?TEST_BIN)}).
-define(TEST_META_1, #metadata{key   = ?TEST_KEY_0,
                               dsize = byte_size(?TEST_BIN),
                               del   = 1
                              }).


object_handler_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun get_a0_/1,
                           fun get_a1_/1,
                           fun get_b0_/1,
                           fun get_b1_/1,
                           fun get_b2_/1,
                           fun get_b3_/1,
                           fun get_c0_/1,
                           fun get_d0_/1,
                           fun put_0_/1,
                           fun put_1_/1,
                           fun put_2_/1,
                           fun put_3_/1,
                           fun delete_0_/1,
                           fun delete_1_/1,
                           fun delete_2_/1,

                           fun head_/1,
                           fun copy_/1,
                           fun prefix_search_/1
                          ]]}.

setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    Node0 = list_to_atom("node_0@" ++ Hostname),
    net_kernel:start([Node0, shortnames]),
    {ok, Node1} = slave:start_link(list_to_atom(Hostname), 'node_1'),

    true = rpc:call(Node0, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(Node1, code, add_path, ["../deps/meck/ebin"]),

    meck:new(leo_logger_api),
    meck:expect(leo_logger_api, new,          fun(_,_,_) -> ok end),
    meck:expect(leo_logger_api, new,          fun(_,_,_,_,_) -> ok end),
    meck:expect(leo_logger_api, new,          fun(_,_,_,_,_,_) -> ok end),
    meck:expect(leo_logger_api, add_appender, fun(_,_) -> ok end),
    meck:expect(leo_logger_api, append,       fun(_,_) -> ok end),
    meck:expect(leo_logger_api, append,       fun(_,_,_) -> ok end),
    {Node0, Node1}.

teardown({_, Node1}) ->
    meck:unload(),
    net_kernel:stop(),
    slave:stop(Node1),
    timer:sleep(100),
    ok.

%%--------------------------------------------------------------------
%% GET
%%--------------------------------------------------------------------
%% @doc  get/1
%% @private
get_a0_({Node0, Node1}) ->
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(get, _Key) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, get,
                fun(_KeyBin, _StartPos, _EndPos) ->
                        {ok, ?TEST_META_0, []}
                end),

    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, get,
                fun(_) ->
                        not_found
                end),

    meck:new(leo_statistics_req_counter),
    meck:expect(leo_statistics_req_counter, increment,
                fun(_) -> ok end),

    Res = leo_storage_handler_object:get({make_ref(), ?TEST_KEY_0}),
    ?assertEqual({error,not_found}, Res),
    ok.

%% @doc  get/1
%% @private
get_a1_({Node0, Node1}) ->
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, get,
                fun(_KeyBin, _StartPos, _EndPos) ->
                        {ok, ?TEST_META_0, []}
                end),

    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, get,
                fun(_) ->
                        #object{data = ?TEST_BIN}
                end),
    meck:expect(leo_object_storage_pool, destroy,
                fun(_) ->
                        ok
                end),

    Res = leo_storage_handler_object:get({make_ref(), ?TEST_KEY_0}),
    ?assertEqual({ok, ?TEST_META_0, ?TEST_BIN}, Res),
    ok.


%% @doc  get/3
%% @private
get_b0_({Node0, Node1}) ->
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, get,
                fun(_KeyBin, _StartPos, _EndPos) ->
                        not_found
                end),

    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, get,
                fun(_) ->
                        not_found
                end),

    Res = leo_storage_handler_object:get(0, ?TEST_KEY_0, 0),
    ?assertEqual({error,not_found}, Res),
    ok.


%% @doc  get/3
%% @private
get_b1_({Node0, Node1}) ->
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}], %% two
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, get,
                fun(_KeyBin, _StartPos, _EndPos) ->
                        {ok, ?TEST_META_0, []}
                end),

    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, get,
                fun(_) ->
                        not_found
                end),

    meck:new(leo_storage_read_repair_server),
    meck:expect(leo_storage_read_repair_server, repair,
                fun(_Pid, Ref, _R, _Node, _Metadata, _ReqId) ->
                        {ok, Ref}
                end),

    Res = leo_storage_handler_object:get(0, ?TEST_KEY_0, 0),
    ?assertEqual({error,not_found}, Res),

    His = meck:history(leo_storage_read_repair_server),
    ?assertEqual(1, length(His)),
    ok.

%% @doc  get/3
%% @private
get_b2_({Node0, _Node1}) ->
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}], %% one
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, get,
                fun(_KeyBin, _StartPos, _EndPos) ->
                        {ok, ?TEST_META_0, []}
                end),

    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, get,
                fun(_) ->
                        not_found
                end),

    meck:new(leo_storage_read_repair_server),
    meck:expect(leo_storage_read_repair_server, repair,
                fun(_Pid, Ref, _R, _Node, _Metadata, _ReqId) ->
                        {ok, Ref}
                end),

    Res = leo_storage_handler_object:get(0, ?TEST_KEY_0, 0),
    ?assertEqual({error,not_found}, Res),

    His = meck:history(leo_storage_read_repair_server),
    ?assertEqual(0, length(His)),
    ok.

%% @doc  get/3
%% @private
get_b3_({Node0, Node1}) ->
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}], %% two
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, get,
                fun(_KeyBin, _StartPos, _EndPos) ->
                        {ok, ?TEST_META_0, []}
                end),

    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, get,
                fun(_) ->
                        #object{data = ?TEST_BIN}
                end),
    meck:expect(leo_object_storage_pool, destroy,
                fun(_) ->
                        ok
                end),

    meck:new(leo_storage_read_repair_server),
    meck:expect(leo_storage_read_repair_server, repair,
                fun(_Pid, Ref, _R, _Node, _Metadata, _ReqId) ->
                        {ok, Ref}
                end),

    Res = leo_storage_handler_object:get(0, ?TEST_KEY_0, 0),
    ?assertEqual({ok, ?TEST_META_0, ?TEST_BIN}, Res),

    His = meck:history(leo_storage_read_repair_server),
    ?assertEqual(1, length(His)),
    ok.


%% @doc  get/4
%% @private
get_c0_({Node0, Node1}) ->
    Checksum0 = 12345,
    Checksum1 = 12340,

    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, get,
                fun(_KeyBin, _StartPos, _EndPos) ->
                        not_found
                end),
    meck:expect(leo_object_storage_api, head,
                fun(_KeyBin) ->
                        {ok, term_to_binary(#metadata{checksum = Checksum0})}
                end),

    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, get,
                fun(_) ->
                        not_found
                end),

    Res0 = leo_storage_handler_object:get(0, ?TEST_KEY_0, Checksum0, 0),
    ?assertEqual({ok,match}, Res0),

    Res1 = leo_storage_handler_object:get(0, ?TEST_KEY_0, Checksum1, 0),
    ?assertEqual({error,not_found}, Res1),
    ok.

get_d0_(_) ->
    %% case of "start_pos > end_pos"
    {error, badarg} = leo_storage_handler_object:get(0, ?TEST_KEY_0, 3, 1, 0),
    ok.



%%--------------------------------------------------------------------
%% PUT
%%--------------------------------------------------------------------
%% put/6
put_0_({Node0, Node1}) ->
    AddrId    = 0,
    Key       = ?TEST_KEY_0,
    Bin       = ?TEST_BIN,
    Size      = byte_size(?TEST_BIN),
    ReqId     = 0,
    Timestamp = 0,

    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, new,
                fun(_) ->
                        []
                end),
    meck:expect(leo_object_storage_pool, get,
                fun(_) ->
                        #object{data = ?TEST_BIN}
                end),
    meck:expect(leo_object_storage_pool, set_ring_hash,
                fun(_, _) ->
                        ok
                end),
    meck:expect(leo_object_storage_pool, destroy,
                fun(_) ->
                        ok
                end),

    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(put, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    meck:new(leo_storage_replicate_server),
    meck:expect(leo_storage_replicate_server, replicate,
                fun(_PId, Ref, _Quorum, _Redundancies, _ObjectPool) ->
                        {ok, Ref}
                end),

    Res = leo_storage_handler_object:put(AddrId, Key, Bin, Size, ReqId, Timestamp),
    ?assertEqual(ok, Res),
    ok.

%% put/2
put_1_({_Node0, _Node1}) ->
    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, head,
                fun(_) ->
                        not_found
                end),

    Ref = make_ref(),
    Res = leo_storage_handler_object:put([], Ref),
    ?assertEqual({error, Ref, timeout}, Res),
    ok.

%% put/2
put_2_({_Node0, _Node1}) ->
    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, head,
                fun(_) ->
                        #metadata{key = ?TEST_KEY_0, addr_id = 0}
                end),

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, put,
                fun(_KeyBin, _ObjPool) ->
                        ok
                end),

    Ref = make_ref(),
    Res = leo_storage_handler_object:put([], Ref),
    ?assertEqual({ok, Ref}, Res),
    ok.

%% put/1
put_3_(_) ->
    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, new,
                fun(_) ->
                        []
                end),
    meck:expect(leo_object_storage_pool, destroy,
                fun(_) ->
                        ok
                end),
    meck:expect(leo_object_storage_pool, head,
                fun(_) ->
                        not_found
                end),

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, head,
                fun(_KeyBin) ->
                        {ok, term_to_binary(#metadata{checksum = 12345})}
                end),

    Object = #object{key = ?TEST_KEY_0},
    Res = leo_storage_handler_object:put(Object),
    ?assertEqual({error,timeout}, Res),
    ok.


%%--------------------------------------------------------------------
%% DELETE
%%--------------------------------------------------------------------
%% delete/4
delete_0_({Node0, Node1}) ->
    AddrId    = 0,
    Key       = ?TEST_KEY_0,
    ReqId     = 0,
    Timestamp = 0,

    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, new,
                fun(_) ->
                        []
                end),
    meck:expect(leo_object_storage_pool, set_ring_hash,
                fun(_, _) ->
                        ok
                end),
    meck:expect(leo_object_storage_pool, destroy,
                fun(_) ->
                        ok
                end),

    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(put, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    meck:new(leo_storage_replicate_server),
    meck:expect(leo_storage_replicate_server, replicate,
                fun(_PId, Ref, _Quorum, _Redundancies, _ObjectPool) ->
                        {ok, Ref}
                end),

    Res = leo_storage_handler_object:delete(AddrId, Key, ReqId, Timestamp),
    ?assertEqual(ok, Res),
    ok.

%% delete/4
delete_1_({_Node0, _Node1}) ->
    Clock = 1,
    Checksum = 5,

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, head,
                fun(_KeyBin) ->
                        {ok, #metadata{checksum = Checksum,
                                       clock    = Clock}}
                end),

    Res = leo_storage_handler_object:delete(#object{key      = ?TEST_KEY_0,
                                                      addr_id  = 0,
                                                      checksum = Checksum,
                                                      clock    = Clock}),
    ?assertEqual({ok, node()}, Res),
    ok.

%% delete/1
delete_2_({_Node0, _Node1}) ->
    Clock0    = 1, Clock1    = 3,
    Checksum0 = 5, Checksum1 = 7,

    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, head,
                fun(_KeyBin) ->
                        {ok, #metadata{checksum = Checksum0,
                                       clock    = Clock0}}
                end),
    meck:expect(leo_object_storage_api, delete,
                fun(_KeyBin, _ObjPool) ->
                        ok
                end),

    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, new,
                fun(_) ->
                        []
                end),
    meck:expect(leo_object_storage_pool, get,
                fun(_) ->
                        #object{addr_id = 0,
                                key = ?TEST_KEY_0}
                end),
    meck:expect(leo_object_storage_pool, destroy,
                fun(_) ->
                        ok
                end),

    Res = leo_storage_handler_object:delete(#object{key      = ?TEST_KEY_0,
                                                      addr_id  = 0,
                                                      checksum = Checksum1,
                                                      clock    = Clock1}),
    ?assertEqual(ok, Res),
    ok.


%%--------------------------------------------------------------------
%% OTHER
%%--------------------------------------------------------------------
head_({_Node0, _Node1}) ->
    %% 1.
    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, head,
                fun(_KeyBin) ->
                        {ok, term_to_binary(?TEST_META_0)}
                end),
    Res0 = leo_storage_handler_object:head(0, ?TEST_KEY_0),
    ?assertEqual({ok, ?TEST_META_0}, Res0),

    %% 2.
    meck:unload(),
    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, head,
                fun(_KeyBin) ->
                        not_found
                end),
    Res1 = leo_storage_handler_object:head(0, ?TEST_KEY_0),
    ?assertEqual({error, not_found}, Res1),

    %% 2.
    meck:unload(),
    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, head,
                fun(_KeyBin) ->
                        {error, []}
                end),
    Res2 = leo_storage_handler_object:head(0, ?TEST_KEY_0),
    ?assertEqual({error, []}, Res2),
    ok.


copy_({Node0, Node1}) ->
    %% 1. for WRITE
    %%
    %% Retrieve metadata from head-func
    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, head,
                fun(_KeyBin) ->
                        {ok, term_to_binary(?TEST_META_0)}
                end),
    meck:expect(leo_object_storage_api, get,
                fun(_KeyBin, _StartPos, _EndPos) ->
                        {ok, ?TEST_META_0, []}
                end),

    %% Retrieve object from get-func
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),


    %% object-pool
    meck:new(leo_object_storage_pool),
    meck:expect(leo_object_storage_pool, new,
                fun(_) ->
                        []
                end),
    meck:expect(leo_object_storage_pool, get,
                fun(_) ->
                        #object{data = ?TEST_BIN}
                end),
    meck:expect(leo_object_storage_pool, destroy,
                fun(_) ->
                        ok
                end),

    %% replicator
    meck:new(leo_storage_replicate_server),
    meck:expect(leo_storage_replicate_server, replicate,
                fun(_PId, Ref, _Quorum, _Redundancies, _ObjectPool) ->
                        {ok, Ref}
                end),

    Res1 = leo_storage_handler_object:copy([Node1, Node1], 0, ?TEST_KEY_0),
    ?assertEqual(ok, Res1),

    %% 2. for DELETE
    %%
    meck:unload(leo_object_storage_api),
    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, head,
                fun(_KeyBin) ->
                        {ok, term_to_binary(?TEST_META_1)}
                end),
    meck:expect(leo_object_storage_api, get,
                fun(_KeyBin, _StartPos, _EndPos) ->
                        {ok, ?TEST_META_1, []}
                end),

    Res2 = leo_storage_handler_object:copy([Node1, Node1], 0, ?TEST_KEY_0),
    ?assertEqual(ok, Res2),
    ok.


prefix_search_({_Node0, _Node1}) ->
    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, fetch_by_key,
                fun(_ParentDir, Fun) ->
                        Fun(term_to_binary({0, ?TEST_KEY_0}), term_to_binary(#metadata{}), []),
                        Fun(term_to_binary({0, ?TEST_DIR_0}), term_to_binary(#metadata{}), [#metadata{key=?TEST_KEY_0}]),
                        Fun(term_to_binary({0, ?TEST_KEY_1}), term_to_binary(#metadata{}), [#metadata{key=?TEST_KEY_0}])
                end),

    Res = leo_storage_handler_object:prefix_search(?TEST_DIR_0),
    ?assertEqual(2, length(Res)),
    ok.

-endif.

