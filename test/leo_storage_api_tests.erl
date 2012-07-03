%%====================================================================
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
%% -------------------------------------------------------------------
%% LeoFS Storage - EUnit
%% @doc
%% @end
%%====================================================================
-module(leo_storage_api_tests).
-author('yosuke hara').
-vsn('0.9.0').

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

api_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun register_in_monitor_/1,
                           fun get_routing_table_chksum_/1,
                           fun synchronize_/1,
                           fun start_/1,
                           fun stop_/1,
                           fun attach_/1,
                           fun compact_/1,
                           fun get_cluster_node_status_/1,
                           fun rebalance_/1
                          ]]}.

setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    Node0 = list_to_atom("node_0@" ++ Hostname),
    net_kernel:start([Node0, shortnames]),
    {ok, Node1} = slave:start_link(list_to_atom(Hostname), 'manager_0'),

    meck:new(leo_logger_api),
    meck:expect(leo_logger_api, new,          fun(_,_,_) -> ok end),
    meck:expect(leo_logger_api, new,          fun(_,_,_,_,_) -> ok end),
    meck:expect(leo_logger_api, new,          fun(_,_,_,_,_,_) -> ok end),
    meck:expect(leo_logger_api, add_appender, fun(_,_) -> ok end),
    meck:expect(leo_logger_api, append,       fun(_,_) -> ok end),
    meck:expect(leo_logger_api, append,       fun(_,_,_) -> ok end),
    [Node0, Node1].

teardown([_, Node1]) ->
    net_kernel:stop(),
    slave:stop(Node1),
    meck:unload(),
    ok.


register_in_monitor_([_Node0, Node1]) ->
    %% 1.
    Res0 = leo_storage_api:register_in_monitor(first),
    ?assertEqual({error, not_found}, Res0),

    %% 2.
    ok = rpc:call(Node1, meck, new,    [leo_manager_api, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_manager_api, register,
                                        fun(_RequestedTimes, _Pid, _Node, storage) ->
                                                ok
                                        end]),

    true = register('leo_storage_sup', leo_hashtable:new()),
    Res1 = leo_storage_api:register_in_monitor(first),
    ?assertEqual(ok, Res1),

    %% 3.
    ok = rpc:call(Node1, meck, unload, [leo_manager_api]),
    ok = rpc:call(Node1, meck, new,    [leo_manager_api, [no_link]]),

    Res2 = leo_storage_api:register_in_monitor(first),
    ?assertEqual({error, ?ERROR_COULD_NOT_CONNECT}, Res2),

    meck:unload(),
    ok.

get_routing_table_chksum_(_) ->
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, checksum,
                fun(ring) ->
                        {ok, {1234, 5678}}
                end),

    Res = leo_storage_api:get_routing_table_chksum(),
    ?assertEqual({ok, {1234, 5678}}, Res),
    meck:unload(),
    ok.

start_([Node0, _]) ->
    %% 1.
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, create,
                fun(Members) ->
                        {ok, Members, [{?CHECKSUM_RING,   {1234, 5678}},
                                       {?CHECKSUM_MEMBER, 1234567890}]}
                end),

    {ok, {Node, Chksums}} = leo_storage_api:start([]),
    ?assertEqual(Node, Node0),
    ?assertEqual({1234, 5678}, Chksums),
    meck:unload(),

    %% 2.
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, create,
                fun(_Members) ->
                        {error , []}
                end),

    {error, {Node, Cause}} = leo_storage_api:start([]),
    ?assertEqual(Node, Node0),
    ?assertEqual([], Cause),
    meck:unload(),
    ok.

stop_(_) ->
    ok.

attach_(_) ->
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, set_options,
                fun(_SystemConf) ->
                        ok
                end),

    ok = leo_storage_api:attach(#system_conf{n = 3,
                                               r = 1,
                                               w = 2,
                                               d = 2,
                                               bit_of_ring = 128}),
    Res = meck:history(leo_redundant_manager_api),
    ?assertEqual(1, length(Res)),

    meck:unload(),
    ok.

synchronize_([Node0, _]) ->
    meck:new(leo_storage_handler_object),
    meck:expect(leo_storage_handler_object, copy,
                fun(_Nodes, _AddrId, _Key) ->
                        ok
                end),

    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(_Key) ->
                        {ok, #redundancies{vnode_id = 12345}}
                end),

    meck:new(leo_storage_mq_client),
    meck:expect(leo_storage_mq_client, publish,
                fun(_Q, _VNodeId, _Key, _ErrorType) ->
                        ok
                end),
    meck:expect(leo_storage_mq_client, publish,
                fun(_Q, _VNodeId, _Node) ->
                        ok
                end),

    %% 1.
    Key = "air/on/g/string",
    ok = leo_storage_api:synchronize(?TYPE_OBJ, [Node0], #metadata{addr_id = 0,
                                                                     key = Key}),
    Res0 = meck:history(leo_storage_handler_object),
    ?assertEqual(1, length(Res0)),

    %% 2.
    ok = leo_storage_api:synchronize(?TYPE_OBJ, Key, []),
    Res1 = meck:history(leo_storage_mq_client),
    ?assertEqual(1, length(Res1)),

    %% 3.
    ok = leo_storage_api:synchronize(sync_by_vnode_id, 0, Node0),
    Res2 = meck:history(leo_storage_mq_client),
    ?assertEqual(2, length(Res2)),

    meck:unload(),
    ok.


compact_(_) ->
    meck:new(leo_object_storage_api),
    meck:expect(leo_object_storage_api, compact,
                fun() ->
                        ok
                end),
    ok = leo_storage_api:compact(),
    Res = meck:history(leo_object_storage_api),
    ?assertEqual(1, length(Res)),

    meck:unload(),
    ok.

get_cluster_node_status_(_) ->
    application:start(mnesia),

    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, checksum,
                fun(ring) ->
                        {ok, {1234, 5678}}
                end),

    {ok, ClusterStatus} = leo_storage_api:get_cluster_node_status(),
    ?assertEqual(true, is_record(ClusterStatus, cluster_node_status)),

    Res = meck:history(leo_redundant_manager_api),
    ?assertEqual(1, length(Res)),

    meck:unload(),
    application:stop(mnesia),
    ok.

rebalance_([Node0, Node1]) ->
    meck:new(leo_storage_mq_client),
    meck:expect(leo_storage_mq_client, publish,
                fun(_Q, _VNodeId, _Node) ->
                        ok
                end),

    ok = leo_storage_api:rebalance([{0,   Node0},
                                      {255, Node1}]),
    Res = meck:history(leo_storage_mq_client),
    ?assertEqual(2, length(Res)),

    meck:unload(),
    ok.

-endif.

