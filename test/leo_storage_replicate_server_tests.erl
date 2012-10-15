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
-module(leo_storage_replicate_server_tests).
-author('yosuke hara').

-include("leo_storage.hrl").
-include_lib("leo_mq/include/leo_mq.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_SERVER_ID,    'replicator_0').
-define(TEST_RING_ID_1,    255).
-define(TEST_KEY_1,        "air/on/g/string/music.png").
-define(TEST_BODY_1,       <<"air-on-g-string">>).
-define(TEST_META_1, #metadata{key       = ?TEST_KEY_1,
                               addr_id   = 1,
                               clock     = 9,
                               timestamp = 8,
                               checksum  = 7}).

-define(TEST_REDUNDANCIES_1, [{Test0Node, true},
                              {Test1Node, true}]).

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

replicate_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun replicate_obj_0_/1,
                           fun replicate_obj_1_/1,
                           fun replicate_obj_2_/1
                          ]]}.

setup() ->
    meck:new(leo_logger),
    meck:expect(leo_logger, append, fun(_,_,_) ->
                                            ok
                                    end),

    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    Test0Node = list_to_atom("test_rep_0@" ++ Hostname),
    net_kernel:start([Test0Node, shortnames]),
    {ok, Test1Node} = slave:start_link(list_to_atom(Hostname), 'test_rep_1'),

    true = rpc:call(Test0Node, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(Test1Node, code, add_path, ["../deps/meck/ebin"]),

    timer:sleep(100),
    {ok, _Pid} = leo_storage_replicate_server:start_link(?TEST_SERVER_ID),
    {Test0Node, Test1Node}.

teardown({_Test0Node, Test1Node}) ->
    meck:unload(),
    net_kernel:stop(),
    slave:stop(Test1Node),

    leo_storage_replicate_server:stop(?TEST_SERVER_ID),
    ok.


%%--------------------------------------------------------------------
%% for Object
%%--------------------------------------------------------------------
%% object-replication#1
replicate_obj_0_({Test0Node, Test1Node}) ->
    gen_mock_2(object, {Test0Node, Test1Node}, ok),
    gen_mock_3(object, Test1Node, ok),

    Object = #object{key     = ?TEST_KEY_1,
                     addr_id = ?TEST_RING_ID_1,
                     dsize   = erlang:byte_size(?TEST_BODY_1),
                     data    = ?TEST_BODY_1},
    PoolPid = leo_object_storage_pool:new(Object),
    Ref     = make_ref(),

    {ok, _, {etag, _}} = leo_storage_replicate_server:replicate(
                           ?TEST_SERVER_ID, Ref, 1, ?TEST_REDUNDANCIES_1, PoolPid),
    timer:sleep(100),
    ok.

%% object-replication#2
replicate_obj_1_({Test0Node, Test1Node}) ->
    gen_mock_2(object, {Test0Node, Test1Node}, fail),
    gen_mock_3(object, Test1Node, ok),
    ?debugVal(ok),

    Object = #object{key     = ?TEST_KEY_1,
                     addr_id = ?TEST_RING_ID_1,
                     dsize   = erlang:byte_size(?TEST_BODY_1),
                     data    = ?TEST_BODY_1},
    PoolPid = leo_object_storage_pool:new(Object),

    Ref = make_ref(),
    {ok, _, {etag, _}} = leo_storage_replicate_server:replicate(
                           ?TEST_SERVER_ID, Ref, 1, ?TEST_REDUNDANCIES_1, PoolPid),
    timer:sleep(100),
    ok.

%% object-replication#3
replicate_obj_2_({Test0Node, Test1Node}) ->
    gen_mock_2(object, {Test0Node, Test1Node}, ok),
    gen_mock_3(object, Test1Node, fail),

    Object = #object{key     = ?TEST_KEY_1,
                     addr_id = ?TEST_RING_ID_1,
                     dsize   = erlang:byte_size(?TEST_BODY_1),
                     data    = ?TEST_BODY_1},
    PoolPid = leo_object_storage_pool:new(Object),
    Ref = make_ref(),
    {ok, _, {etag, _}} = leo_storage_replicate_server:replicate(
                             ?TEST_SERVER_ID, Ref, 1, ?TEST_REDUNDANCIES_1, PoolPid),
    timer:sleep(100),
    ok.


%%--------------------------------------------------------------------
%% INTERNAL-FUNCTIONS
%%--------------------------------------------------------------------
%% for object-operation #1.
gen_mock_2(object, {_Test0Node, _Test1Node}, Case) ->
    meck:new(leo_storage_mq_client),
    meck:expect(leo_storage_mq_client, publish,
                fun(Type, VNodeId, Key, _ErrorType) ->
                        ?assertEqual(?QUEUE_TYPE_REPLICATION_MISS, Type),
                        ?assertEqual(?TEST_RING_ID_1,              VNodeId),
                        ?assertEqual(?TEST_KEY_1,                  Key),
                        ok
                end),

    meck:new(leo_storage_handler_object),
    meck:expect(leo_storage_handler_object, put,
                fun(local, PoolPid, Ref) ->
                        ?assertEqual(true, erlang:is_pid(PoolPid)),
                        ?assertEqual(true, erlang:is_reference(Ref)),

                        case Case of
                            ok   -> {ok, Ref, {etag, 1}};
                            fail -> {error, Ref, []}
                        end
                end),
    ok.

%% for object-operation #2.
gen_mock_3(object, Test1Node, Case) ->
    ok = rpc:call(Test1Node, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Test1Node, meck, expect, [leo_storage_handler_object, put,
                                            fun(#object{addr_id = VNodeId,
                                                        key     = Key,
                                                        data    = Body,
                                                        del     = DelFlag}) ->
                                                    ?assertEqual(?TEST_RING_ID_1, VNodeId),
                                                    ?assertEqual(?TEST_KEY_1,     Key),
                                                    ?assertEqual(?TEST_BODY_1,    Body),
                                                    ?assertEqual(0,               DelFlag),
                                                    case Case of
                                                        ok   -> {ok, {etag, 1}};
                                                        fail -> {error, []}
                                                    end
                                            end]),
    ok.
-endif.
