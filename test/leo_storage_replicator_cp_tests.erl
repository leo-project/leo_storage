%%====================================================================
%%
%% LeoFS Storage
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%%====================================================================
-module(leo_storage_replicator_cp_tests).

-include("leo_storage.hrl").
-include_lib("leo_mq/include/leo_mq.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_SERVER_ID, 'replicator_0').
-define(TEST_RING_ID_1, 255).
-define(TEST_KEY_1, <<"air/on/g/string/music.png">>).
-define(TEST_BODY_1, <<"air-on-g-string">>).
-define(TEST_META_1, #?METADATA{key = ?TEST_KEY_1,
                                addr_id = 1,
                                clock = 9,
                                timestamp = 8,
                                checksum = 7}).

-define(TEST_REDUNDANCIES_1, [#redundant_node{node = TestNode_1, available = true},
                              #redundant_node{node = TestNode_2, available = true}]).

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

replicate_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun replicate_obj_1_/1,
                           fun replicate_obj_2_/1,
                           fun replicate_obj_3_/1
                          ]]}.

setup() ->
    meck:new(leo_logger, [non_strict]),
    meck:expect(leo_logger, append, fun(_,_,_) ->
                                            ok
                                    end),

    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    TestNode_1 = list_to_atom("test_rep_0@" ++ Hostname),
    net_kernel:start([TestNode_1, shortnames]),
    {ok, TestNode_2} = slave:start_link(list_to_atom(Hostname), 'test_rep_1'),

    true = rpc:call(TestNode_1, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(TestNode_2, code, add_path, ["../deps/meck/ebin"]),

    timer:sleep(100),
    {TestNode_1, TestNode_2}.

teardown({_TestNode_1, TestNode_2}) ->
    meck:unload(),
    net_kernel:stop(),
    slave:stop(TestNode_2),
    ok.


%%--------------------------------------------------------------------
%% for Object
%%--------------------------------------------------------------------
%% object-replication#1
replicate_obj_1_({TestNode_1, TestNode_2}) ->
    gen_mock_1(ok),
    gen_mock_2(TestNode_2, ok),

    Object = #?OBJECT{key = ?TEST_KEY_1,
                      addr_id = ?TEST_RING_ID_1,
                      dsize = erlang:byte_size(?TEST_BODY_1),
                      data = ?TEST_BODY_1},

    F = fun({ok, _Method, ETag}) ->
                {ok, ETag};
           ({error, Cause}) ->
                {error, Cause}
        end,
    {ok, _} = leo_storage_replicator_cp:replicate(
                put, 1, ?TEST_REDUNDANCIES_1, Object, F),
    timer:sleep(100),
    ok.

%% object-replication#2
replicate_obj_2_({TestNode_1, TestNode_2}) ->
    gen_mock_1(fail),
    gen_mock_2(TestNode_2, ok),

    Object = #?OBJECT{key = ?TEST_KEY_1,
                      addr_id = ?TEST_RING_ID_1,
                      dsize = erlang:byte_size(?TEST_BODY_1),
                      data  = ?TEST_BODY_1},

    F = fun({ok, _Method, ETag}) ->
                {ok, ETag};
           ({error, Cause}) ->
                {error, Cause}
        end,
    %% {ok, {etag, _}} =
    Res = leo_storage_replicator_cp:replicate(
            put, 1, ?TEST_REDUNDANCIES_1, Object, F),
    ?assertEqual({ok, 1}, Res),
    timer:sleep(100),
    ok.

%% object-replication#3
replicate_obj_3_({TestNode_1, TestNode_2}) ->
    gen_mock_1(ok),
    gen_mock_2(TestNode_2, fail),

    Object = #?OBJECT{key = ?TEST_KEY_1,
                      addr_id = ?TEST_RING_ID_1,
                      dsize = erlang:byte_size(?TEST_BODY_1),
                      data = ?TEST_BODY_1},

    F = fun({ok, _Method, ETag}) ->
                {ok, ETag};
           ({error, Cause}) ->
                {error, Cause}
        end,
    {ok, {etag, _}} = leo_storage_replicator_cp:replicate(
                        put, 1, ?TEST_REDUNDANCIES_1, Object, F),
    timer:sleep(100),
    ok.


%%--------------------------------------------------------------------
%% INTERNAL-FUNCTIONS
%%--------------------------------------------------------------------
%% for object-operation #1.
gen_mock_1(Case) ->
    meck:new(leo_storage_mq, [non_strict]),
    meck:expect(leo_storage_mq, publish,
                fun(Type, VNodeId, Key, _ErrorType) ->
                        ?assertEqual(?QUEUE_ID_PER_OBJECT, Type),
                        ?assertEqual(?TEST_RING_ID_1, VNodeId),
                        ?assertEqual(?TEST_KEY_1, Key),
                        ok
                end),

    meck:new(leo_storage_handler_object, [non_strict]),
    meck:expect(leo_storage_handler_object, put,
                fun({_Object, Ref}) ->
                        ?assertEqual(true, erlang:is_reference(Ref)),

                        case Case of
                            ok ->
                                {ok, Ref, {etag, 1}};
                            fail ->
                                {error, Ref, []}
                        end
                end),
    meck:expect(leo_storage_handler_object, put,
                fun(Ref, From, _Object, _ReqId) ->
                        case Case of
                            ok ->
                                erlang:send(From, {Ref, {ok, 1}});
                            fail ->
                                erlang:send(From, {Ref, {error, {node(), []}}})
                        end
                end),
    ok.

%% for object-operation #2.
gen_mock_2(TestNode_2, Case) ->
    ok = rpc:call(TestNode_2, meck, new,
                  [leo_storage_handler_object, [no_link, non_strict]]),
    ok = rpc:call(TestNode_2, meck, expect,
                  [leo_storage_handler_object, put,
                   fun(#?OBJECT{addr_id = VNodeId,
                                key = Key,
                                data = Body,
                                del = DelFlag}) ->
                           ?assertEqual(?TEST_RING_ID_1, VNodeId),
                           ?assertEqual(?TEST_KEY_1, Key),
                           ?assertEqual(?TEST_BODY_1, Body),
                           ?assertEqual(0, DelFlag),
                           case Case of
                               ok ->
                                   {ok, {etag, 1}};
                               fail ->
                                   {error, []}
                           end
                   end]),
    ok = rpc:call(TestNode_2, meck, expect,
                  [leo_storage_handler_object, put,
                   fun(Ref, From,_Object,_ReqId) ->
                           case Case of
                               ok ->
                                   erlang:send(From, {Ref, {ok, 1}});
                               fail ->
                                   erlang:send(From, {Ref, {error, {node(),[]}}})
                           end
                   end]),
    ok.
-endif.
