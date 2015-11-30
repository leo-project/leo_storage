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
-module(leo_storage_replicator_ec_tests).

-include("leo_storage.hrl").
-include_lib("leo_mq/include/leo_mq.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_SERVER_ID, 'replicator_ec').
-define(TEST_RING_ID_1, 255).
-define(TEST_KEY_1, <<"air/on/g/string/music.png">>).
-define(TEST_BODY_1, <<"air-on-g-string">>).
-define(TEST_META_1, #?METADATA{key = ?TEST_KEY_1,
                                addr_id = 1,
                                clock = 9,
                                timestamp = 8,
                                checksum = 7}).

-define(TEST_REDUNDANCIES_1, [#redundant_node{node = TestNode_1, available = true},
                              #redundant_node{node = TestNode_2, available = true},
                              #redundant_node{node = TestNode_2, available = true},
                              #redundant_node{node = TestNode_1, available = true},
                              #redundant_node{node = TestNode_1, available = true},
                              #redundant_node{node = TestNode_1, available = true}
                             ]).

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

replicate_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun replicate_obj_1_/1
                           %% fun replicate_obj_2_/1,
                           %% fun replicate_obj_3_/1
                          ]]}.

setup() ->
    meck:new(leo_logger, [non_strict]),
    meck:expect(leo_logger, append, fun(_,_,_) ->
                                            ok
                                    end),

    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    TestNode_1 = list_to_atom("testnode_11@" ++ Hostname),
    net_kernel:start([TestNode_1, shortnames]),
    {ok, TestNode_2} = slave:start_link(list_to_atom(Hostname), 'testnode_12'),

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
    gen_mock_2([TestNode_1, TestNode_2], ok),

    CodingParam_K = 4,
    CodingParam_M = 2,
    TotalReplicas = CodingParam_K + CodingParam_M,
    Fragments = lists:map(
                  fun(Index) ->
                          FIdBin = list_to_binary(integer_to_list(Index)),
                          #?OBJECT{key = << ?TEST_KEY_1/binary, "\n", FIdBin/binary >>,
                                   addr_id = ?TEST_RING_ID_1,
                                   data = ?TEST_BODY_1,
                                   cindex = Index,
                                   csize = erlang:byte_size(?TEST_BODY_1)
                                  }
                  end, lists:seq(1, TotalReplicas)),

    F = fun({ok, _Method, ETag}) ->
                {ok, ETag};
           ({error, Cause}) ->
                {error, Cause}
        end,

    Quorum = ?quorum_of_fragments(
                ?CMD_PUT, 2, 1, 1,
                CodingParam_M, TotalReplicas),
    ?debugVal(Quorum),

    {ok, _} = leo_storage_replicator_ec:replicate(
                put, Quorum, ?TEST_REDUNDANCIES_1, Fragments, F),
    timer:sleep(100),
    ok.

%% %% object-replication#2
%% replicate_obj_2_({TestNode_1, TestNode_2}) ->
%%     gen_mock_1({TestNode_1, TestNode_2}, fail),
%%     gen_mock_2(TestNode_2, ok),
%%
%%     Object = #?OBJECT{key = ?TEST_KEY_1,
%%                       addr_id = ?TEST_RING_ID_1,
%%                       dsize = erlang:byte_size(?TEST_BODY_1),
%%                       data = ?TEST_BODY_1},
%%
%%     F = fun({ok, _Method, ETag}) ->
%%                 {ok, ETag};
%%            ({error, Cause}) ->
%%                 {error, Cause}
%%         end,
%%     %% {ok, {etag, _}} =
%%     Res = leo_storage_replicator_cp:replicate(
%%             put, 1, ?TEST_REDUNDANCIES_1, Object, F),
%%     ?assertEqual({ok, 1}, Res),
%%     timer:sleep(100),
%%     ok.

%% %% object-replication#3
%% replicate_obj_3_({TestNode_1, TestNode_2}) ->
%%     gen_mock_1({TestNode_1, TestNode_2}, ok),
%%     gen_mock_2(TestNode_2, fail),
%%
%%     Object = #?OBJECT{key = ?TEST_KEY_1,
%%                       addr_id = ?TEST_RING_ID_1,
%%                       dsize = erlang:byte_size(?TEST_BODY_1),
%%                       data = ?TEST_BODY_1},
%%
%%     F = fun({ok, _Method, ETag}) ->
%%                 {ok, ETag};
%%            ({error, Cause}) ->
%%                 {error, Cause}
%%         end,
%%     {ok, {etag, _}} = leo_storage_replicator_cp:replicate(
%%                         put, 1, ?TEST_REDUNDANCIES_1, Object, F),
%%     timer:sleep(100),
%%     ok.


%%--------------------------------------------------------------------
%% INTERNAL-FUNCTIONS
%%--------------------------------------------------------------------
%% for object-operation #1.
gen_mock_1(Case) ->
    meck:new(leo_storage_mq, [non_strict]),
    meck:expect(leo_storage_mq, publish,
                fun(Type, AddrId, Key, _FragmentIdL) ->
                        ?assertEqual(?QUEUE_TYPE_PER_FRAGMENT, Type),
                        ?assertEqual(?TEST_RING_ID_1, AddrId),
                        ?assertEqual(?TEST_KEY_1, Key),
                        ok
                end),

    meck:new(leo_storage_handler_object, [non_strict]),
    meck:expect(leo_storage_handler_object, put,
                fun({Fragments, Ref}) ->
                        ?assertEqual(true, erlang:is_reference(Ref)),
                        case Case of
                            ok ->
                                FIdL = [FId || {FId,_Object} <- Fragments],
                                {ok, Ref, {FIdL, []}};
                            fail ->
                                ErrorL = [{FId, cause} || {FId,_Object} <- Fragments],
                                {error, Ref, ErrorL}
                        end
                end),
    meck:expect(leo_storage_handler_object, put,
                fun(Ref, From, Fragments,_ReqId) ->
                        case Case of
                            ok ->
                                FIdL = [FId || {FId,_Object} <- Fragments],
                                erlang:send(From, {Ref, {ok, {FIdL, []}}});
                            fail ->
                                ErrorL = [{FId, cause} || {FId,_Object} <- Fragments],
                                erlang:send(From, {Ref, {error, {node(), ErrorL}}})
                        end
                end),
    ok.

%% for object-operation #2.
gen_mock_2([],_) ->
    ok;
gen_mock_2([H|T], Case) ->
    catch rpc:call(H, meck, new,
                   [leo_storage_handler_object, [no_link, non_strict]]),
    catch rpc:call(H, meck, expect,
                   [leo_storage_handler_object, put,
                    fun({Fragments, Ref}) ->
                            ?assertEqual(true, erlang:is_reference(Ref)),
                            case Case of
                                ok ->
                                    FIdL = [FId || {FId,_Object} <- Fragments],
                                    {ok, Ref, {FIdL, []}};
                                fail ->
                                    ErrorL = [{FId, cause} || {FId,_Object} <- Fragments],
                                    {error, Ref, ErrorL}
                            end
                    end]),
    catch rpc:call(H, meck, expect,
                   [leo_storage_handler_object, put,
                    fun(Ref, From, Fragments,_ReqId) ->
                            case Case of
                                ok ->
                                    FIdL = [FId || {FId,_Object} <- Fragments],
                                    erlang:send(From, {Ref, {ok, {FIdL, []}}});
                                fail ->
                                    ErrorL = [{FId, cause} || {FId,_Object} <- Fragments],
                                    erlang:send(From, {Ref, {error, {node(), ErrorL}}})
                            end
                    end]),
    gen_mock_2(T, Case).
-endif.
