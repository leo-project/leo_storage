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
     [{with, [T]} || T <- [fun replicate_obj_1_/1,
                           fun replicate_obj_2_/1,
                           fun replicate_obj_3_/1
                          ]]}.

setup() ->
    %% Create mocks
    meck:new(leo_logger, [non_strict]),
    meck:expect(leo_logger, append, fun(_,_,_) ->
                                            ok
                                    end),

    meck:new(leo_storage_mq, [non_strict]),
    meck:expect(leo_storage_mq, publish, fun(_,_,_,_) ->
                                                 ok
                                         end),

    meck:new(leo_storage_msg_collector, [non_strict]),
    meck:expect(leo_storage_msg_collector, notify, fun(_,_,_) ->
                                                           ok
                                                   end),
    %% Prepare network env
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
    gen_mock([{TestNode_1,ok}, {TestNode_2,ok}]),

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

    Quorum = ?quorum_of_fragments(?CMD_PUT, 2, 1, 1,
                                  CodingParam_M, TotalReplicas),
    Ret = leo_storage_replicator_ec:replicate(
            put, Quorum, ?TEST_REDUNDANCIES_1, Fragments, F),
    ?assertEqual({ok,{etag,0}}, Ret),
    timer:sleep(100),
    ok.

%% object-replication#2
replicate_obj_2_({TestNode_1, TestNode_2}) ->
    gen_mock([{TestNode_1,ok}, {TestNode_2,fail}]),

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

    Quorum = ?quorum_of_fragments(?CMD_PUT, 2, 1, 1,
                                  CodingParam_M, TotalReplicas),
    Ret = leo_storage_replicator_ec:replicate(
            put, Quorum, ?TEST_REDUNDANCIES_1, Fragments, F),
    ?assertEqual({ok,{etag,0}}, Ret),
    timer:sleep(100),
    ok.

%% object-replication#2
replicate_obj_3_({TestNode_1, TestNode_2}) ->
    gen_mock([{TestNode_1,fail}, {TestNode_2,ok}]),

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

    Quorum = ?quorum_of_fragments(?CMD_PUT, 2, 1, 1,
                                  CodingParam_M, TotalReplicas),
    Ret = leo_storage_replicator_ec:replicate(
            put, Quorum, ?TEST_REDUNDANCIES_1, Fragments, F),
    ?assertEqual({error,[{TestNode_1,[{1,cause},
                                      {4,cause},
                                      {5,cause},
                                      {6,cause}]}]}, Ret),
    timer:sleep(100),
    ok.

%% @doc for object-operation #2.
%% @private
gen_mock([]) ->
    ok;
gen_mock([{H, Case}|T]) ->
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
    gen_mock(T).

-endif.
