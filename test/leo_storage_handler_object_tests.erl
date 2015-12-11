%%======================================================================
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
%% ---------------------------------------------------------------------
%% LeoFS Storage - EUnit
%%======================================================================
-module(leo_storage_handler_object_tests).

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

-define(TEST_BUCKET, <<"air">>).
-define(TEST_KEY_1, <<"air/on/g/string/1">>).
-define(TEST_KEY_2, <<"air/on/g/string/2">>).
-define(TEST_KEY_3, <<"air/on/g/string/3">>).
-define(TEST_KEY_4, <<"air/on/g/string/4">>).
-define(TEST_KEY_5, <<"air/on/g/string/5">>).
-define(TEST_KEY_6, <<"air/on/g/string/6">>).
-define(TEST_KEY_7, <<"air/on/g/string/7">>).
-define(TEST_KEY_8, <<"air/on/g/string/8">>).
-define(TEST_KEY_9, <<"air/on/g/string/9">>).


-define(TEST_BIN, <<"V">>).
-define(TEST_META_1, #?METADATA{key = ?TEST_KEY_2,
                                dsize = byte_size(?TEST_BIN)}).
-define(TEST_META_2, #?METADATA{key = ?TEST_KEY_5,
                                dsize = byte_size(?TEST_BIN)
                               }).
-define(TEST_META_3, #?METADATA{key = ?TEST_KEY_7,
                                dsize = byte_size(?TEST_BIN)
                               }).
-define(TEST_META_4, #?METADATA{key = ?TEST_KEY_8,
                                dsize = byte_size(?TEST_BIN)
                               }).


-define(TEST_META_9, #?METADATA{key = ?TEST_KEY_1,
                                dsize = byte_size(?TEST_BIN),
                                del = 1
                               }).



-define(TEST_REDUNDANCIES_1, [#redundant_node{node = TestNode_1, available = true},
                              #redundant_node{node = TestNode_2, available = true},
                              #redundant_node{node = TestNode_2, available = true},
                              #redundant_node{node = TestNode_1, available = true},
                              #redundant_node{node = TestNode_1, available = true},
                              #redundant_node{node = TestNode_1, available = true}
                             ]).


object_handler_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun get_1_/1,
                           fun get_2/1,
                           fun put_1_/1,
                           fun put_2_/1,
                           fun put_3_/1,
                           fun delete_1_/1,
                           fun head_/1,
                           fun copy_/1
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
    meck:expect(leo_storage_mq, publish, fun(_,_) ->
                                                 ok
                                         end),

    %% Prepare network env
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    TestNode_1 = list_to_atom("node_0@" ++ Hostname),
    net_kernel:start([TestNode_1, shortnames]),
    {ok, TestNode_2} = slave:start_link(list_to_atom(Hostname), 'node_1'),

    true = rpc:call(TestNode_1, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(TestNode_2, code, add_path, ["../deps/meck/ebin"]),
    {TestNode_1, TestNode_2}.

teardown({_, TestNode_2}) ->
    meck:unload(),
    net_kernel:stop(),
    slave:stop(TestNode_2),
    timer:sleep(100),
    ok.


%%--------------------------------------------------------------------
%% GET
%%--------------------------------------------------------------------
%% @doc  get/1
%% @private
get_1_({TestNode_1, TestNode_2}) ->
    %% leo_redundant_manager_api
    meck:new(leo_redundant_manager_api, [non_strict]),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(get, _Key) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{TestNode_1,true}, {TestNode_2,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    %% leo_object_storage_api
    meck:new(leo_object_storage_api, [non_strict]),
    meck:expect(leo_object_storage_api, get,
                fun(_Key, _StartPos, _EndPos, _IsForcedCheck) ->
                        not_found
                end),

    meck:new(leo_statistics_req_counter, [non_strict]),
    meck:expect(leo_statistics_req_counter, increment,
                fun(_) -> ok end),

    meck:new(leo_metrics_req, [non_strict]),
    meck:expect(leo_metrics_req, notify, fun(_) -> ok end),

    meck:new(leo_watchdog_state, [non_strict]),
    meck:expect(leo_watchdog_state, find_not_safe_items, fun(_) -> not_found end),

    Ref = make_ref(),
    Res = leo_storage_handler_object:get({Ref, ?TEST_KEY_1}),
    ?assertEqual({error, Ref, not_found}, Res),
    ok.

%% @doc  get/1
%% @private
get_2({TestNode_1, TestNode_2}) ->
    %% leo_redundant_manager_api
    meck:new(leo_redundant_manager_api, [non_strict]),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{TestNode_1,true}, {TestNode_2,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    %% leo_object_storage_api
    meck:new(leo_object_storage_api, [non_strict]),
    meck:expect(leo_object_storage_api, get,
                fun(_Key, _StartPos, _EndPos) ->
                        {ok, ?TEST_META_1, #?OBJECT{}}
                end),
    meck:expect(leo_object_storage_api, get,
                fun(_Key, _StartPos, _EndPos, _IsForcedCheck) ->
                        {ok, ?TEST_META_1, #?OBJECT{}}
                end),

    meck:new(leo_metrics_req, [non_strict]),
    meck:expect(leo_metrics_req, notify, fun(_) -> ok end),

    meck:new(leo_watchdog_state, [non_strict]),
    meck:expect(leo_watchdog_state, find_not_safe_items, fun(_) -> not_found end),

    meck:expect(leo_storage_read_repairer, repair,
                fun(_,_,_,_) -> {ok, ?TEST_META_1, <<>>} end),

    Ref = make_ref(),
    Res = leo_storage_handler_object:get({Ref, ?TEST_KEY_2}),
    ?assertEqual({ok, Ref, ?TEST_META_1, <<>>}, Res),
    ok.

%%--------------------------------------------------------------------
%% PUT
%%--------------------------------------------------------------------
%% put/6
put_1_({TestNode_1, TestNode_2}) ->
    AddrId = 0,
    Key = ?TEST_KEY_3,
    Bin = ?TEST_BIN,
    Size = byte_size(?TEST_BIN),
    ReqId = 0,
    Timestamp = 0,

    meck:new(leo_redundant_manager_api, [non_strict]),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(put, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [#redundant_node{node = TestNode_1,
                                                                    available = true},
                                                    #redundant_node{node = TestNode_2,
                                                                    available = true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    meck:new(leo_storage_replicator_cp, [non_strict]),
    meck:expect(leo_storage_replicator_cp, replicate,
                fun(_Method,_Quorum,_Redundancies,_ObjectPool,_Callback) ->
                        {ok, {etag, 1}}
                end),

    ok = rpc:call(TestNode_2, meck, new,
                  [leo_metrics_req, [no_link, non_strict]]),
    ok = rpc:call(TestNode_2, meck, expect,
                  [leo_metrics_req, notify, fun(_) -> ok end]),

    meck:new(leo_metrics_req, [non_strict]),
    meck:expect(leo_metrics_req, notify,
                fun(_) ->
                        ok
                end),

    meck:new(leo_watchdog_state, [non_strict]),
    meck:expect(leo_watchdog_state, find_not_safe_items,
                fun(_) ->
                        not_found
                end),

    Object = #?OBJECT{method = ?CMD_PUT,
                      addr_id = AddrId,
                      key = Key,
                      data = Bin,
                      dsize = Size,
                      req_id = ReqId,
                      timestamp = Timestamp,
                      del = 0},
    {ok, _Checksum} = leo_storage_handler_object:put(Object, 0),
    ok.

%% put/2
put_2_({_TestNode_1, _TestNode_2}) ->
    meck:new(leo_object_storage_api, [non_strict]),
    meck:expect(leo_object_storage_api, put,
                fun(_Key, _ObjPool) ->
                        {ok, 1}
                end),

    meck:new(leo_watchdog_state, [non_strict]),
    meck:expect(leo_watchdog_state, find_not_safe_items,
                fun(_) ->
                        not_found
                end),

    Ref = make_ref(),
    {ok, Ref,_Etag} = leo_storage_handler_object:put(
                        {#?OBJECT{key = ?TEST_KEY_3}, Ref}),
    ok.

%% put an object for the erasure-coding
put_3_({TestNode_1, TestNode_2}) ->
    %% Create mocks
    meck:new(leo_object_storage_api, [non_strict]),
    meck:expect(leo_object_storage_api, head,
                fun({_AddrId,_Key}) ->
                        {ok, term_to_binary(#?METADATA{key = ?TEST_KEY_3})}
                end),
    meck:expect(leo_object_storage_api, put,
                fun(_Key, _ObjPool) ->
                        {ok, 1}
                end),

    meck:new(leo_watchdog_state, [non_strict]),
    meck:expect(leo_watchdog_state, find_not_safe_items,
                fun(_) ->
                        not_found
                end),

    meck:new(leo_metrics_req, [non_strict]),
    meck:expect(leo_metrics_req, notify, fun(_) -> ok end),

    meck:new(leo_redundant_manager_api, [non_strict]),
    meck:expect(leo_redundant_manager_api, collect_redundancies_by_key,
                fun(_Key,_TotalReplicas) ->
                        {ok, {[{n, 2},
                               {w, 1},
                               {r, 1},
                               {d, 1}], ?TEST_REDUNDANCIES_1}}
                end),

    meck:new(leo_storage_event_notifier, [non_strict]),
    meck:expect(leo_storage_event_notifier, operate,
                fun(_,_) ->
                        ok
                end),

    gen_mock([{TestNode_1, ok},{TestNode_2, ok}]),

    %% Execute the erasure-coding
    DSize = 1024 * 1024,
    Bin = crypto:rand_bytes(DSize),
    Checksum = leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)),
    meck:new(leo_storage_replicator_cp, [non_strict]),
    meck:expect(leo_storage_replicator_cp, replicate,
                fun(_,_,_,_,_) ->
                        {ok,{etag,Checksum}}
                end),

    Ret = leo_storage_handler_object:put(
            #?OBJECT{redundancy_method = ?RED_ERASURE_CODE,
                     method = put,
                     addr_id = 123,
                     key = ?TEST_KEY_3,
                     data = Bin,
                     dsize = DSize,
                     ec_lib = 'vandrs',
                     ec_params = {4,2}}, 1),
    ?assertEqual({ok,{etag,Checksum}}, Ret),
    ok.


%%--------------------------------------------------------------------
%% DELETE
%%--------------------------------------------------------------------
%% delete/4
delete_1_({TestNode_1, TestNode_2}) ->
    AddrId = 0,
    Key = ?TEST_KEY_4,
    ReqId = 0,
    Timestamp = 0,

    meck:new(leo_redundant_manager_api, [non_strict]),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(_,_AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [#redundant_node{node = TestNode_1,
                                                                    available = true},
                                                    #redundant_node{node = TestNode_2,
                                                                    available = true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(_) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [#redundant_node{node = TestNode_1,
                                                                    available = true},
                                                    #redundant_node{node = TestNode_2,
                                                                    available = true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),
    meck:expect(leo_redundant_manager_api, get_members_by_status,
                fun(_State) ->
                        not_found
                end),

    meck:new(leo_storage_replicator_cp, [non_strict]),
    meck:expect(leo_storage_replicator_cp, replicate,
                fun(_Method,_Quorum,_Redundancies,_ObjectPool,_Callback) ->
                        {ok, 0}
                end),

    meck:new(leo_object_storage_api, [non_strict]),
    meck:expect(leo_object_storage_api, fetch_by_key,
                fun(_ParentDir,_) ->
                        {ok, []}
                end),
    meck:expect(leo_object_storage_api, head,
                fun(_) ->
                        {ok, term_to_binary(#?METADATA{key = ?TEST_KEY_4,
                                                       dsize = byte_size(?TEST_BIN)})}
                end),

    meck:new(leo_watchdog_state, [non_strict]),
    meck:expect(leo_watchdog_state, find_not_safe_items, fun(_) -> not_found end),

    meck:new(leo_directory_cache, [non_strict]),
    meck:expect(leo_directory_cache, delete, fun(_) -> ok end),

    meck:new(leo_directory_sync, [non_strict]),
    meck:expect(leo_directory_sync, delete, fun(_) -> ok end),
    meck:expect(leo_directory_sync, append, fun(_,_) -> ok end),

    meck:new(leo_cache_api, [non_strict]),
    meck:expect(leo_cache_api, delete, fun(_) -> ok end),

    ok = rpc:call(TestNode_2, meck, new,
                  [leo_cache_api, [no_link, non_strict]]),
    ok = rpc:call(TestNode_2, meck, expect,
                  [leo_cache_api, delete, fun(_) -> ok end]),

    ok = rpc:call(TestNode_2, meck, new,
                  [leo_metrics_req, [no_link, non_strict]]),
    ok = rpc:call(TestNode_2, meck, expect,
                  [leo_metrics_req, notify, fun(_) -> ok end]),

    meck:new(leo_metrics_req, [non_strict]),
    meck:expect(leo_metrics_req, notify, fun(_) -> ok end),


    Object_1 = #?OBJECT{method = ?CMD_DELETE,
                        addr_id = AddrId,
                        key = << Key/binary, "/" >>,
                        data = <<>>,
                        dsize = 0,
                        req_id = ReqId,
                        timestamp = Timestamp,
                        del = 1},
    Res_1 = leo_storage_handler_object:delete(Object_1, 0),
    ?assertEqual(ok, Res_1),

    Object_2 = #?OBJECT{method = ?CMD_DELETE,
                        addr_id = AddrId,
                        key = << Key/binary >>,
                        data = <<>>,
                        dsize = 0,
                        req_id = ReqId,
                        timestamp = Timestamp,
                        del = 1},
    Res_2 = leo_storage_handler_object:delete(Object_2, 0),
    ?assertEqual(ok, Res_2),
    ok.


%%--------------------------------------------------------------------
%% OTHER
%%--------------------------------------------------------------------
head_({TestNode_1, TestNode_2}) ->
    %% 1.
    meck:new(leo_object_storage_api, [non_strict]),
    meck:expect(leo_object_storage_api, head,
                fun(_Key) ->
                        {ok, term_to_binary(?TEST_META_2)}
                end),
    meck:new(leo_redundant_manager_api, [non_strict]),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [#redundant_node{node = TestNode_1,
                                                                    available = true},
                                                    #redundant_node{node = TestNode_2,
                                                                    available = true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    Res0 = leo_storage_handler_object:head(0, ?TEST_KEY_5),
    ?assertEqual({ok, ?TEST_META_2}, Res0),

    %% 2.
    meck:unload(),
    meck:new(leo_object_storage_api, [non_strict]),
    meck:expect(leo_object_storage_api, head,
                fun(_Key) ->
                        not_found
                end),
    meck:new(leo_redundant_manager_api, [non_strict]),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [#redundant_node{node = TestNode_1,
                                                                    available = true},
                                                    #redundant_node{node = TestNode_2,
                                                                    available = true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    Res1 = leo_storage_handler_object:head(0, ?TEST_KEY_6),
    ?assertEqual({error, not_found}, Res1),

    %% 2.
    meck:unload(),
    meck:new(leo_redundant_manager_api, [non_strict]),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [#redundant_node{node = TestNode_1,
                                                                    available = false},
                                                    #redundant_node{node = TestNode_2,
                                                                    available = true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),

    ok = rpc:call(TestNode_2, meck, new,
                  [leo_object_storage_api, [no_link, non_strict]]),
    ok = rpc:call(TestNode_2, meck, expect,
                  [leo_object_storage_api, head,
                   fun(_Arg) ->
                           {ok, term_to_binary(?TEST_META_3)}
                   end]),

    {ok, Res2} = leo_storage_handler_object:head(0, ?TEST_KEY_7),
    ?assertEqual(?TEST_META_3, Res2),
    ok.


copy_({TestNode_1, TestNode_2}) ->
    %% 1. for WRITE
    %%
    %% Retrieve metadata from head-func
    meck:new(leo_object_storage_api, [non_strict]),
    meck:expect(leo_object_storage_api, head,
                fun(_Key) ->
                        {ok, term_to_binary(?TEST_META_4)}
                end),
    meck:expect(leo_object_storage_api, get,
                fun(_Key, _StartPos, _EndPos) ->
                        {ok, ?TEST_META_4, #?OBJECT{}}
                end),
    meck:expect(leo_object_storage_api, get,
                fun(_Key, _StartPos, _EndPos, _IsForcedCheck) ->
                        {ok, ?TEST_META_4, #?OBJECT{}}
                end),

    %% Retrieve object from get-func
    meck:new(leo_redundant_manager_api, [non_strict]),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(get, _AddrId) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{TestNode_1,true}, {TestNode_2,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),
    meck:expect(leo_redundant_manager_api, get_member_by_node,
                fun(_) ->
                        {ok, #member{state = ?STATE_RUNNING}}
                end),

    %% ording-reda
    meck:new(leo_ordning_reda_api, [non_strict]),
    meck:expect(leo_ordning_reda_api, add_container,
                fun(_,_,_) ->
                        ok
                end),
    meck:expect(leo_ordning_reda_api, stack,
                fun(_,_,_) ->
                        ok
                end),
    meck:new(leo_metrics_req, [non_strict]),
    meck:expect(leo_metrics_req, notify, fun(_) -> ok end),

    meck:new(leo_watchdog_state, [non_strict]),
    meck:expect(leo_watchdog_state, find_not_safe_items, fun(_) -> not_found end),

    meck:new(leo_storage_event_notifier, [non_strict]),
    meck:expect(leo_storage_event_notifier, replicate, fun(_,_,_) -> ok end),

    Res1 = leo_storage_handler_object:replicate([TestNode_2, TestNode_2], 0, ?TEST_KEY_8),
    ?assertEqual(ok, Res1),

    %% 2. for DELETE
    %%
    meck:unload(leo_object_storage_api),
    meck:new(leo_object_storage_api, [non_strict]),
    meck:expect(leo_object_storage_api, head,
                fun(_Key) ->
                        {ok, term_to_binary(?TEST_META_2)}
                end),
    meck:expect(leo_object_storage_api, get,
                fun(_Key, _StartPos, _EndPos) ->
                        {ok, ?TEST_META_2, #?OBJECT{}}
                end),
    meck:expect(leo_object_storage_api, get,
                fun(_Key, _StartPos, _EndPos, _IsForcedCheck) ->
                        {ok, ?TEST_META_2, #?OBJECT{}}
                end),

    Res2 = leo_storage_handler_object:replicate(
             [TestNode_2, TestNode_2], 0, ?TEST_KEY_9),
    ?assertEqual(ok, Res2),
    ok.


%% @doc for object-operation #2.
%% @private
gen_mock([]) ->
    ok;
gen_mock([{H, Case}|T]) when H /= node() ->
    catch rpc:call(H, meck, new,
                   [leo_storage_handler_object, [no_link, non_strict]]),
    catch rpc:call(H, meck, expect,
                   [leo_storage_handler_object, put,
                    fun({[{_,#?OBJECT{redundancy_method = ?RED_ERASURE_CODE}}|_] = Fragments, Ref}) ->
                            case Case of
                                ok ->
                                    FIdL = [FId || {FId,_Object} <- Fragments],
                                    {ok, Ref, {FIdL, []}};
                                fail ->
                                    ErrorL = [{FId, cause} || {FId,_Object} <- Fragments],
                                    {error, Ref, ErrorL}
                            end;
                       ({_Object, Ref}) ->
                            {ok, Ref, 1}
                    end]),
    catch rpc:call(H, meck, expect,
                   [leo_storage_handler_object, put,
                    fun(Ref, From, Fragments,_ReqId) ->
                            case Case of
                                ok when is_list(Fragments) ->
                                    FIdL = [FId || {FId,_Object} <- Fragments],
                                    erlang:send(From, {Ref, {ok, {FIdL, []}}});
                                ok ->
                                    erlang:send(From, {Ref, {ok, 1}});
                                fail when is_list(Fragments) ->
                                    ErrorL = [{FId, cause} || {FId,_Object} <- Fragments],
                                    erlang:send(From, {Ref, {error, {node(), ErrorL}}});
                                fail ->
                                    erlang:send(From, {Ref, {error, {node(), 'cause'}}})
                            end
                    end]),
    gen_mock(T);
gen_mock([_|T]) ->
    gen_mock(T).

-endif.
