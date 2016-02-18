%%====================================================================
%%
%% LeoFS Storage
%%
%% Copyright (c) 2012-2016 Rakuten, Inc.
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
-module(leo_storage_handler_directory_tests).

-include("leo_storage.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

object_handler_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun find_by_parent_dir_/1
                          ]]}.

setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    Node0 = list_to_atom("node_0@" ++ Hostname),
    net_kernel:start([Node0, shortnames]),
    {ok, Node1} = slave:start_link(list_to_atom(Hostname), 'node_1'),

    true = rpc:call(Node0, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(Node1, code, add_path, ["../deps/meck/ebin"]),

    [Node0, Node1].

teardown([_, Node1]) ->
    net_kernel:stop(),
    slave:stop(Node1),
    ok.


find_by_parent_dir_([Node0, Node1]) ->
    meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, get_members,
                     fun() ->
                             Members = [#member{node  = Node0,
                                                state = ?STATE_RUNNING},
                                        #member{node  = Node1,
                                                state = ?STATE_RUNNING},
                                        #member{node  = 'stoped_node',
                                                state = ?STATE_STOP}
                                       ],
                             {ok, Members}
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                     fun(_Dir) ->
                             ?debugVal(_Dir),
                             {ok, #redundancies{
                                     vnode_id_to = 0,
                                     nodes = [#redundant_node{available = false,
                                                              node = Node0},
                                              #redundant_node{available = true,
                                                              node = Node1}
                                             ]}}
                     end),

    ok = rpc:call(Node1, meck, new,    [leo_backend_db_api, [no_link, non_strict]]),
    ok = rpc:call(Node1, meck, expect, [leo_backend_db_api, get,
                                        fun(_,_) ->
                                                {ok, <<>>}
                                        end]),
    ok = rpc:call(Node1, meck, expect, [leo_backend_db_api, fetch,
                                        fun(_InstanceName,_KeyBin,_Fun,_MaxKeys) ->
                                                {ok, [#?METADATA{key = <<"air/on/g/0.png">>},
                                                      #?METADATA{key = <<"air/on/g/1.png">>}
                                                     ]}
                                        end]),

    ok = rpc:call(Node1, meck, new,    [leo_cache_api, [no_link, non_strict]]),
    ok = rpc:call(Node1, meck, expect, [leo_cache_api, get,
                                        fun(_) ->
                                                not_found
                                        end]),
    ok = rpc:call(Node1, meck, expect, [leo_cache_api, put,
                                        fun(_,_) ->
                                                ok
                                        end]),

    ok = rpc:call(Node1, meck, new,    [leo_misc, [no_link, non_strict]]),
    ok = rpc:call(Node1, meck, expect, [leo_misc, binary_tokens,
                                        fun(_,_) ->
                                                [<<"air/on/g/">>]
                                        end]),

    ok = rpc:call(Node1, meck, new,    [leo_date, [no_link, non_strict]]),
    ok = rpc:call(Node1, meck, expect, [leo_date, now,
                                        fun() ->
                                                0
                                        end]),

    ok = rpc:call(Node1, meck, new,    [leo_directory_cache, [no_link, non_strict]]),
    ok = rpc:call(Node1, meck, expect, [leo_directory_cache, merge,
                                        fun(_,_) ->
                                                ok
                                        end]),

    ok = meck:new(leo_directory_mq, [non_strict]),
    ok = meck:expect(leo_directory_mq, publish,
                     fun(_) ->
                             ok
                     end),

    {ok, Res} = leo_storage_handler_directory:find_by_parent_dir(
                  <<"air/on/g/">>, <<>>, <<>>, 1000),
    ?debugVal(Res),
    ?assertEqual(2, length(Res)),

    meck:unload(),
    ok.

-endif.
