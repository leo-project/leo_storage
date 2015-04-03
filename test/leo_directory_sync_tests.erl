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
%%====================================================================
-module(leo_directory_sync_tests).
-author('yosuke hara').

-include("leo_storage.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").


%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

api_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun stack_/1
                          ]]}.
setup() ->
    %% prepare network.
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    Node = list_to_atom("test_dir_0@" ++ Hostname),
    net_kernel:start([Node, shortnames]),

    %% launch ordning-reda
    leo_backend_db_sup:start_link(),
    leo_ordning_reda_api:start(),
    leo_directory_sync:start(),

    %% mock
    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, get_member_by_node,
                     fun(_Node) ->
                             {ok, #member{state = ?STATE_RUNNING}}
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                     fun(_Dir) ->
                             ?debugVal(_Dir),
                             {ok, #redundancies{
                                     nodes = [#redundant_node{available = true,
                                                              node = Node}]}}
                     end),
    Node.
teardown(_Node) ->
    leo_backend_db_sup:stop(),
    net_kernel:stop(),
    meck:unload(),
    ok.


%% @doc TEST leo_directory_sync:append/1
stack_(_) ->
    %%
    %% TEST: get_directories
    %%
    %% case-1
    Key_1 = <<"a/b/c/d/e.png">>,
    Metadata_1 = #?METADATA{key = Key_1,
                            ksize = byte_size(Key_1)},
    Ret_1 = leo_directory_sync:get_directories(Metadata_1),
    ?assertEqual(5, length(Ret_1)),
    ?assertEqual(<<"a/">>,            (lists:nth(1, Ret_1))#?METADATA.key),
    ?assertEqual(<<"a/b/">>,          (lists:nth(2, Ret_1))#?METADATA.key),
    ?assertEqual(<<"a/b/c/">>,        (lists:nth(3, Ret_1))#?METADATA.key),
    ?assertEqual(<<"a/b/c/d/">>,      (lists:nth(4, Ret_1))#?METADATA.key),
    ?assertEqual(<<"a/b/c/d/e.png">>, (lists:nth(5, Ret_1))#?METADATA.key),

    %% case-2
    Key_2 = <<"a/b/c/d/">>,
    Metadata_2 = #?METADATA{key = Key_2,
                            ksize = byte_size(Key_2)},
    Ret_2 = leo_directory_sync:get_directories(Metadata_2),
    ?assertEqual(4, length(Ret_2)),
    ?assertEqual(<<"a/">>,            (lists:nth(1, Ret_2))#?METADATA.key),
    ?assertEqual(<<"a/b/">>,          (lists:nth(2, Ret_2))#?METADATA.key),
    ?assertEqual(<<"a/b/c/">>,        (lists:nth(3, Ret_2))#?METADATA.key),
    ?assertEqual(<<"a/b/c/d/">>,      (lists:nth(4, Ret_2))#?METADATA.key),

    %% case-3
    Key_3 = <<"a.png">>,
    Metadata_3 = #?METADATA{key = Key_3,
                            ksize = byte_size(Key_3)},
    Ret_3 = leo_directory_sync:get_directories(Metadata_3),
    ?assertEqual(1, length(Ret_3)),
    ?assertEqual(<<"a.png">>, (lists:nth(1, Ret_3))#?METADATA.key),

    %% case-4
    Key_4 = <<"a/">>,
    Metadata_4 = #?METADATA{key = Key_4,
                            ksize = byte_size(Key_4)},
    Ret_4 = leo_directory_sync:get_directories(Metadata_4),
    ?assertEqual(1, length(Ret_4)),
    ?assertEqual(<<"a/">>, (lists:nth(1, Ret_4))#?METADATA.key),

    %%
    %% TEST: Stack and Store metadatas
    %%
    Key_5 = <<"a/b/c/d/e.png">>,
    Metadata_5 = #?METADATA{key = Key_5,
                            ksize = byte_size(Key_5)},

    Key_6 = <<"a/b/c/d.png">>,
    Metadata_6 = #?METADATA{key = Key_6,
                            ksize = byte_size(Key_6)},

    Key_7 = <<"a/b/c.png">>,
    Metadata_7 = #?METADATA{key = Key_7,
                            ksize = byte_size(Key_7)},

    ok = leo_directory_sync:append(Metadata_5),
    ok = leo_directory_sync:append(Metadata_6),
    ok = leo_directory_sync:append(Metadata_7),
    timer:sleep(timer:seconds(3)),

    {ok, Bin_5} = leo_backend_db_api:get(?DIR_DB_ID, term_to_binary({0, <<"a/b/c/d/">>, Key_5})),
    {ok, Bin_6} = leo_backend_db_api:get(?DIR_DB_ID, term_to_binary({0, <<"a/b/c/">>,   Key_6})),
    {ok, Bin_7} = leo_backend_db_api:get(?DIR_DB_ID, term_to_binary({0, <<"a/b/">>,     Key_7})),
    ?assertEqual(Metadata_5, binary_to_term(Bin_5)),
    ?assertEqual(Metadata_6, binary_to_term(Bin_6)),
    ?assertEqual(Metadata_7, binary_to_term(Bin_7)),
    ok.


-endif.

