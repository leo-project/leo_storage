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
                             {ok, #redundancies{
                                     id = 0,
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
    ok = meck:new(leo_directory_cache, [non_strict]),
    ok = meck:expect(leo_directory_cache, append,
                     fun(_,_) ->
                             ok
                     end),
    ok = meck:expect(leo_directory_cache, delete,
                     fun(_) ->
                             ok
                     end),

    ok = meck:new(leo_cache_api, [non_strict]),
    ok = meck:expect(leo_cache_api, put,
                     fun(_,_) ->
                             ok
                     end),
    ok = meck:expect(leo_cache_api, delete,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_cache_api, get,
                     fun(_) ->
                             not_found
                     end),

    ok = meck:new(leo_directory_mq, [non_strict]),
    ok = meck:expect(leo_directory_mq, publish,
                     fun(_) ->
                             ok
                     end),

    %%
    %% TEST: get_directories
    %%
    %% case-1
    ?debugFmt("~n --- TEST#1", []),
    Key_1 = <<"a/b/c/d/e.png">>,
    Metadata_1 = #?METADATA{key = Key_1,
                            ksize = byte_size(Key_1)},
    Ret_1 = leo_directory_sync:get_directories(Metadata_1),
    ?assertEqual(4, length(Ret_1)),
    ?assertEqual(<<"a/">>,       (lists:nth(1, Ret_1))#?METADATA.key),
    ?assertEqual(<<"a/b/">>,     (lists:nth(2, Ret_1))#?METADATA.key),
    ?assertEqual(<<"a/b/c/">>,   (lists:nth(3, Ret_1))#?METADATA.key),
    ?assertEqual(<<"a/b/c/d/">>, (lists:nth(4, Ret_1))#?METADATA.key),

    %% case-2
    Key_2 = <<"a/b/c/d/">>,
    Metadata_2 = #?METADATA{key = Key_2,
                            ksize = byte_size(Key_2)},
    Ret_2 = leo_directory_sync:get_directories(Metadata_2),
    ?assertEqual(4, length(Ret_2)),
    ?assertEqual(<<"a/">>,       (lists:nth(1, Ret_2))#?METADATA.key),
    ?assertEqual(<<"a/b/">>,     (lists:nth(2, Ret_2))#?METADATA.key),
    ?assertEqual(<<"a/b/c/">>,   (lists:nth(3, Ret_2))#?METADATA.key),
    ?assertEqual(<<"a/b/c/d/">>, (lists:nth(4, Ret_2))#?METADATA.key),

    %% case-3
    Key_3 = <<"a.png">>,
    Metadata_3 = #?METADATA{key = Key_3,
                            ksize = byte_size(Key_3)},
    Ret_3 = leo_directory_sync:get_directories(Metadata_3),
    ?assertEqual(0, length(Ret_3)),

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
    timer:sleep(timer:seconds(2) + 500),

    {ok, Bin_5} = leo_backend_db_api:get(?DIR_DB_ID, <<"a/b/c/d/", "\t", Key_5/binary >>),
    {ok, Bin_6} = leo_backend_db_api:get(?DIR_DB_ID, <<"a/b/c/",   "\t", Key_6/binary >>),
    {ok, Bin_7} = leo_backend_db_api:get(?DIR_DB_ID, <<"a/b/",     "\t", Key_7/binary >>),

    ?assertEqual(Metadata_5, binary_to_term(Bin_5)),
    ?assertEqual(Metadata_6, binary_to_term(Bin_6)),
    ?assertEqual(Metadata_7, binary_to_term(Bin_7)),

    %%
    %% TEST: Store metadatas w/sync
    %%
    ?debugFmt("~n --- TEST: Store metadatas w/sync", []),
    Key_8 = <<"a/$$_dir_$$">>,
    Metadata_8 = #?METADATA{key = Key_8,
                            ksize = byte_size(Key_8)},
    ok = leo_directory_sync:append(Metadata_8, ?DIR_SYNC),
    {ok, Bin_8} = leo_backend_db_api:get(?DIR_DB_ID, <<"a/", "\t", Key_8/binary >>),
    ?assertEqual(Metadata_8, binary_to_term(Bin_8)),

    %%
    %% TEST: Add directories w/sync
    %%
    Key_9 = <<"ab/bc/cd/de/">>,
    ok = leo_directory_sync:create_directories([Key_9]),

    ParentDir_9 = leo_directory_sync:get_directory_from_key(Key_9),
    Dir_9 = << ParentDir_9/binary, "\t", Key_9/binary >>,
    {ok, Metadata_9} = leo_backend_db_api:get(?DIR_DB_ID, Dir_9),
    Metadata_9_1 = binary_to_term(Metadata_9),
    ?assertEqual(Key_9, Metadata_9_1#?METADATA.key),
    ?assertEqual(byte_size(Key_9), Metadata_9_1#?METADATA.ksize),
    ?assertEqual(-1, Metadata_9_1#?METADATA.dsize),
    ?assertEqual(true, (0 < Metadata_9_1#?METADATA.clock)),
    ?assertEqual(true, (0 < Metadata_9_1#?METADATA.timestamp)),

    %% @TODO: Remove an directory w/sync
    ok.

-endif.
