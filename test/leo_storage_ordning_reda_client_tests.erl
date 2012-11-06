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
-module(leo_storage_ordning_reda_client_tests).
-author('yosuke hara').

-include("leo_storage.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_ordning_reda/include/leo_ordning_reda.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").


%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

ordning_reda_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun suite_regular_/1,
                           fun suite_error_/1
                          ]]}.

setup() ->
    %% prepare network.
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    Node = list_to_atom("test_0@" ++ Hostname),
    net_kernel:start([Node, shortnames]),

    %% launch ordning-reda
    leo_ordning_reda_api:start(),
    leo_storage_ordning_reda_client:start_link(Node, 1024, 5000),

    %% mock
    ok = meck:new(leo_redundant_manager_api),
    ok = meck:expect(leo_redundant_manager_api, get_member_by_node,
                     fun(_Node) ->
                             {ok, #member{state = ?STATE_RUNNING}}
                     end),
    Node.

teardown(Node) ->
    leo_storage_ordning_reda_client:stop(Node),

    net_kernel:stop(),
    meck:unload(),
    ok.

suite_regular_(Node) ->
    ok = meck:new(leo_object_storage_api),
    ok = meck:expect(leo_object_storage_api, store,
                     fun(_Metadata, _Object) ->
                             ?debugVal({_Metadata, byte_size(_Object)}),
                             ok
                     end),
    stack(Node),
    ok.

suite_error_(Node) ->
    ok = meck:new(leo_object_storage_api),
    ok = meck:expect(leo_object_storage_api, store,
                     fun(_Metadata, _Object) ->
                             {error, "Not stored"}
                     end),
    stack(Node),
    ok.


stack(Node) ->
    AddrId = 1024,
    Size   = 512,
    Key1   = <<"photo/hawaii-0.jpg">>,
    Key2   = <<"photo/hawaii-1.jpg">>,
    Key3   = <<"photo/hawaii-2.jpg">>,
    Key4   = <<"photo/hawaii-3.jpg">>,
    Meta1  = #metadata{addr_id = AddrId, key = Key1, dsize = Size, ksize = 18},
    Meta2  = #metadata{addr_id = AddrId, key = Key2, dsize = Size, ksize = 18},
    Meta3  = #metadata{addr_id = AddrId, key = Key3, dsize = Size, ksize = 18},
    Meta4  = #metadata{addr_id = AddrId, key = Key4, dsize = Size, ksize = 18},
    Object = crypto:rand_bytes(Size),

    _ = leo_storage_ordning_reda_client:stack([Node], AddrId, Key1, Meta1, Object),
    _ = leo_storage_ordning_reda_client:stack([Node], AddrId, Key2, Meta2, Object),
    _ = leo_storage_ordning_reda_client:stack([Node], AddrId, Key3, Meta3, Object),
    _ = leo_storage_ordning_reda_client:stack([Node], AddrId, Key4, Meta4, Object),
    ok.

-endif.

