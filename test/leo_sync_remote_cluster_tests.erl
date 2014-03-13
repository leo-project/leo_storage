%%====================================================================
%%
%% LeoFS Storage
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
%%====================================================================
-module(leo_sync_remote_cluster_tests).
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

suite_test_() ->
    {setup,
     fun () ->
             [] = os:cmd("epmd -daemon"),
             {ok, Hostname} = inet:gethostname(),
             Node = list_to_atom("test_0@" ++ Hostname),
             net_kernel:start([Node, shortnames]),

             leo_ordning_reda_api:start(),

             ok = gen_mocks(Node),
             ok
     end,
     fun (_) ->
             net_kernel:stop(),
             meck:unload(),

             folsom:stop(),
             mnesia:stop()
     end,
     [
      {"test", {timeout, 300, fun suite/0}}
     ]}.


%% @doc Generate mocks
%% @private
gen_mocks(Node) ->
    ok = meck:new(leo_redundant_manager_api),
    ok = meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                     fun(_AddrId) ->
                             {ok, #redundancies{n = 3,
                                                r = 1,
                                                w = 2,
                                                d = 2,
                                                nodes = [#redundant_node{node = Node},
                                                         #redundant_node{node = Node},
                                                         #redundant_node{node = Node}]
                                               }}
                     end),

    ok = meck:new(leo_storage_replicator),
    ok = meck:expect(leo_storage_replicator, replicate,
                     fun(_,_,_,_,_) ->
                             ok
                     end),


    ClusterId = "cluster_1",
    ok = meck:new(leo_mdcr_tbl_cluster_info),
    ok = meck:expect(leo_mdcr_tbl_cluster_info, all,
                     fun() ->
                             {ok, [#?CLUSTER_INFO{cluster_id = ClusterId,
                                                  num_of_dc_replicas = 1}]}
                     end),
    ok = meck:expect(leo_mdcr_tbl_cluster_info, get,
                     fun(_ClusterId) ->
                             {ok, #?CLUSTER_INFO{cluster_id = ClusterId,
                                                 num_of_dc_replicas = 1}}
                     end),

    ok = meck:new(leo_cluster_tbl_conf),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{num_of_dc_replicas = 1}}
                     end),
    ok = meck:new(leo_mdcr_tbl_cluster_member),
    ok = meck:expect(leo_mdcr_tbl_cluster_member, find_by_limit,
                     fun(_,_) ->
                             {ok, [#?CLUSTER_MEMBER{node = Node, cluster_id = ClusterId},
                                   #?CLUSTER_MEMBER{node = Node, cluster_id = ClusterId},
                                   #?CLUSTER_MEMBER{node = Node, cluster_id = ClusterId}
                                  ]}
                     end),

    ok = meck:new(leo_rpc),
    ok = meck:expect(leo_rpc, call,
                     fun(_Node,_Module,_Method,_Args,_Timeout) ->
                             %% ?debugVal({_Node,_Module,_Method,_Args,_Timeout}),
                             erlang:apply(_Module, _Method, _Args),
                             ok
                     end),
    ok = meck:new(leo_storage_mq_client),
    ok = meck:expect(leo_storage_mq_client, publish,
                     fun(_,_,_,_) ->
                             ok
                     end),
    ok.


suite() ->
    stack("cluster_1",100),
    stack([],100),
    ok.


%% @private
%% @private
stack(_,0) ->
    ok;
stack(ClusterId, Index) ->
    AddrId = 1024,
    Size   = erlang:phash2(leo_date:now(), 512) + 64,
    Key    = list_to_binary(lists:append(["test/photo/leo/", integer_to_list(Index)])),
    Meta   = #metadata{addr_id = AddrId, key = Key, dsize = Size, ksize = byte_size(Key)},
    Object = crypto:rand_bytes(Size),

    case ClusterId of
        [] -> ok = leo_sync_remote_cluster:stack(Meta, Object);
        _  -> ok = leo_sync_remote_cluster:stack(ClusterId, Meta, Object)
    end,
    stack(ClusterId, Index - 1).

-endif.

