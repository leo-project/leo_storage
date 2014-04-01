%%======================================================================
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
%%
%%======================================================================
-module(leo_storage_handle_sync).

-author('Yosuke Hara').

-behaviour(leo_mdcr_sync_cluster_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([get_metadatas/1,
         force_sync/1]).
-export([handle_call/1]).

-define(DEF_THRESHOLD_LEN, 100).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Compare local-metadatas with remote-metadatas
get_metadatas(ListAddrAndKey) ->
    get_metadatas_1(ListAddrAndKey, []).

get_metadatas_1([], Acc) ->
    {ok, Acc};
get_metadatas_1([AddrAndKey|Rest], Acc) ->
    case leo_object_storage_api:head(AddrAndKey) of
        {ok, MetaBin} ->
            Metadata = binary_to_term(MetaBin),
            get_metadatas_1(Rest, [Metadata|Acc]);
        _ ->
            {AddrId, Key} = AddrAndKey,
            get_metadatas_1(Rest, [#?METADATA{addr_id = AddrId,
                                              key = Key}|Acc])
    end.


%% @doc Synchronize object with remote-cluster(s)
%%
-spec(force_sync(atom()) ->
             ok).
force_sync(ClusterId) when is_atom(ClusterId) ->
    handle_call(ClusterId);
force_sync(_) ->
    {error, invalid_parameter}.


%%--------------------------------------------------------------------
%% CALLBACK FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Handle sync objects with a remote storage cluster
%%
handle_call(ClusterId) ->
    send_metadata(ClusterId).


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc synchronize objects with a remote-cluster
%% @private
-spec(send_metadata(atom()) ->
             ok).
send_metadata(ClusterId) ->
    Callback = send_addrid_and_key_callback(ClusterId),
    case leo_object_storage_api:fetch_by_addr_id(0, Callback) of
        {ok, []} ->
            ok;
        {ok, RetL} ->
            send_addrid_and_key_to_remote(ClusterId, RetL);
        Error ->
            Error
    end.

%% @private
-spec(send_addrid_and_key_callback(atom()) ->
             ok).
send_addrid_and_key_callback(ClusterId) ->
    fun(K, V, Acc) when length(Acc) >= ?DEF_THRESHOLD_LEN ->
            ok = send_addrid_and_key_to_remote(ClusterId, Acc),
            send_addrid_and_key_callback_1(K, V, []);
       (K, V, Acc) ->
            send_addrid_and_key_callback_1(K, V, Acc)
    end.

%% @private
send_addrid_and_key_callback_1(_K, V, Acc) ->
    Metadata_1 = binary_to_term(V),
    Metadata_2 = leo_object_storage_transformer:transform_metadata(Metadata_1),
    #?METADATA{addr_id = AddrId} = Metadata_2,

    case leo_redundant_manager_api:get_redundancies_by_addr_id(put, AddrId) of
        {ok, #redundancies{nodes = Redundancies}} ->
            Nodes = [N || #redundant_node{node = N,
                                          available = true} <- Redundancies],
            send_addrid_and_key_callback_2(Nodes, Metadata_2, Acc);
        _Other ->
            Acc
    end.

%% @doc Retrieve an object,
%%      then it is stacked into a buffer of a transfer
%% @private
send_addrid_and_key_callback_2([Node|_],
                               #?METADATA{key = Key,
                                          addr_id = AddrId}, Acc) when Node == erlang:node() ->
    [{AddrId, Key}|Acc];
send_addrid_and_key_callback_2(_,_,Acc) ->
    Acc.


%% @doc Send list of metadatas to remote-storage(s),
%%      then compare with metadatas of remote-storage
%%      if found an inconsist metadata then fix it
%% @private
send_addrid_and_key_to_remote(ClusterId, ListAddrIdAndKey) ->
    case leo_sync_remote_cluster:get_cluster_members(ClusterId) of
        {ok, ListMembers} ->
            send_addrid_and_key_to_remote_1(ListMembers, ListAddrIdAndKey);
        {error, Cause} ->
            ?warn("send_addrid_and_key_callback/1", "cause:~p", [Cause]),
            ok
    end.

%% @private
send_addrid_and_key_to_remote_1([],_ListAddrIdAndKey) ->
    ok;
send_addrid_and_key_to_remote_1([#mdc_replication_info{
                                    cluster_members = Members}|Rest], ListAddrIdAndKey) ->
    case send_addrid_and_key_to_remote_2(Members, ListAddrIdAndKey, 0) of
        {ok, RetL} ->
            %% Compare with local-cluster's objects
            leo_sync_remote_cluster:compare_metadata(RetL);
        _ ->
            void
    end,
    send_addrid_and_key_to_remote_1(Rest, ListAddrIdAndKey).


%% @private
send_addrid_and_key_to_remote_2([],_ListAddrIdAndKey,_RetryTimes) ->
    %% @TODO enqueue a fail msg
    ok;
send_addrid_and_key_to_remote_2([_|Rest], ListAddrIdAndKey, ?DEF_MAX_RETRY_TIMES) ->
    send_addrid_and_key_to_remote_2(Rest, ListAddrIdAndKey, 0);
send_addrid_and_key_to_remote_2([#?CLUSTER_MEMBER{
                                     node = Node,
                                     port = Port}|Rest] = ClusterMembes,
                                ListAddrIdAndKey, RetryTimes) ->
    Node_1 = list_to_atom(lists:append([atom_to_list(Node),
                                        ":",
                                        integer_to_list(Port)])),
    case leo_rpc:call(Node_1, erlang, node, []) of
        Msg when Msg == timeout orelse
                 element(1, Msg) == badrpc ->
            send_addrid_and_key_to_remote_2(ClusterMembes, ListAddrIdAndKey, RetryTimes + 1);
        _ ->
            Timeout = ?env_mdcr_req_timeout(),
            Ret = case leo_rpc:call(Node_1, ?MODULE, get_metadatas,
                                    [ListAddrIdAndKey], Timeout) of
                      {ok, RetL} ->
                          {ok, RetL};
                      {error, Cause} ->
                          {error, Cause};
                      {badrpc, Cause} ->
                          {error, Cause};
                      timeout = Cause ->
                          {error, Cause}
                  end,

            case Ret of
                {ok, _} ->
                    Ret;
                {error,_Cause} ->
                    send_addrid_and_key_to_remote_2(Rest, ListAddrIdAndKey, 0)
            end
    end.
