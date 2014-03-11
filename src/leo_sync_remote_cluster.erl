%%======================================================================
%%
%% Leo Storage
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
%%======================================================================
-module(leo_sync_remote_cluster).
-author('Yosuke Hara').

-behaviour(leo_ordning_reda_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_ordning_reda/include/leo_ordning_reda.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, start_link/1, stop/1,
         stack/2, store/2,
         gen_id/0, gen_id/1]).
-export([handle_send/3,
         handle_fail/2]).

-define(DEF_TIMEOUT_REMOTE_CLUSTER, timer:seconds(30)).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Add a container into the supervisor
%%
-spec(start_link() ->
             ok | {error, any()}).
start_link() ->
    start_link(gen_id()).

-spec(start_link(atom()) ->
             ok | {error, any()}).
start_link(UId) ->
    BufSize = ?env_mdcr_sync_proc_buf_size(),
    Timeout = ?env_mdcr_sync_proc_timeout(),
    leo_ordning_reda_api:add_container(UId, [{module,      ?MODULE},
                                             {buffer_size, BufSize},
                                             {timeout,     Timeout}]).


%% @doc Remove a container from the supervisor
%%
-spec(stop(atom()) ->
             ok | {error, any()}).
stop(Id) ->
    leo_ordning_reda_api:remove_container(Id).


%% @doc Stack a object into the ordning&reda
%%
-spec(stack(#metadata{}, binary()) ->
             ok | {error, any()}).
stack(Metadata, Object) ->
    stack_fun(Metadata, Object).


%% Store stacked objects
%%
-spec(store(string(), binary()) ->
             ok | {error, any()}).
store(ClusterId, CompressedObjs) ->
    case catch lz4:unpack(CompressedObjs) of
        {ok, OriginalObjects} ->
            case slice_and_replicate(ClusterId, OriginalObjects) of
                ok ->
                    ok;
                {error, _Cause} ->
                    {error, fail_storing_files}
            end;
        {_, Cause} ->
            {error, Cause}
    end.


%% Generate a sync-proc of ID
%%
-spec(gen_id() ->
             atom()).
gen_id() ->
    gen_id(integer_to_list(erlang:phash2(leo_date:clock(),
                                         ?env_num_of_mdcr_sync_procs()) + 1)).
-spec(gen_id(pos_integer()) ->
             atom()).
gen_id(Id) ->
    list_to_atom(lists:append([?DEF_PREDIX_MDCR_SYNC_PROC, Id])).


%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------
%% @doc Handle send object to a remote-node.
%%
handle_send(_UId, StackedInfo, CompressedObjs) ->
    %% Retrieve remote-members from
    case get_cluster_members() of
        {ok, ListMembers} ->
            send(ListMembers, StackedInfo, CompressedObjs);
        {error,_Reason} ->
            {error,_Reason}
    end.


%% @doc Handle a fail process
%%
-spec(handle_fail(atom(), list({integer(), string()})) ->
             ok | {error, any()}).
handle_fail(_, []) ->
    ok;
handle_fail(UId, [{AddrId, Key}|Rest] = _StackInfo) ->
    ?warn("handle_fail/2","uid:~w, addr-id:~w, key:~s", [UId, AddrId, Key]),

    ok = leo_storage_mq_client:publish(
           ?QUEUE_TYPE_SYNC_OBJ_WITH_DC, AddrId, Key),
    handle_fail(UId, Rest).


%%--------------------------------------------------------------------
%% INNER FUNCTION
%%--------------------------------------------------------------------
%% @doc Stack an object into "ordning-reda"
%% @private
-spec(stack_fun(#metadata{}, binary()) ->
             ok | {error, any()}).
stack_fun(#metadata{addr_id = AddrId,
                    key = Key} = Metadata, Object) ->
    MetaBin  = term_to_binary(Metadata),
    MetaSize = byte_size(MetaBin),
    ObjSize  = byte_size(Object),
    Data = << MetaSize:?DEF_BIN_META_SIZE, MetaBin/binary,
              ObjSize:?DEF_BIN_OBJ_SIZE, Object/binary,
              ?DEF_BIN_PADDING/binary >>,

    UId = gen_id(),
    stack_fun(UId, AddrId, Key, Data).


stack_fun(UId, AddrId, Key, Data) ->
    case leo_ordning_reda_api:stack(UId, {AddrId, Key}, Data) of
        ok ->
            ok;
        {error, undefined} ->
            case start_link(UId) of
                ok ->
                    stack_fun(UId, AddrId, Key, Data);
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Slicing objects and Store objects
%% @private
-spec(slice_and_replicate(string(), binary()) ->
             ok | {error, any()}).
slice_and_replicate(_, <<>>) ->
    ok;
slice_and_replicate(ClusterId, StackedObjs) ->
    %% Retrieve a metadata and an object from stacked objects,
    %% then replicate an object into the storage cluster
    case slice(StackedObjs) of
        {ok, Metadata, Object, StackedObjs_1} ->
            case replicate(ClusterId, Metadata, Object) of
                ok ->
                    ok;
                {error, Cause} ->
                    %% Enqueue the fail message
                    %%
                    ?warn("slice_and_replicate/1","key:~s, cause:~p",
                          [binary_to_list(Metadata#metadata.key), Cause])
            end,
            slice_and_replicate(ClusterId, StackedObjs_1);
        {error, Cause}->
            {error, Cause}
    end.

%% @private
slice(StackedObjs) ->
    try
        %% Retrieve metadata
        <<MetaSize:?DEF_BIN_META_SIZE, Rest_1/binary>> = StackedObjs,
        MetaBin  = binary:part(Rest_1, {0, MetaSize}),
        Rest_2   = binary:part(Rest_1, {MetaSize, byte_size(Rest_1) - MetaSize}),
        Metadata = binary_to_term(MetaBin),

        %% Retrieve object
        <<ObjSize:?DEF_BIN_OBJ_SIZE, Rest_3/binary>> = Rest_2,
        Object = binary:part(Rest_3, {0, ObjSize}),
        Rest_4 = binary:part(Rest_3, {ObjSize, byte_size(Rest_3) - ObjSize}),

        %% Retrieve footer
        <<_Fotter:64, Rest_5/binary>> = Rest_4,
        {ok, Metadata, Object, Rest_5}
    catch
        _:Cause ->
            ?error("slice/1", "cause:~p",[Cause]),
            {error, invalid_format}
    end.

%% @private
replicate(ClusterId, Metadata, Object) ->
    %% Retrieve redundancies of the cluster,
    %% then overwrite 'n' and 'w' for the mdc-replication
    case leo_mdcr_tbl_cluster_info:get(ClusterId) of
        {ok, #?CLUSTER_INFO{num_of_dc_replicas = NumOfReplicas}} ->
            case leo_redundant_manager_api:get_redundancies_by_addr_id(Metadata#metadata.addr_id) of
                {ok, #redundancies{} = Redundancies} ->

                    %% Replicate an object into the storage cluster
                    %%
                    CustomMetaBin  = term_to_binary([{'cluster_id',      ClusterId},
                                                     {'num_of_replicas', NumOfReplicas}]),
                    leo_storage_handler_object:put(
                      Redundancies#redundancies{n = NumOfReplicas, w = 1},
                      Metadata#metadata{msize = erlang:byte_size(CustomMetaBin)},
                      CustomMetaBin, Object);
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Retrieve cluster members
%% @private
get_cluster_members() ->
    case leo_mdcr_tbl_cluster_info:all() of
        {ok, ClusterInfoList} ->
            case leo_cluster_tbl_conf:get() of
                {ok, #?SYSTEM_CONF{num_of_dc_replicas = NumOfReplicas}} ->
                    case get_cluster_members_1(ClusterInfoList, NumOfReplicas, []) of
                        {ok, MDCR_Info} ->
                            {ok,  MDCR_Info};
                        {error, Cause} ->
                            {error, Cause}
                    end;
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.

%% @private
get_cluster_members_1([],_NumOfReplicas, Acc) ->
    {ok, Acc};
get_cluster_members_1([#?CLUSTER_INFO{cluster_id = ClusterId}|Rest],
                      NumOfReplicas, Acc) ->
    case leo_mdcr_tbl_cluster_member:find_by_limit(
           ClusterId, ?DEF_NUM_OF_REMOTE_MEMBERS) of
        {ok, ClusterMembers} ->
            get_cluster_members_1(Rest, NumOfReplicas,
                                  [#mdc_replication_info{cluster_id = ClusterId,
                                                         num_of_replicas = NumOfReplicas,
                                                         cluster_members = ClusterMembers}
                                   |Acc]);
        not_found = Cause ->
            {error, Cause};
        {error, Cause} ->
            {error, Cause}
    end.


%% Send stacked objects to remote-members with leo-rpc
%% @private
send([],_StackedInfo,_CompressedObjs) ->
    ok;
send([#mdc_replication_info{cluster_id = ClusterId,
                            cluster_members = Members}|Rest], StackedInfo, CompressedObjs) ->
    ok = send_1(Members, ClusterId, StackedInfo, CompressedObjs),
    send(Rest, StackedInfo, CompressedObjs).

%% @private
send_1([], ClusterId, StackedInfo,_CompressedObjs) ->
    ok = enqueue_fail_replication(StackedInfo, ClusterId);
send_1([#?CLUSTER_MEMBER{node = Node}|Rest], ClusterId, StackedInfo, CompressedObjs) ->
    Ret = case leo_rpc:call(Node, ?MODULE, store,
                            [ClusterId, CompressedObjs],
                            ?DEF_TIMEOUT_REMOTE_CLUSTER) of
              ok ->
                  ok;
              {error, Cause} ->
                  {error, Cause};
              {badrpc, Cause} ->
                  {error, Cause};
              timeout = Cause ->
                  {error, Cause}
          end,

    case Ret of
        ok ->
            ok;
        {error, Reason} ->
            ?warn("send_1/4","node:~w, cause:~p", [Node, Reason]),
            send_1(Rest, ClusterId, StackedInfo, CompressedObjs)
    end.


%% @doc Put messages of fail replication into the queue
%% @private
enqueue_fail_replication([],_ClusterId) ->
    ok;
enqueue_fail_replication([{AddrId, Key}|Rest], ClusterId) ->
    ok = leo_storage_mq_client:publish(
           ?QUEUE_TYPE_SYNC_OBJ_WITH_DC, ClusterId, AddrId, Key),
    enqueue_fail_replication(Rest, ClusterId).
