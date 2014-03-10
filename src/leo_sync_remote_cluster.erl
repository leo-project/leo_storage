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

-export([start_link/0, start_link/3, stop/1,
         stack/2, request/1,
         gen_id/0, gen_id/1]).
-export([handle_send/3,
         handle_fail/2]).

-define(DEF_TIMEOUT_REMOTE_CLUSTER, timer:seconds(30)).

-define(BIN_META_SIZE, 16). %% metadata-size
-define(BIN_OBJ_SIZE,  32). %% object-size
-define(LEN_PADDING,    8). %% footer-size
-define(BIN_PADDING, <<0:64>>). %% footer


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Add a container into the supervisor
%%
-spec(start_link() ->
             ok | {error, any()}).
start_link() ->
    BufSize = ?env_mdcr_sync_proc_buf_size(),
    Timeout = ?env_mdcr_sync_proc_timeout(),
    start_link(gen_id(), BufSize, Timeout).

-spec(start_link(atom(), integer(), integer()) ->
             ok | {error, any()}).
start_link(Id, BufSize, Timeout) ->
    leo_ordning_reda_api:add_container(Id, [{module,      ?MODULE},
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


%% Request from a remote-node
%%
-spec(request(binary()) ->
             ok | {error, any()}).
request(CompressedObjs) ->
    case catch lz4:unpack(CompressedObjs) of
        {ok, OriginalObjects} ->
            case slice_and_store(OriginalObjects) of
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
    gen_id(erlang:phash2(leo_date:clock(),
                         ?env_num_of_mdcr_sync_procs()) + 1).
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
%% @doc
%% @private
-spec(stack_fun(#metadata{}, binary()) ->
             ok | {error, any()}).
stack_fun(#metadata{addr_id = AddrId,
                    key = Key} = Metadata, Object) ->
    MetaBin  = term_to_binary(Metadata),
    MetaSize = byte_size(MetaBin),
    ObjSize  = byte_size(Object),
    Data = << MetaSize:?BIN_META_SIZE, MetaBin/binary,
              ObjSize:?BIN_OBJ_SIZE, Object/binary, ?BIN_PADDING/binary >>,

    case leo_ordning_reda_api:stack(gen_id(), {AddrId, Key}, Data) of
        ok ->
            ok;
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Slicing objects and Store objects
%%
-spec(slice_and_store(binary()) ->
             ok | {error, any()}).
slice_and_store(Objects) ->
    try
        %% Retrieve metadata
        <<MetaSize:?BIN_META_SIZE, Rest_1/binary>> = Objects,
        MetaBin  = binary:part(Rest_1, {0, MetaSize}),
        Rest_2   = binary:part(Rest_1, {MetaSize, byte_size(Rest_1) - MetaSize}),
        Metadata = binary_to_term(MetaBin),

        %% Retrieve object
        <<ObjSize:?BIN_OBJ_SIZE, Rest_3/binary>> = Rest_2,
        Object = binary:part(Rest_3, {0, ObjSize}),
        Rest_4  = binary:part(Rest_3, {ObjSize, byte_size(Rest_3) - ObjSize}),

        %% Retrieve footer
        <<_Fotter:64, Rest_5/binary>> = Rest_4,

        ?debugVal({Metadata, Object, Rest_5}),
        %% @TODO: Store an object to object-storage
        %% case leo_object_storage_api:store(Metadata#metadata{ring_hash = RingHashCur},
        %%                                   Object) of
        %%     ok ->
        %%         slice_and_store(Rest_5, Errors);
        %%     {error, Cause} ->
        %%         ?warn("slice_and_store/2","key:~s, cause:~p",
        %%               [binary_to_list(Metadata#metadata.key), Cause]),
        %%         slice_and_store(Rest_5, [Metadata|Errors])
        %% end.
        ok
    catch
        _:Cause ->
            ?error("slice_and_store/1","cause:~p",[Cause]),
            {error, invalid_format}
    end.


%% @doc Retrieve cluster members
%% @private
get_cluster_members() ->
    case leo_mdcr_tbl_cluster_info:all() of
        {ok, ClusterInfoList} ->
            case leo_cluster_tbl_conf:get() of
                {ok, #?SYSTEM_CONF{num_of_dc_replicas =  NumOfReplicas}} ->
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
send([{ClusterId, Members}|Rest], StackedInfo, CompressedObjs) ->
    ok = send_1(Members, ClusterId, StackedInfo, CompressedObjs),
    send(Rest, StackedInfo, CompressedObjs).

%% @private
send_1([], ClusterId, StackedInfo,_CompressedObjs) ->
    ok = enqueue_fail_replication(StackedInfo, ClusterId);
send_1([Node|Rest], ClusterId, StackedInfo, CompressedObjs) ->
    Ret = case leo_rpc:call(Node, ?MODULE, request,
                            [CompressedObjs], ?DEF_TIMEOUT_REMOTE_CLUSTER) of
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
            ?warn("sned_1/2","node:~w, cause:~p", [Node, Reason]),
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
