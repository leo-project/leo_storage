%%======================================================================
%%
%% Leo Storage
%%
%% Copyright (c) 2012 Rakuten, Inc.
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
%% Leo Storage - OrdningReda Client
%% @doc
%% @end
%%======================================================================
-module(leo_storage_ordning_reda_client).

-author('Yosuke Hara').

-behaviour(leo_ordning_reda_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_ordning_reda/include/leo_ordning_reda.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/3, stop/1, stack/5,  request/1]).
-export([handle_send/2,
         handle_fail/2]).

-define(BIN_META_SIZE, 16). %% metadata-size
-define(BIN_OBJ_SIZE,  32). %% object-size
-define(LEN_PADDING,    8). %% footer-size
-define(BIN_PADDING, <<0:64>>). %% footer

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Add a container into the supervisor
%%
-spec(start_link(atom(), integer(), integer()) ->
             ok | {error, any()}).
start_link(Node, BufSize, Timeout) ->
    leo_ordning_reda_api:add_container(stack, Node, [{module,      ?MODULE},
                                                     {buffer_size, BufSize},
                                                     {timeout,     Timeout}]).

%% @doc Remove a container from the supervisor
%%
-spec(stop(atom()) ->
             ok | {error, any()}).
stop(Node) ->
    leo_ordning_reda_api:remove_container(stack, Node).


%% @doc Stack a object into the ordning&reda
%%
-spec(stack(list(atom()), integer(), string(), tuple(), binary()) ->
             ok | {error, any()}).
stack(DestNodes, AddrId, Key, Metadata, Object) ->
    stack_fun(DestNodes, AddrId, Key, Metadata, Object, []).


%% @TODO
%% Request from a remote-node
%%
-spec(request(binary()) ->
             ok | {error, any()}).
request(CompressedObjs) ->
    ?debugVal(byte_size(CompressedObjs)),
    case catch snappy:decompress(CompressedObjs) of
        {ok, Objects} ->
            ?debugVal(byte_size(Objects)),
            slice(Objects);
        {_, Cause} ->
            {error, Cause}
    end.


%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------
%% @doc Handle send object to a remote-node.
%%
handle_send(Node, CompressedObjs) ->
    ?info("handle_send/2","node:~w, size:~w", [Node, byte_size(CompressedObjs)]),

    RPCKey = rpc:async_call(Node, ?MODULE, request, [CompressedObjs]),
    case rpc:nb_yield(RPCKey, infinity) of
        {value, ok} ->
            ok;
        {value, {error, Cause}} ->
            {error, Cause};
        {value, {badrpc, Cause}} ->
            {error, Cause};
        timeout = Cause ->
            {error, Cause}
    end.


%% @doc Handle a fail process
%%
-spec(handle_fail(atom(), list({integer(), string()})) ->
             ok | {error, any()}).
handle_fail(_, []) ->
    ok;
handle_fail(Node, [{AddrId, Key}|Rest]) ->
    ?warn("handle_fail/2","node:~w, addr-id:~w, key:~s", [Node, AddrId, Key]),

    ok = leo_storage_mq_client:publish(
           ?QUEUE_TYPE_REPLICATION_MISS, AddrId, Key, ?ERR_TYPE_REPLICATE_DATA),
    handle_fail(Node, Rest).


%%--------------------------------------------------------------------
%% INNER FUNCTION
%%--------------------------------------------------------------------
%% @doc
%%
-spec(stack_fun(list(atom()), integer(), string(), tuple(), binary(), list()) ->
             ok | {error, any()}).
stack_fun([],_AddrId,_Key,_Metadata,_Object, []) ->
    ok;
stack_fun([],_AddrId,_Key,_Metadata,_Object, E) ->
    {error, lists:reverse(E)};
stack_fun([Node|Rest] = NodeList, AddrId, Key, Metadata, Object, E) ->
    case node_state(Node) of
        ok ->
            MetaBin  = term_to_binary(Metadata),
            MetaSize = byte_size(MetaBin),
            ObjSize  = byte_size(Object),
            Data = <<MetaSize:?BIN_META_SIZE, MetaBin/binary, ObjSize:?BIN_OBJ_SIZE, Object/binary, ?BIN_PADDING/binary>>,

            case leo_ordning_reda_api:stack(Node, AddrId, Key, Data) of
                ok ->
                    stack_fun(Rest, AddrId, Key, Metadata, Object, E);
                {error, undefined} ->
                    ok = start_link(Node, ?env_size_of_stacked_objs(), ?env_stacking_timeout()),
                    stack_fun(NodeList, AddrId, Key, Metadata, Object, E);
                {error, Cause} ->
                    stack_fun(Rest, AddrId, Key, Metadata, Object, [{Node, Cause}|E])
            end;
        {error, Cause} ->
            stack_fun(Rest, AddrId, Key, Metadata, Object, [{Node, Cause}|E])
    end.


%% @doc Retrieve the node state from redundant-manager and ordning-reda
%%
-spec(node_state(atom()) ->
             ok | {error, inactive}).
node_state(Node) ->
    case leo_redundant_manager_api:get_member_by_node(Node) of
        {ok, #member{state = ?STATE_RUNNING}} ->
            ok;
        _ ->
            {error, inactive}
    end.


%% @doc Slicing objects.
%%
-spec(slice(binary()) ->
             ok | {error, any()}).
slice(Objects) ->
    slice(Objects, []).

-spec(slice(binary(), list()) ->
             ok | {error, any()}).
slice(<<>>, []) ->
    ok;
slice(<<>>, Errors) ->
    {error, Errors};
slice(Objects, Errors) ->
    <<MetaSize:?BIN_META_SIZE, Rest0/binary>> = Objects,

    MetaBin = binary:part(Rest0, {0, MetaSize}),
    Rest1   = binary:part(Rest0, {MetaSize, byte_size(Rest0) - MetaSize}),

    <<ObjSize:?BIN_OBJ_SIZE, Rest2/binary>> = Rest1,

    Object  = binary:part(Rest2, {0, ObjSize}),
    Rest3   = binary:part(Rest2, {ObjSize, byte_size(Rest2) - ObjSize}),

    <<_Fotter:64, Rest4/binary>> = Rest3,

    ?debugVal(MetaSize),
    ?debugVal({MetaBin, binary_to_term(MetaBin)}),
    ?debugVal(ObjSize),
    ?debugVal(byte_size(Object)),
    
    slice(Rest4, Errors).

    %% case leo_object_storage_api:store(Metadata, Object) of
    %%     ok ->
    %%         slice(Rest4, Error);
    %%     {error, Cause} ->
    %%         %% ?warn("receive_and_store/1","cause:~p",[Cause])
    %%         slice(Rest4, [Cause|Error])
    %% end.

