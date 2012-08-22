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

-export([start_link/3, stop/1, stack/4]).
-export([handle_send/2,
         handle_fail/2]).


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
-spec(stack(list(atom()), integer(), string(), binary()) ->
             ok | {error, any()}).
stack(DestNodes, AddrId, Key, Data) ->
    stack_fun(DestNodes, AddrId, Key, Data, []).


%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------
%% @doc
%%
handle_send(Node, StackedObjects) ->
    ?info("handle_send/2","node:~w, size:~w",[Node, length(StackedObjects)]),

    RPCKey = rpc:async_call(
               Node, leo_storage_handler_object, receive_and_store, [StackedObjects]),
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


%% @doc
%%
%% @TODO
handle_fail(Node, Errors) ->
    ?info("handle_fail/2","node:~w, size:~w",[Node, length(Errors)]),
    ?debugVal({Node, length(Errors)}),
    ok.


%%--------------------------------------------------------------------
%% INNER FUNCTION
%%--------------------------------------------------------------------
%% @doc
%%
-spec(stack_fun(list(atom()), integer(), string(), binary(), list()) ->
             ok | {error, any()}).
stack_fun([],_AddrId,_Key,_Data, []) ->
    ok;
stack_fun([],_AddrId,_Key,_Data, E) ->
    {error, lists:reverse(E)};

stack_fun([Node|Rest] = NodeList, AddrId, Key, Data, E) ->
    case node_state(Node) of
        ok ->
            case leo_ordning_reda_api:stack(Node, AddrId, Key, Data) of
                ok ->
                    stack_fun(Rest, AddrId, Key, Data, E);
                {error, undefined} ->
                    ok = start_link(Node, ?env_size_of_stacked_objs(), ?env_stacking_timeout()),
                    stack_fun(NodeList, AddrId, Key, Data, E);
                {error, Cause} ->
                    stack_fun(Rest, AddrId, Key, Data, [{Node, Cause}|E])
            end;
        {error, Cause} ->
            stack_fun(Rest, AddrId, Key, Data, [{Node, Cause}|E])
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

