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
%% ---------------------------------------------------------------------
%% LeoFS - Replicator.
%% @doc
%% @end
%%======================================================================
-module(leo_storage_replicator).

-author('Yosuke Hara').

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([replicate/5]).

-type(error_msg_type() :: ?ERR_TYPE_REPLICATE_DATA  |
                          ?ERR_TYPE_DELETE_DATA).

-record(req_params, {
          pid     :: pid(),
          addr_id :: integer(),
          key     :: binary(),
          object  :: #?OBJECT{},
          req_id  :: integer()}).

-record(state, {
          method       :: atom(),
          addr_id      :: integer(),
          key          :: binary(),
          num_of_nodes :: pos_integer(),
          callback     :: function(),
          errors = []  :: list()
         }).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Replicate an object to local-node and remote-nodes.
%%
-spec(replicate(put|delete, pos_integer(), list(), #?OBJECT{}, function()) ->
             {ok, reference()} | {error, {reference(), any()}}).
replicate(Method, Quorum, Nodes, Object, Callback) ->
    AddrId = Object#?OBJECT.addr_id,
    Key    = Object#?OBJECT.key,
    ReqId  = Object#?OBJECT.req_id,
    From = self(),

    ok = replicate_1(Nodes, From, AddrId, Key, Object, ReqId),

    loop(Quorum, From, [], #state{method       = Method,
                                  addr_id      = AddrId,
                                  key          = Key,
                                  num_of_nodes = erlang:length(Nodes),
                                  callback     = Callback,
                                  errors       = []}).

%% @private
replicate_1([],_From,_AddrId,_Key,_Object,_ReqId) ->
    ok;
replicate_1([#redundant_node{node = Node,
                             available = true}|Rest],
            From, AddrId, Key, Object, ReqId) when Node == erlang:node() ->
    spawn(fun() ->
                  replicate_fun(local, #req_params{pid     = From,
                                                   addr_id = AddrId,
                                                   key     = Key,
                                                   object  = Object,
                                                   req_id  = ReqId})
          end),
    replicate_1(Rest, From, AddrId, Key, Object, ReqId);

replicate_1([#redundant_node{node = Node,
                             available = true}|Rest],
            From, AddrId, Key, Object, ReqId) ->
    true = rpc:cast(Node, leo_storage_handler_object, put, [From, Object, ReqId]),
    replicate_1(Rest, From, AddrId, Key, Object, ReqId);

replicate_1([#redundant_node{node = Node,
                             available = false}|Rest],
            From, AddrId, Key, Object, ReqId) ->
    erlang:send(From, {error, {Node, nodedown}}),
    replicate_1(Rest, From, AddrId, Key, Object, ReqId).


%% @doc Waiting for messages (replication)
%% @private
loop(0,_From, ResL, #state{method = Method,
                           callback = Callback}) ->
    Callback({ok, Method, hd(ResL)});

loop(W,_From,_ResL, #state{num_of_nodes = NumOfNodes,
                           callback = Callback,
                           errors = Errors}) when (NumOfNodes - W) < length(Errors) ->
    Callback({error, Errors});

loop(W, From, ResL, #state{addr_id = AddrId,
                           key = Key,
                           callback = Callback,
                           errors = Errors} = State) ->
    receive
        {ok, Checksum} ->
            loop(W-1, From, [Checksum|ResL], State);
        {error, {Node, Cause}} ->
            ok = enqueue(?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
            loop(W, From, ResL, State#state{errors = [{Node, Cause}|Errors]})
    after
        ?DEF_REQ_TIMEOUT ->
            case (W >= 0) of
                true ->
                    Callback({error, timeout});
                false ->
                    void
            end
    end.


%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Request a replication-message for local-node.
%%
-spec(replicate_fun(local | atom(), #req_params{}) ->
             {ok, atom()} | {error, atom(), any()}).
replicate_fun(local, #req_params{pid     = Pid,
                                 key     = Key,
                                 object  = Object,
                                 req_id  = ReqId}) ->
    Ref  = make_ref(),
    Ret  = case leo_storage_handler_object:put(Object, Ref) of
               {ok, Ref, Checksum} ->
                   {ok, Checksum};
               {error, Ref, Cause} ->
                   ?warn("replicate_fun/2", "key:~s, node:~w, reqid:~w, cause:~p",
                         [Key, local, ReqId, Cause]),
                   {error, {node(), Cause}}
           end,
    erlang:send(Pid, Ret).


%% @doc Input a message into the queue.
%%
-spec(enqueue(error_msg_type(), integer(), string()) ->
             ok | void).
enqueue(?ERR_TYPE_REPLICATE_DATA = Type,  AddrId, Key) ->
    leo_storage_mq_client:publish(?QUEUE_TYPE_PER_OBJECT, AddrId, Key, Type);
enqueue(?ERR_TYPE_DELETE_DATA = Type,     AddrId, Key) ->
    leo_storage_mq_client:publish(?QUEUE_TYPE_PER_OBJECT, AddrId, Key, Type);
enqueue(_,_,_) ->
    void.
