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
          addr_id :: non_neg_integer(),
          key     :: binary(),
          object  :: #?OBJECT{},
          req_id  :: integer()}).

-record(state, {
          method       :: atom(),
          addr_id      :: non_neg_integer(),
          key          :: binary(),
          num_of_nodes :: pos_integer(),
          callback     :: function(),
          errors = []  :: list(),
          is_reply = false ::boolean()
         }).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Replicate an object to local-node and remote-nodes.
%%
-spec(replicate(Method, Quorum, Nodes, Object, Callback) ->
             any() when Method::put|delete,
                        Quorum::pos_integer(),
                        Nodes::list(),
                        Object:: #?OBJECT{},
                        Callback::function()).
replicate(Method, Quorum, Nodes, Object, Callback) ->
    AddrId = Object#?OBJECT.addr_id,
    Key    = Object#?OBJECT.key,
    ReqId  = Object#?OBJECT.req_id,
    NumOfNodes = erlang:length(Nodes),

    Ref  = make_ref(),
    From = self(),
    Pid  = spawn(fun() ->
                         loop(NumOfNodes, Quorum, [],
                              Ref, From, #state{method       = Method,
                                                addr_id      = AddrId,
                                                key          = Key,
                                                num_of_nodes = NumOfNodes,
                                                callback     = Callback,
                                                errors       = [],
                                                is_reply     = false
                                               })
                 end),

    ok = replicate_1(Nodes, Ref, Pid, AddrId, Key, Object, ReqId),
    receive
        {Ref, Reply} ->
            Callback(Reply)
    after
        (?DEF_REQ_TIMEOUT + timer:seconds(1)) ->
            Cause = timeout,
            ?warn("replicate/5",
                  "method:~w, key:~p, cause:~p",
                  [Method, Key, Cause]),
            Callback({error, Cause})
    end.

%% @private
replicate_1([],_Ref,_From,_AddrId,_Key,_Object,_ReqId) ->
    ok;
%% for local-node
replicate_1([#redundant_node{node = Node,
                             available = true}|Rest],
            Ref, From, AddrId, Key, Object, ReqId) when Node == erlang:node() ->
    spawn(fun() ->
                  replicate_fun(Ref, #req_params{pid     = From,
                                                 addr_id = AddrId,
                                                 key     = Key,
                                                 object  = Object,
                                                 req_id  = ReqId})
          end),
    replicate_1(Rest, Ref, From, AddrId, Key, Object, ReqId);
%% for remote-node
replicate_1([#redundant_node{node = Node,
                             available = true}|Rest],
            Ref, From, AddrId, Key, Object, ReqId) ->
    true = rpc:cast(Node, leo_storage_handler_object, put, [Ref, From, Object, ReqId]),
    replicate_1(Rest, Ref, From, AddrId, Key, Object, ReqId);
%% for unavailable node
replicate_1([#redundant_node{node = Node,
                             available = false}|Rest],
            Ref, From, AddrId, Key, Object, ReqId) ->
    erlang:send(From, {Ref, {error, {Node, nodedown}}}),
    replicate_1(Rest, Ref, From, AddrId, Key, Object, ReqId).


%% @doc Waiting for messages (replication)
%% @private
loop(0, 0,_ResL,_Ref,_From, #state{is_reply = true}) ->
    ok;
loop(0, 0, ResL, Ref, From, #state{method = Method}) ->
    erlang:send(From, {Ref, {ok, Method, hd(ResL)}});
loop(_, W,_ResL, Ref, From, #state{num_of_nodes = N,
                                   errors = E}) when (N - W) < length(E) ->
    erlang:send(From, {Ref, {error, E}});
loop(N, W, ResL, Ref, From, #state{method = Method,
                                   addr_id = AddrId,
                                   key = Key,
                                   errors = E,
                                   is_reply = IsReply} = State) ->
    receive
        {Ref, {ok, Checksum}} ->
            ResL_1 = [Checksum|ResL],
            {W_1, State_1} =
                case ((W - 1) < 1) of
                    true when IsReply == false ->
                        erlang:send(From, {Ref, {ok, Method, hd(ResL_1)}}),
                        {0, State#state{is_reply = true}};
                    true ->
                        {0, State};
                    false ->
                        {W - 1, State}
                end,
            loop(N-1, W_1, ResL_1, Ref, From, State_1);
        {Ref, {error, {_Node, not_found}}} when Method == 'delete'->
            {W_1, State_1} =
                case ((W - 1) < 1) of
                    true when IsReply == false ->
                        erlang:send(From, {Ref, {ok, Method, 0}}),
                        {0, State#state{is_reply = true}};
                    true ->
                        {0, State};
                    false ->
                        {W - 1, State}
                end,
            loop(N-1, W_1, [0|ResL], Ref, From, State_1);
        {Ref, {error, {Node, Cause}}} ->
            ?warn("loop/6", "method:~w, node:~w, key:~p, cause:~p",
                  [Method, Node, Key, Cause]),
            _ = enqueue( Method, ?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
            State_1 = State#state{errors = [{Node, Cause}|E]},
            loop(N-1, W, ResL, Ref, From, State_1)
    after
        ?DEF_REQ_TIMEOUT ->
            case (W >= 0) of
                true ->
                    Cause = timeout,
                   ?warn("loop/6",
                         "method:~w, key:~p, cause:~p",
                         [Method, Key, Cause]),
                    erlang:send(From, {Ref, {error, Cause}});
                false ->
                    void
            end
    end.


%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Request a replication-message for local-node.
%%
-spec(replicate_fun(reference(), #req_params{}) ->
             {ok, atom()} | {error, atom(), any()}).
replicate_fun(Ref, #req_params{pid     = Pid,
                               key     = Key,
                               object  = Object,
                               req_id  = ReqId}) ->
    %% Ref  = make_ref(),
    Ret  = case leo_storage_handler_object:put({Object, Ref}) of
               {ok, Ref, Checksum} ->
                   {Ref, {ok, Checksum}};
               {error, Ref, not_found = Cause} ->
                   {Ref, {error, {node(), Cause}}};
               {error, Ref, Cause} ->
                   ?warn("replicate_fun/2",
                         "key:~s, node:~w, reqid:~w, cause:~p",
                         [Key, local, ReqId, Cause]),
                   {Ref, {error, {node(), Cause}}}
           end,
    erlang:send(Pid, Ret).


%% @doc Input a message into the queue.
%%
-spec(enqueue(Method, Type, AddrId, Key) ->
             ok when Method::type_of_method(),
                     Type::error_msg_type(),
                     AddrId::non_neg_integer(),
                     Key::binary()).
enqueue('put', ?ERR_TYPE_REPLICATE_DATA = Type,  AddrId, Key) ->
    QId = ?QUEUE_TYPE_PER_OBJECT,
    case leo_storage_mq:publish(QId, AddrId, Key, Type) of
        ok ->
            ok;
        {error, Cause} ->
            ?warn("enqueue/1", "qid:~p, addr-id:~p, key:~p, type:~p, cause:~p",
                  [QId, AddrId, Key, Type, Cause])
    end;
enqueue('delete', _Type,  AddrId, Key) ->
    QId = ?QUEUE_TYPE_ASYNC_DELETION,
    case leo_storage_mq:publish(QId, AddrId, Key) of
        ok ->
            ok;
        {error, Cause} ->
            ?warn("enqueue/1", "qid:~p, addr-id:~p, key:~p, cause:~p",
                  [QId, AddrId, Key, Cause])
    end.
