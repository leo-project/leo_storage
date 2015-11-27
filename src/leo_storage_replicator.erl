%%======================================================================
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
%% ---------------------------------------------------------------------
%% LeoFS - Replicator.
%% @doc
%% @end
%%======================================================================
-module(leo_storage_replicator).

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([replicate/5, init_loop/6]).

-type(error_msg_type() :: ?ERR_TYPE_REPLICATE_DATA |
                          ?ERR_TYPE_DELETE_DATA).

-record(req_params, {
          pid :: pid(),
          addr_id :: non_neg_integer(),
          key :: binary(),
          object :: #?OBJECT{},
          req_id :: integer()}).

-record(state, {
          method :: atom(),
          addr_id :: non_neg_integer(),
          key :: binary(),
          object :: #?OBJECT{},
          num_of_nodes :: pos_integer(),
          req_id :: non_neg_integer(),
          callback :: function(),
          errors = [] :: list(),
          is_reply = false ::boolean()
         }).


%% @doc Replicate/Store an object to local-node and remote-nodes.
-spec(replicate(Method, Quorum, Nodes, Object, Callback) ->
             any() when Method::put|delete,
                        Quorum::pos_integer(),
                        Nodes::list(),
                        Object:: #?OBJECT{}|[#?OBJECT{}],
                        Callback::function()).
replicate(Method, Quorum, Nodes, #?OBJECT{rep_method = ?REP_COPY,
                                          addr_id = AddrId,
                                          key = Key,
                                          req_id = ReqId} = Object, Callback) ->
    NumOfNodes = erlang:length(Nodes),
    Ref = erlang:make_ref(),
    State = #state{method = Method,
                   addr_id = AddrId,
                   key = Key,
                   object = Object,
                   num_of_nodes = NumOfNodes,
                   req_id = ReqId,
                   callback = Callback,
                   errors = [],
                   is_reply = false
                  },
    case proc_lib:start(?MODULE, init_loop,
                        [?REP_COPY, NumOfNodes, Quorum, Ref, erlang:self(), State]) of
        {ok, Ref, Observer} ->
            replicate_1(Nodes, Ref, Observer, State);
        _ ->
            Callback({error, ["Failed to initialize"]})
    end;
replicate(Method, Quorum, Nodes, [#?OBJECT{rep_method = ?REP_ERASURE_CODE,
                                           addr_id = AddrId,
                                           key = Key,
                                           req_id = ReqId}|_] = Fragments, Callback) ->
    TotalNodes = erlang:length(Nodes),
    case (TotalNodes == erlang:length(Fragments)) of
        true ->
            Fun = fun({#redundant_node{node = Node,
                                       available = Available
                                      }, #?OBJECT{fid = FId} = FObj}, Acc) ->
                          dict:append({Node, Available}, {FId, FObj}, Acc)
                  end,
            NodeWithFragmntL = dict:to_list(
                                 lists:foldl(Fun, dict:new(),
                                             lists:zip(Nodes, Fragments))),
            Ref = erlang:make_ref(),
            State = #state{method = Method,
                           addr_id = AddrId,
                           key = Key,
                           num_of_nodes = TotalNodes,
                           req_id = ReqId,
                           callback = Callback,
                           errors = [],
                           is_reply = false},
            ?debugVal({Method, Quorum, TotalNodes, Nodes, Key, Fragments}),
            ?debugVal(NodeWithFragmntL),
            ?debugVal(State),

            case proc_lib:start(?MODULE, init_loop,
                                [?REP_ERASURE_CODE, Quorum, Ref, erlang:self(), State]) of
                {ok, Ref, Observer} ->
                    replicate_2(NodeWithFragmntL, Ref, Observer, State);
                _ ->
                    Callback({error, ["Failed to initialize"]})
            end;
        false ->
            Callback({error, ["Failed to initialize"]})
    end.


%% @doc For the object-copy method
%% @private
replicate_1([], Ref,_Observer, #state{method = Method,
                                      key = Key,
                                      callback = Callback}) ->
    receive
        {Ref, Reply} ->
            Callback(Reply)
    after
        (?DEF_REQ_TIMEOUT + timer:seconds(1)) ->
            %% for watchdog
            ok = leo_storage_msg_collector:notify(?ERROR_MSG_TIMEOUT, Method, Key),
            Cause = timeout,
            ?warn("replicate_1/4", [{method, Method},
                                    {key, Key}, {cause, Cause}]),
            Callback({error, [Cause]})
    end;
replicate_1([#redundant_node{node = Node,
                             available = true}|Rest],
            Ref, Observer, #state{addr_id = AddrId,
                                  key = Key,
                                  object = Object,
                                  req_id = ReqId} = State) when Node == erlang:node() ->
    %% for a local-node
    spawn(fun() ->
                  replicate_fun(Ref, #req_params{pid = Observer,
                                                 addr_id = AddrId,
                                                 key = Key,
                                                 object = Object,
                                                 req_id = ReqId})
          end),
    replicate_1(Rest, Ref, Observer, State);
replicate_1([#redundant_node{node = Node,
                             available = true}|Rest],
            Ref, Observer, #state{object = Object,
                                  req_id = ReqId} = State) ->
    %% for a remote-node
    true = rpc:cast(Node, leo_storage_handler_object,
                    put, [Ref, Observer, Object, ReqId]),
    replicate_1(Rest, Ref, Observer, State);
replicate_1([#redundant_node{node = Node,
                             available = false}|Rest], Ref, Observer, State) ->
    %% An unavailable node
    erlang:send(Observer, {Ref, {error, {Node, nodedown}}}),
    replicate_1(Rest, Ref, Observer, State).


%% @doc For the erasure-coding method
%% @private
replicate_2([], Ref, Observer, State) ->
    replicate_1([],  Ref, Observer, State);
replicate_2([{{Node, true}, IdWithFragmentL}|Rest], Ref,
            Observer, #state{req_id = ReqId} = State) when Node == erlang:node() ->
    %% for a local-node
    spawn(fun() ->
                  Ret = case leo_storage_handler_object:put({IdWithFragmentL, Ref}) of
                            {ok, Ref, Checksum} ->
                                {Ref, {ok, Checksum, erlang:length(IdWithFragmentL)}};
                            {error, Ref, not_found = Cause} ->
                                {Ref, {error, {node(), Cause}}};
                            {error, Ref, Cause} ->
                                FIdList = [FId || {FId,_FragmentObj} <- IdWithFragmentL],
                                #?OBJECT{key = Key} = erlang:hd(IdWithFragmentL),
                                ?warn("replicate_2/4",
                                      [{key, Key}, {node, local},
                                       {fragment_id_list, FIdList},
                                       {req_id, ReqId}, {cause, Cause}]),
                                {Ref, {error, {node(), Cause}}}
                        end,
                  erlang:send(Observer, Ret)
          end),
    replicate_2(Rest, Ref, Observer, State);
replicate_2([{{Node, true}, IdWithFragmentL}|Rest], Ref,
            Observer, #state{req_id = ReqId}  = State) ->
    %% for a remote-node
    true = rpc:cast(Node, leo_storage_handler_object,
                    put, [Ref, Observer, IdWithFragmentL, ReqId]),
    replicate_2(Rest, Ref, Observer, State);
replicate_2([{{Node, false}, IdWithFragmentL}|Rest], Ref, Observer, State) ->
    %% An unavailable node
    erlang:send(Observer, {Ref, {error, {Node, nodedown, IdWithFragmentL}}}),
    replicate_2(Rest, Ref, Observer, State).


%% @doc
%% @private
init_loop(?REP_COPY, NumOfNodes, Quorum, Ref, Parent, State) ->
    ok = proc_lib:init_ack(Parent, {ok, Ref, self()}),
    loop(NumOfNodes, Quorum, [], Ref, Parent, State);

init_loop(?REP_ERASURE_CODE, NumOfNodes, Quorum, Ref, Parent, State) ->
    ok = proc_lib:init_ack(Parent, {ok, Ref, self()}),
    loop(NumOfNodes, Quorum, [], Ref, Parent, State).


%% @doc Waiting for messages (replication)
%% @private
loop(0, 0,_ResL,_Ref,_Observer, #state{is_reply = true}) ->
    ok;
loop(0, 0, ResL, Ref, Observer, #state{method = Method}) ->
    erlang:send(Observer, {Ref, {ok, Method, hd(ResL)}});
loop(_, W,_ResL, Ref, Observer, #state{num_of_nodes = N,
                                       errors = E}) when (N - W) < length(E) ->
    erlang:send(Observer, {Ref, {error, E}});
loop(N, W, ResL, Ref, Observer, #state{method = Method,
                                       addr_id = AddrId,
                                       key = Key,
                                       errors = E,
                                       callback = Callback,
                                       is_reply = IsReply} = State) ->
    receive
        %%
        %% Regular cases
        %%
        {Ref, {ok, Checksum}} ->
            ResL_1 = [Checksum|ResL],
            {W_1, State_1} =
                case ((W - 1) < 1) of
                    true when IsReply == false ->
                        erlang:send(Observer, {Ref, {ok, Method, erlang:hd(ResL_1)}}),
                        {0, State#state{is_reply = true}};
                    true ->
                        {0, State};
                    false ->
                        {W - 1, State}
                end,
            loop(N - 1, W_1, ResL_1, Ref, Observer, State_1);
        %%
        %% Error cases
        %%
        {Ref, {error, {_Node, not_found}}} when Method == 'delete'->
            {W_1, State_1} =
                case ((W - 1) < 1) of
                    true when IsReply == false ->
                        erlang:send(Observer, {Ref, {ok, Method, 0}}),
                        {0, State#state{is_reply = true}};
                    true ->
                        {0, State};
                    false ->
                        {W - 1, State}
                end,
            loop(N - 1, W_1, [0|ResL], Ref, Observer, State_1);
        {Ref, {error, {Node, Cause}}} ->
            enqueue(Method, ?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
            State_1 = State#state{errors = [{Node, Cause}|E]},
            loop(N-1, W, ResL, Ref, Observer, State_1);
        {Ref, {error, {Node, Cause, IdWithFragmentL}}} ->
            %% @TODO:
            FIdList = [FId || {FId,_FObj} <- IdWithFragmentL],
            enqueue(Method, ?ERR_TYPE_STORE_FRAGMENT, AddrId, Key, FIdList),
            State_1 = State#state{errors = [{Node, Cause}|E]},
            loop(N - 1, W, ResL, Ref, Observer, State_1)

    after
        ?DEF_REQ_TIMEOUT ->
            case (W > 0) of
                true ->
                    %% for recovering message of the repair-obj's MQ
                    enqueue(Method, ?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
                    %% set reply
                    Cause = timeout,
                    ?warn("loop/6",
                          [{method, Method}, {key, Key}, {cause, Cause}]),
                    Callback({error, [Cause]});
                false ->
                    void
            end
    end.


%% @doc Request a replication-message for local-node.
%% @private
-spec(replicate_fun(reference(), #req_params{}) ->
             {ok, atom()} | {error, atom(), any()}).
replicate_fun(Ref, #req_params{pid = Pid,
                               key = Key,
                               object = Object,
                               req_id = ReqId}) ->
    %% Ref  = make_ref(),
    Ret = case leo_storage_handler_object:put({Object, Ref}) of
              {ok, Ref, Checksum} ->
                  {Ref, {ok, Checksum}};
              {error, Ref, not_found = Cause} ->
                  {Ref, {error, {node(), Cause}}};
              {error, Ref, Cause} ->
                  ?warn("replicate_fun/2",
                        [{key, Key}, {node, local},
                         {req_id, ReqId}, {cause, Cause}]),
                  {Ref, {error, {node(), Cause}}}
          end,
    erlang:send(Pid, Ret).


%% @doc Input a message into the queue.
%% @private
-spec(enqueue(Method, Type, AddrId, Key) ->
             ok when Method::type_of_method(),
                     Type::error_msg_type(),
                     AddrId::non_neg_integer(),
                     Key::binary()).
enqueue(?CMD_PUT, ?ERR_TYPE_REPLICATE_DATA = Type,  AddrId, Key) ->
    QId = ?QUEUE_TYPE_PER_OBJECT,
    case leo_storage_mq:publish(QId, AddrId, Key, Type) of
        ok ->
            ok;
        {error, Cause} ->
            ?warn("enqueue/1",
                  [{qid, QId}, {addr_id, AddrId},
                   {key, Key}, {type, Type}, {cause, Cause}])
    end;
enqueue(?CMD_DELETE, ?ERR_TYPE_REPLICATE_DATA, AddrId, Key) ->
    QId = ?QUEUE_TYPE_ASYNC_DELETE_OBJ,
    case leo_storage_mq:publish(QId, AddrId, Key) of
        ok ->
            ok;
        {error, Cause} ->
            ?warn("enqueue/1",
                  [{qid, QId}, {addr_id, AddrId},
                   {key, Key}, {cause, Cause}])
    end.

%% @private
enqueue(?CMD_PUT, ?ERR_TYPE_STORE_FRAGMENT,  AddrId, Key, FragmentIdL) ->
    QId = ?QUEUE_TYPE_PER_FRAGMENT,
    case leo_storage_mq:publish(QId, AddrId, Key, FragmentIdL) of
        ok ->
            ok;
        {error, Cause} ->
            ?warn("enqueue/1",
                  [{qid, QId}, {addr_id, AddrId},
                   {key, Key}, {fragment_id_list, FragmentIdL},
                   {cause, Cause}])
    end;
enqueue(?CMD_DELETE, ?ERR_TYPE_STORE_FRAGMENT, AddrId, Key, FragmentIdL) ->
    QId = ?QUEUE_TYPE_PER_FRAGMENT,
    case leo_storage_mq:publish(QId, AddrId, Key, FragmentIdL) of
        ok ->
            ok;
        {error, Cause} ->
            ?warn("enqueue/1",
                  [{qid, QId}, {addr_id, AddrId},
                   {key, Key}, {fragment_id_list, FragmentIdL},
                   {cause, Cause}])
    end.
