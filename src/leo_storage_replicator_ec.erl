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
%% @doc Object replicator (for erasure-coding)
%% @end
%%======================================================================
-module(leo_storage_replicator_ec).

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([replicate/5, init_loop/5]).

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


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Replicate an object to local-node and remote-nodes.
%%
-spec(replicate(Method, Quorum, Nodes, Fragments, Callback) ->
             any() when Method::put|delete,
                        Quorum::pos_integer(),
                        Nodes::list(),
                        Fragments::[#?OBJECT{}],
                        Callback::function()).
replicate(Method, Quorum, Nodes, [#?OBJECT{addr_id = AddrId,
                                           key = Key,
                                           req_id = ReqId}|_] = Fragments, Callback) ->
    TotalNodes = erlang:length(Nodes),
    case (TotalNodes == erlang:length(Fragments)) of
        true ->
            NodeWithFragmentL = dict:to_list(
                                  lists:foldl(
                                    fun({#redundant_node{node = Node,
                                                         available = Available},
                                         #?OBJECT{cindex = FId} = FObj}, Acc) ->
                                            dict:append({Node, Available}, {FId, FObj}, Acc)
                                    end, dict:new(), lists:zip(Nodes, Fragments))),
            Key_1 = begin
                        {Pos,_} = lists:last(binary:matches(Key, [<<"\n">>], [])),
                        binary:part(Key, 0, Pos)
                    end,

            Ref = erlang:make_ref(),
            State = #state{method = Method,
                           addr_id = AddrId,
                           key = Key_1,
                           num_of_nodes = TotalNodes,
                           req_id = ReqId,
                           callback = Callback,
                           errors = [],
                           is_reply = false},

            case proc_lib:start(?MODULE, init_loop,
                                [TotalNodes, Quorum, Ref, erlang:self(), State]) of
                {ok, Ref, Observer} ->
                    replicate_1(NodeWithFragmentL, Ref, Observer, State);
                _Error ->
                    Callback({error, ["Failed to initialize"]})
            end;
        false ->
            Callback({error, ["Failed to initialize"]})
    end.

%% @doc
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

            %% reply error
            Cause = timeout,
            ?warn("replicate/4",
                  [{method, Method}, {key, Key}, {cause, Cause}]),
            Callback({error, [Cause]})
    end;

%% for a local-node
replicate_1([{{Node, true}, IdWithFragmentL}|Rest],
            Ref, Observer, State) when Node == erlang:node() ->
    spawn(
      fun() ->
              Ret = case leo_storage_handler_object:put({IdWithFragmentL, Ref}) of
                        {ok, Ref, {RetL, ErrorL}} ->
                            {Ref, {ok, {RetL, ErrorL}}};
                        {error, Ref, ErrorL} ->
                            {Ref, {error, {erlang:node(), ErrorL}}}
                    end,
              erlang:send(Observer, Ret)
      end),
    replicate_1(Rest, Ref, Observer, State);

%% for a remote-node
replicate_1([{{Node, true}, IdWithFragmentL}|Rest],
            Ref, Observer, #state{req_id = ReqId} = State) ->
    true = rpc:cast(Node, leo_storage_handler_object,
                    put, [Ref, Observer, IdWithFragmentL, ReqId]),
    replicate_1(Rest, Ref, Observer, State);

%% An unavailable node
replicate_1([{{Node, false}, IdWithFragmentL}|Rest], Ref, Observer, State) ->
    erlang:send(Observer, {Ref, {error, {Node, nodedown, IdWithFragmentL}}}),
    replicate_1(Rest, Ref, Observer, State).


%% @doc Initialize the receiver
init_loop(TotalNodes, Quorum, Ref, Pid, State) ->
    ok = proc_lib:init_ack(Pid, {ok, Ref, self()}),
    loop(TotalNodes, Quorum, Ref, Pid, State).


%% @doc Waiting for messages (replication)
%% @private
loop(0, 0,_Ref,_From, #state{is_reply = true}) ->
    ok;
loop(0, 0, Ref, From, #state{method = Method}) ->
    erlang:send(From, {Ref, {ok, Method, {etag, 0}}});
loop(0,_W, Ref, From, #state{errors = E}) ->
    erlang:send(From, {Ref, {error, E}});
loop(N, W, Ref, From, #state{method = Method,
                             addr_id = AddrId,
                             key = Key,
                             errors = E,
                             callback = Callback,
                             is_reply = IsReply} = State) ->
    receive
        {Ref, {ok, {RetL, ErrorL}}} ->
            NumOfFragments = erlang:length(RetL),
            W_1 = W - NumOfFragments,
            {W_2, State_1} =
                case (W_1 < 1) of
                    true when IsReply == false ->
                        erlang:send(From, {Ref, {ok, Method, {etag, 0}}}),
                        {0, State#state{is_reply = true}};
                    true ->
                        {0, State};
                    false ->
                        {W_1, State}
                end,

            %% enqueue error fragments
            {ok, {_FragmentIdL,_CauseL}} = enqueue(Method, AddrId, Key, ErrorL),
            loop(N - NumOfFragments, W_2, Ref, From, State_1);

        {Ref, {error, {Node, ErrorL}}} ->
            %% enqueue error fragments
            {ok, {_FragmentIdL, CauseL}} = enqueue(Method, AddrId, Key, ErrorL),
            State_1 = State#state{errors = [{Node, CauseL}|E]},

            NumOfFragments = erlang:length(ErrorL),
            loop(N - NumOfFragments, W, Ref, From, State_1)
    after
        ?DEF_REQ_TIMEOUT ->
            case (W > 0) of
                true ->
                    Cause = timeout,
                    ?warn("loop/5",
                          [{method, Method}, {key, Key}, {cause, Cause}]),
                    Callback({error, [Cause]});
                false ->
                    void
            end
    end.


%% @doc Enqueue error fragments
%% @private
-spec(enqueue(Method, AddrId, Key, ErrorL) ->
             {ok, {FragmentIdL, CauseL}} when Method::type_of_method(),
                                              AddrId::non_neg_integer(),
                                              Key::binary(),
                                              ErrorL::[{non_neg_integer(), any()}],
                                              FragmentIdL::[non_neg_integer()],
                                              CauseL::[any()]).
enqueue(_,_,_,[]) ->
    {ok, {[],[]}};
enqueue(Method, AddrId, Key, ErrorL) ->
    {FragmentIdL, CauseL} =
        lists:foldl(fun({FId, Cause}, {Acc_1, Acc_2}) ->
                            {[FId|Acc_1], [{FId, Cause}|Acc_2]}
                    end, {[],[]}, ErrorL),

    FragmentIdL_1 = lists:reverse(FragmentIdL),
    ok = enqueue_1(Method, AddrId, Key, FragmentIdL_1),
    {ok, {FragmentIdL, lists:reverse(CauseL)}}.

%% @private
-spec(enqueue_1(Method, AddrId, Key, FragmentIdL) ->
             ok when Method::type_of_method(),
                     AddrId::non_neg_integer(),
                     Key::binary(),
                     FragmentIdL::[non_neg_integer()]).
enqueue_1(?CMD_PUT, AddrId, Key, FragmentIdL) ->
    QId = ?QUEUE_TYPE_PER_FRAGMENT,
    case leo_storage_mq:publish(QId, AddrId, Key, FragmentIdL) of
        ok ->
            ok;
        {error, Cause} ->
            ?warn("enqueue_1/4",
                  [{qid, QId}, {addr_id, AddrId},
                   {key, Key}, {fragment_id_list, FragmentIdL},
                   {cause, Cause}]),
            ok
    end;
enqueue_1(?CMD_DELETE, AddrId, Key, FragmentIdL) ->
    QId = ?QUEUE_TYPE_PER_FRAGMENT,
    case leo_storage_mq:publish(QId, AddrId, Key, FragmentIdL) of
        ok ->
            ok;
        {error, Cause} ->
            ?warn("enqueue_1/4",
                  [{qid, QId}, {addr_id, AddrId},
                   {key, Key}, {fragment_id_list, FragmentIdL},
                   {cause, Cause}]),
            ok
    end;
enqueue_1(_,_,_,_) ->
    ok.
