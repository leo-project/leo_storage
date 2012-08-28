%%====================================================================
%%
%% LeoFS Storage
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
%% -------------------------------------------------------------------
%% LeoFS Storage - MQ Client
%% @doc
%% @end
%%====================================================================
-module(leo_storage_mq_client).

-author('Yosuke Hara').

-include("leo_storage.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_mq/include/leo_mq.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").

-export([start/1, publish/3, publish/4, publish/5, subscribe/2]).

-define(SLASH, "/").
-define(MSG_PATH_REPLICATION_MISS,  "0").
-define(MSG_PATH_INCONSISTENT_DATA, "1").
-define(MSG_PATH_SYNC_VNODE_ID,     "2").
-define(MSG_PATH_REBALANCE,         "3").

-type(queue_type() :: ?QUEUE_TYPE_REPLICATION_MISS  |
                      ?QUEUE_TYPE_INCONSISTENT_DATA |
                      ?QUEUE_TYPE_SYNC_BY_VNODE_ID).

-type(queue_id()   :: ?QUEUE_ID_REPLICATE_MISS |
                      ?QUEUE_ID_INCONSISTENT_DATA |
                      ?QUEUE_ID_SYNC_BY_VNODE_ID |
                      ?QUEUE_ID_REBALANCE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc create queues and launch mq-servers.
%%
-spec(start(string()) ->
             ok | {error, any()}).
start(RootPath0) ->
    RootPath1 =
        case (string:len(RootPath0) == string:rstr(RootPath0, ?SLASH)) of
            true  -> RootPath0;
            false -> RootPath0 ++ ?SLASH
        end,

    ?TBL_REBALANCE_COUNTER = ets:new(?TBL_REBALANCE_COUNTER,
                                     [named_table, public, {read_concurrency, true}]),
    lists:foreach(
      fun({Id, Path, MaxInterval, MinInterval}) ->
              leo_mq_api:new(Id, [{?MQ_PROP_MOD,          ?MODULE},
                                  {?MQ_PROP_FUN,          ?MQ_SUBSCRIBE_FUN},
                                  {?MQ_PROP_ROOT_PATH,    RootPath1 ++ Path},
                                  {?MQ_PROP_DB_PROCS,     ?env_num_of_mq_procs()},
                                  {?MQ_PROP_MAX_INTERVAL, MaxInterval},
                                  {?MQ_PROP_MIN_INTERVAL, MinInterval}
                                 ])

      end, [{?QUEUE_ID_REPLICATE_MISS,    ?MSG_PATH_REPLICATION_MISS,  64, 16},
            {?QUEUE_ID_INCONSISTENT_DATA, ?MSG_PATH_INCONSISTENT_DATA, 64, 16},
            {?QUEUE_ID_SYNC_BY_VNODE_ID,  ?MSG_PATH_SYNC_VNODE_ID,     32,  8},
            {?QUEUE_ID_REBALANCE,         ?MSG_PATH_REBALANCE,         32,  8}
           ]),
    ok.


%% -------------------------------------------------------------------
%% Publish
%% -------------------------------------------------------------------
%% @doc Input a message into the queue.
%%
-spec(publish(queue_type(), integer(), atom()) ->
             ok).
publish(?QUEUE_TYPE_SYNC_BY_VNODE_ID, VNodeId, Node) ->
    KeyBin     = term_to_binary(VNodeId),
    MessageBin = term_to_binary(#sync_unit_of_vnode_message{id        = leo_utils:clock(),
                                                            vnode_id  = VNodeId,
                                                            node      = Node,
                                                            timestamp = leo_utils:now()}),
    leo_mq_api:publish(?QUEUE_ID_SYNC_BY_VNODE_ID, KeyBin, MessageBin);
publish(_,_,_) ->
    {error, badarg}.

-spec(publish(queue_type(), integer(), any(), any()) ->
             ok).
publish(?QUEUE_TYPE_REPLICATION_MISS, AddrId, Key, ErrorType) ->
    KeyBin = term_to_binary({ErrorType, Key}),
    PublishedAt = leo_utils:now(),
    MessageBin  = term_to_binary(#inconsistent_data_message{id        = leo_utils:clock(),
                                                            type      = ErrorType,
                                                            addr_id   = AddrId,
                                                            key       = Key,
                                                            timestamp = PublishedAt}),
    leo_mq_api:publish(?QUEUE_ID_REPLICATE_MISS, KeyBin, MessageBin);

publish(?QUEUE_TYPE_INCONSISTENT_DATA, AddrId, Key, ErrorType) ->
    KeyBin = term_to_binary({ErrorType, Key}),
    PublishedAt = leo_utils:now(),
    MessageBin  = term_to_binary(#inconsistent_data_message{id        = leo_utils:clock(),
                                                            type      = ErrorType,
                                                            addr_id   = AddrId,
                                                            key       = Key,
                                                            timestamp = PublishedAt}),
    leo_mq_api:publish(?QUEUE_ID_INCONSISTENT_DATA, KeyBin, MessageBin);

publish(_,_,_,_) ->
    {error, badarg}.

publish(?QUEUE_TYPE_REBALANCE, Node, VNodeId, AddrId, Key) ->
    KeyBin     = term_to_binary({Node, AddrId, Key}),
    MessageBin = term_to_binary(#rebalance_message{id        = leo_utils:clock(),
                                                   vnode_id  = VNodeId,
                                                   addr_id   = AddrId,
                                                   key       = Key,
                                                   node      = Node,
                                                   timestamp = leo_utils:now()}),
    Table = ?TBL_REBALANCE_COUNTER,
    case ets_lookup(Table, VNodeId) of
        {ok, 0} -> ets:insert(Table, {VNodeId, 0});
        _Other  -> void
    end,
    ok = increment_counter(Table, VNodeId),
    leo_mq_api:publish(?QUEUE_ID_REBALANCE, KeyBin, MessageBin);

publish(_,_,_,_,_) ->
    {error, badarg}.


%% -------------------------------------------------------------------
%% Subscribe.
%% -------------------------------------------------------------------
%% @doc Subscribe a message from the queue.
%%
-spec(subscribe(queue_id() , binary()) ->
             ok | {error, any()}).
subscribe(Id, MessageBin) when Id == ?QUEUE_ID_REPLICATE_MISS;
                               Id == ?QUEUE_ID_INCONSISTENT_DATA ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            {error, Cause};
        #inconsistent_data_message{addr_id = AddrId,
                                   key     = Key,
                                   type    = ErrorType} ->
            case correct_redundancies(Key) of
                ok ->
                    ok;
                Error ->
                    Type = case Id of
                               ?QUEUE_ID_REPLICATE_MISS    -> ?QUEUE_TYPE_REPLICATION_MISS;
                               ?QUEUE_ID_INCONSISTENT_DATA -> ?QUEUE_TYPE_INCONSISTENT_DATA
                           end,
                    publish(Type, AddrId, Key, ErrorType),
                    Error
            end
    end;


subscribe(?QUEUE_ID_SYNC_BY_VNODE_ID, MessageBin) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            {error, Cause};
        #sync_unit_of_vnode_message{vnode_id = ToVNodeId,
                                    node     = Node} ->
            case leo_redundant_manager_api:range_of_vnodes(ToVNodeId) of
                {ok, Res} ->
                    {ok, {CurRingHash, _PrevRingHash}} =
                        leo_redundant_manager_api:checksum(?CHECKSUM_RING),
                    ok = sync_vnodes(Node, CurRingHash, Res),
                    notify_rebalance_message_to_manager(ToVNodeId);
                Error ->
                    Error
            end
    end;


subscribe(?QUEUE_ID_REBALANCE, MessageBin) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            {error, Cause};
        #rebalance_message{node     = Node,
                           vnode_id = VNodeId,
                           addr_id  = AddrId,
                           key      = Key} ->
            ok = decrement_counter(?TBL_REBALANCE_COUNTER, VNodeId),
            case leo_storage_handler_object:copy([Node], AddrId, Key) of
                ok ->
                    ok;
                Error ->
                    ok = leo_storage_mq_client:publish(
                           ?QUEUE_TYPE_REPLICATION_MISS, AddrId, Key, ?ERR_TYPE_REPLICATE_DATA),
                    Error
            end
    end.


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc synchronize by vnode-id.
%%
-spec(sync_vnodes(atom(), integer(), list()) ->
             ok).
sync_vnodes(_, _, []) ->
    ok;
sync_vnodes(Node, RingHash, [{FromAddrId, ToAddrId}|T]) ->
    Fun = fun(K, V, Acc) ->
                  {AddrId, Key} = binary_to_term(K),
                  Metadata      = binary_to_term(V),

                  %% Note: An object of copy is NOT equal current ring-hash.
                  %%       Then a message in the rebalance-queue.
                  case (AddrId >= FromAddrId andalso
                        AddrId =< ToAddrId) of
                      true when Metadata#metadata.ring_hash =/= RingHash ->
                          VNodeId = ToAddrId,

                          case leo_redundant_manager_api:get_redundancies_by_addr_id(put, AddrId) of
                              {ok, #redundancies{nodes = Redundancies}} ->
                                  Nodes = [N || {N, _} <- Redundancies],
                                  case lists:member(Node, Nodes) of
                                      true ->
                                          VNodeId = ToAddrId,
                                          ?MODULE:publish(?QUEUE_TYPE_REBALANCE, Node, VNodeId, AddrId, Key);
                                      false ->
                                          void
                                  end,
                                  {ok, Acc};
                              _ ->
                                  {ok, Acc}
                          end;
                      true  -> {ok, Acc};
                      false -> {ok, Acc}
                  end
          end,

    _ = leo_object_storage_api:fetch_by_addr_id(FromAddrId, Fun),
    _ = notify_message_to_manager(?env_manager_nodes(leo_storage), ToAddrId, erlang:node()),
    sync_vnodes(Node, RingHash, T).


%% @doc Notify a message to manager node(s)
%%
-spec(notify_message_to_manager(list(), integer(), atom()) ->
             ok | {error, any()}).
notify_message_to_manager([],_VNodeId,_Node) ->
    {error, 'fail_notification'};
notify_message_to_manager([Manager|T], VNodeId, Node) ->
    Res = case rpc:call(list_to_atom(Manager), leo_manager_api, notify,
                        [synchronized, VNodeId, Node], ?DEF_REQ_TIMEOUT) of
              ok ->
                  ok;
              {_, Cause} ->
                  ?warn("notify_message_to_manager/3","vnode-id:~w, node:~w, cause:~p",
                        [VNodeId, Node, Cause]),
                  {error, Cause};
              timeout = Cause ->
                  ?warn("notify_message_to_manager/3","vnode-id:~w, node:~w, cause:~p",
                        [VNodeId, Node, Cause]),
                  {error, Cause}
          end,

    case Res of
        ok ->
            ok;
        _Error ->
            notify_message_to_manager(T, VNodeId, Node)
    end.


%% @doc correct_redundancies/1 - first.
%%
-spec(correct_redundancies(string()) ->
             ok | {error, any()}).
correct_redundancies(Key) ->
    {ok, #redundancies{nodes = Redundancies,
                       id    = AddrId}} = leo_redundant_manager_api:get_redundancies_by_key(Key),
    correct_redundancies1(Key, AddrId, Redundancies, [], []).

%% correct_redundancies1/5 - next.
%%
-spec(correct_redundancies1(string(), integer(), list(), list(), list()) ->
             ok | {error, any()}).
correct_redundancies1(_Key,_AddrId, [], [], _ErrorNodes) ->
    {error, not_found};

correct_redundancies1(_Key,_AddrId, [], Metadatas, ErrorNodes) ->
    correct_redundancies2(Metadatas, ErrorNodes);

correct_redundancies1(Key, AddrId, [{Node, _}|T], Metadatas, ErrorNodes) ->
    RPCKey = rpc:async_call(Node, leo_storage_handler_object, head, [AddrId, Key]),

    case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
        {value, {ok, Metadata}} ->
            correct_redundancies1(Key, AddrId, T, [{Node, Metadata}|Metadatas], ErrorNodes);
        _Error ->
            correct_redundancies1(Key, AddrId, T, Metadatas, [Node|ErrorNodes])
    end.


%% correct_redundancies2/3
%%
-spec(correct_redundancies2(list(), list()) ->
             ok | {error, any()}).
correct_redundancies2(ListOfMetadata, ErrorNodes) ->
    [{_, Metadata} = H|_] = lists:sort(fun({_, M1}, {_, M2}) ->
                                               M1#metadata.clock >= M2#metadata.clock;
                                          (_,_) ->
                                               false
                                       end, ListOfMetadata),

    {_, CorrectNodes, InconsistentNodes} =
        lists:foldl(
          fun({Node, _},
              {{DestNode, _Metadata} = Dest, C, R}) when Node =:= DestNode ->
                  {Dest, [Node|C], R};
             ({Node, #metadata{clock = Clock}},
              {{DestNode, #metadata{clock = DestClock}} = Dest, C, R}) when Node  =/= DestNode,
                                                                            Clock =:= DestClock ->
                  {Dest, [Node|C], R};
             ({Node, #metadata{clock = Clock}},
              {{DestNode, #metadata{clock = DestClock}} = Dest, C, R}) when Node  =/= DestNode,
                                                                            Clock =/= DestClock ->
                  {Dest, C, [Node|R]}
          end, {H, [], []}, ListOfMetadata),

    correct_redundancies3(ErrorNodes ++ InconsistentNodes, CorrectNodes, Metadata).


%% correct_redundancies3/4 - last.
%%
-spec(correct_redundancies3(list(), list(), #metadata{}) ->
             ok | {error, any()}).
correct_redundancies3([], _, _) ->
    ok;
correct_redundancies3(_, [], _) ->
    {error, 'could not fix inconsistency'};
correct_redundancies3(InconsistentNodes, [Node|Rest], Metadata) ->
    RPCKey = rpc:async_call(Node, leo_storage_api, synchronize,
                            [?TYPE_OBJ, InconsistentNodes, Metadata]),
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
              {value, ok} ->
                  ok;
              {value, {error, Cause}} ->
                  {error, Cause};
              {value, {badrpc = Cause, _}} ->
                  {error, Cause};
              timeout = Cause->
                  {error, Cause}
          end,

    case Ret of
        ok ->
            Ret;
        {error, _Why} ->
            correct_redundancies3(InconsistentNodes, Rest, Metadata)
    end.


%% @doc Notify a rebalance-progress messages to manager.
%%      - Retrieve # of published messages for rebalance,
%%        after notify a message to manager.
%%
-spec(notify_rebalance_message_to_manager(integer()) ->
             ok | {error, any()}).
notify_rebalance_message_to_manager(VNodeId) ->
    case ets_lookup(?TBL_REBALANCE_COUNTER, VNodeId) of
        {ok, NumOfMessages} ->
            lists:foldl(fun(_Manager, true) ->
                                void;
                           (Manager0, false = Res) ->
                                Manager1 = case is_atom(Manager0) of
                                               true  -> Manager0;
                                               false -> list_to_atom(Manager0)
                                           end,

                                case catch rpc:call(Manager1, leo_manager_api, notify,
                                                    [rebalance, VNodeId, erlang:node(), NumOfMessages],
                                                    ?DEF_REQ_TIMEOUT) of
                                    ok ->
                                        true;
                                    {_, Cause} ->
                                        ?error("notify_rebalance_message_to_manager/1",
                                               "manager:~p, vnode_id:~w, ~ncause:~p",
                                               [Manager1, VNodeId, Cause]),
                                        Res;
                                    timeout = Cause ->
                                        ?error("notify_rebalance_message_to_manager/1",
                                               "manager:~p, vnode_id:~w, ~ncause:~p",
                                               [Manager1, VNodeId, Cause]),
                                        Res
                                end
                        end, false, ?env_manager_nodes(leo_storage)),
            ok;
        Error ->
            Error
    end.


-spec(ets_lookup(atom(), integer()) ->
             list() | {error, any()}).
ets_lookup(Table, Key) ->
    case catch ets:lookup(Table, Key) of
        [] ->
            {ok, 0};
        [{_Key, Value}] ->
            {ok, Value};
        {'EXIT', Cause} ->
            {error, Cause}
    end.


increment_counter(Table, Key) ->
    catch ets:update_counter(Table, Key, 1),
    ok.

decrement_counter(Table, Key) ->
    catch ets:update_counter(Table, Key, -1),
    ok.

