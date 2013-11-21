%%====================================================================
%%
%% LeoFS Storage
%%
%% Copyright (c) 2012-2013 Rakuten, Inc.
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

-behaviour(leo_mq_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_mq/include/leo_mq.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start/2, start/3,
         publish/2, publish/3, publish/4, publish/5]).
-export([init/0, handle_call/1]).

-define(SLASH, "/").
-define(MSG_PATH_PER_OBJECT,     "1").
-define(MSG_PATH_SYNC_VNODE_ID,  "2").
-define(MSG_PATH_REBALANCE,      "3").
-define(MSG_PATH_ASYNC_DELETION, "4").
-define(MSG_PATH_RECOVERY_NODE,  "5").

-type(queue_type() :: ?QUEUE_TYPE_PER_OBJECT  |
                      ?QUEUE_TYPE_SYNC_BY_VNODE_ID  |
                      ?QUEUE_TYPE_ASYNC_DELETION |
                      ?QUEUE_TYPE_RECOVERY_NODE).

-type(queue_id()   :: ?QUEUE_ID_PER_OBJECT |
                      ?QUEUE_ID_SYNC_BY_VNODE_ID |
                      ?QUEUE_ID_REBALANCE |
                      ?QUEUE_ID_ASYNC_DELETION |
                      ?QUEUE_ID_RECOVERY_NODE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc create queues and launch mq-servers.
%%
-spec(start(string(), list(tuple())) ->
             ok | {error, any()}).
start(RootPath0, Intervals) ->
    start(leo_storage_sup, Intervals, RootPath0).

-spec(start(pid(), list(tuple()), string()) ->
             ok | {error, any()}).
start(RefSup, Intervals, RootPath0) ->
    %% launch mq-sup under storage-sup
    RefMqSup =
        case whereis(leo_mq_sup) of
            undefined ->
                ChildSpec = {leo_mq_sup,
                             {leo_mq_sup, start_link, []},
                             permanent, 2000, supervisor, [leo_mq_sup]},
                {ok, Pid} = supervisor:start_child(RefSup, ChildSpec),
                Pid;
            Pid ->
                Pid
        end,

    %% launch queue-processes
    RootPath1 =
        case (string:len(RootPath0) == string:rstr(RootPath0, ?SLASH)) of
            true  -> RootPath0;
            false -> RootPath0 ++ ?SLASH
        end,

    ?TBL_REBALANCE_COUNTER = ets:new(?TBL_REBALANCE_COUNTER,
                                     [named_table, public, {read_concurrency, true}]),
    lists:foreach(
      fun({Id, Path, MaxInterval, MinInterval}) ->
              leo_mq_api:new(RefMqSup, Id, [{?MQ_PROP_MOD,          ?MODULE},
                                            {?MQ_PROP_FUN,          ?MQ_SUBSCRIBE_FUN},
                                            {?MQ_PROP_ROOT_PATH,    RootPath1 ++ Path},
                                            {?MQ_PROP_DB_PROCS,     ?env_num_of_mq_procs()},
                                            {?MQ_PROP_MAX_INTERVAL, MaxInterval},
                                            {?MQ_PROP_MIN_INTERVAL, MinInterval}
                                           ])
      end, [{?QUEUE_ID_PER_OBJECT, ?MSG_PATH_PER_OBJECT,
             leo_misc:get_value(cns_interval_per_object_max, Intervals, ?DEF_MQ_INTERVAL_MAX),
             leo_misc:get_value(cns_interval_per_object_min, Intervals, ?DEF_MQ_INTERVAL_MIN)
            },
            {?QUEUE_ID_SYNC_BY_VNODE_ID, ?MSG_PATH_SYNC_VNODE_ID,
             leo_misc:get_value(cns_interval_sync_by_vnode_id_max, Intervals, ?DEF_MQ_INTERVAL_MAX),
             leo_misc:get_value(cns_interval_sync_by_vnode_id_min, Intervals, ?DEF_MQ_INTERVAL_MIN)
            },
            {?QUEUE_ID_REBALANCE, ?MSG_PATH_REBALANCE,
             leo_misc:get_value(cns_interval_rebalance_max, Intervals, ?DEF_MQ_INTERVAL_MAX),
             leo_misc:get_value(cns_interval_rebalance_min, Intervals, ?DEF_MQ_INTERVAL_MIN)
            },
            {?QUEUE_ID_ASYNC_DELETION, ?MSG_PATH_ASYNC_DELETION,
             leo_misc:get_value(cns_interval_async_deletion_max, Intervals, ?DEF_MQ_INTERVAL_MAX),
             leo_misc:get_value(cns_interval_async_deletion_min, Intervals, ?DEF_MQ_INTERVAL_MIN)
            },
            {?QUEUE_ID_RECOVERY_NODE, ?MSG_PATH_RECOVERY_NODE,
             leo_misc:get_value(cns_interval_recovery_node_max, Intervals, ?DEF_MQ_INTERVAL_MAX),
             leo_misc:get_value(cns_interval_recovery_node_min, Intervals, ?DEF_MQ_INTERVAL_MIN)
            }]),
    ok.

%% @doc Input a message into the queue.
%%
-spec(publish(queue_type(), atom()) ->
             ok).
publish(?QUEUE_TYPE_RECOVERY_NODE = Id, Node) ->
    KeyBin     = term_to_binary(Node),
    MessageBin = term_to_binary(#recovery_node_message{id        = leo_date:clock(),
                                                       node      = Node,
                                                       timestamp = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);
publish(_,_) ->
    {error, badarg}.

-spec(publish(queue_type(), integer(), atom()) ->
             ok).
publish(?QUEUE_TYPE_SYNC_BY_VNODE_ID = Id, VNodeId, Node) ->
    KeyBin     = term_to_binary(VNodeId),
    MessageBin = term_to_binary(#sync_unit_of_vnode_message{id        = leo_date:clock(),
                                                            vnode_id  = VNodeId,
                                                            node      = Node,
                                                            timestamp = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(?QUEUE_TYPE_ASYNC_DELETION = Id, AddrId, Key) ->
    KeyBin     = term_to_binary({AddrId, Key}),
    MessageBin = term_to_binary(#async_deletion_message{id        = leo_date:clock(),
                                                        addr_id   = AddrId,
                                                        key       = Key,
                                                        timestamp = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(_,_,_) ->
    {error, badarg}.

-spec(publish(queue_type(), integer(), any(), any()) ->
             ok).
publish(?QUEUE_TYPE_PER_OBJECT = Id, AddrId, Key, ErrorType) ->
    KeyBin = term_to_binary({ErrorType, Key}),
    PublishedAt = leo_date:now(),
    MessageBin  = term_to_binary(#inconsistent_data_message{id        = leo_date:clock(),
                                                            type      = ErrorType,
                                                            addr_id   = AddrId,
                                                            key       = Key,
                                                            timestamp = PublishedAt}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(_,_,_,_) ->
    {error, badarg}.

publish(?QUEUE_TYPE_REBALANCE = Id, Node, VNodeId, AddrId, Key) ->
    KeyBin     = term_to_binary({Node, AddrId, Key}),
    MessageBin = term_to_binary(#rebalance_message{id        = leo_date:clock(),
                                                   vnode_id  = VNodeId,
                                                   addr_id   = AddrId,
                                                   key       = Key,
                                                   node      = Node,
                                                   timestamp = leo_date:now()}),
    Table = ?TBL_REBALANCE_COUNTER,
    case ets_lookup(Table, VNodeId) of
        {ok, 0} -> ets:insert(Table, {VNodeId, 0});
        _Other  -> void
    end,
    ok = increment_counter(Table, VNodeId),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(_,_,_,_,_) ->
    {error, badarg}.


%% -------------------------------------------------------------------
%% Callbacks
%% -------------------------------------------------------------------
%% @doc Initializer
%%
-spec(init() ->
             ok | {error, any()}).
init() ->
    ok.


%% @doc Subscribe a message from the queue.
%%
-spec(handle_call({publish | consume, any() | queue_id() , any() | binary()}) ->
             ok | {error, any()}).
handle_call({publish, _Id, _Reply}) ->
    ok;

handle_call({consume, ?QUEUE_ID_PER_OBJECT, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_PER_OBJECT", "cause:~p", [Cause]),
            {error, Cause};
        #inconsistent_data_message{addr_id = AddrId,
                                   key     = Key,
                                   type    = ErrorType} ->
            case correct_redundancies(Key) of
                ok ->
                    ok;
                {error, not_found} ->
                    ok;
                {error, Cause} ->
                    publish(?QUEUE_TYPE_PER_OBJECT, AddrId, Key, ErrorType),
                    ?warn("handle_call/1 - QUEUE_ID_PER_OBJECT", "cause:~p", [Cause]),
                    {error, Cause}
            end
    end;

handle_call({consume, ?QUEUE_ID_SYNC_BY_VNODE_ID, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_SYNC_BY_VNODE_ID", "cause:~p", [Cause]),
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

handle_call({consume, ?QUEUE_ID_REBALANCE, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_REBALANCE", "cause:~p", [Cause]),
            {error, Cause};
        #rebalance_message{node     = Node,
                           vnode_id = VNodeId,
                           addr_id  = AddrId,
                           key      = Key} ->
            %% NOTE:
            %% If remote-node status is NOT 'running',
            %%     this function cannot operate 'copy'.
            case leo_redundant_manager_api:get_member_by_node(Node) of
                {ok, #member{state = ?STATE_RUNNING}} ->
                    %% Copy objects to a remote-node
                    ok = decrement_counter(?TBL_REBALANCE_COUNTER, VNodeId),

                    case leo_redundant_manager_api:get_redundancies_by_addr_id(get, AddrId) of
                        {ok, #redundancies{nodes = Redundancies}} ->
                            case delete_node_from_redundancies(Redundancies, Node, []) of
                                {ok, [#redundant_node{node = N,
                                                      available = true,
                                                      can_read_repair = true}|_]} when N == node() ->
                                    case leo_storage_handler_object:copy([Node], AddrId, Key) of
                                        ok ->
                                            ok;
                                        Error ->
                                            ok = leo_storage_mq_client:publish(
                                                   ?QUEUE_TYPE_PER_OBJECT, AddrId, Key,
                                                   ?ERR_TYPE_REPLICATE_DATA),
                                            Error
                                    end;
                                _ ->
                                    ok
                            end;
                        _ ->
                            ok
                    end;
                _ ->
                    {error, inactive}
            end
    end;

handle_call({consume, ?QUEUE_ID_ASYNC_DELETION, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_ASYNC_DELETION", "cause:~p", [Cause]),
            {error, Cause};
        #async_deletion_message{addr_id  = AddrId,
                                key      = Key} ->
            case leo_storage_handler_object:delete(#object{addr_id   = AddrId,
                                                           key       = Key,
                                                           clock     = leo_date:clock(),
                                                           timestamp = leo_date:now()
                                                          }, 0) of
                ok ->
                    ok;
                {error, _Cause} ->
                    publish(?QUEUE_TYPE_ASYNC_DELETION, AddrId, Key)
            end
    end;

handle_call({consume, ?QUEUE_ID_RECOVERY_NODE, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_RECOVERY_NODE", "cause:~p", [Cause]),
            {error, Cause};
        #recovery_node_message{node = Node} ->
            recover_node(Node)
    end.


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc synchronize by vnode-id.
%%
-spec(recover_node(atom()) ->
             ok).
recover_node(Node) ->
    Fun = fun(K, _V, Acc) ->
                  {AddrId, Key} = binary_to_term(K),

                  case leo_redundant_manager_api:get_redundancies_by_addr_id(put, AddrId) of
                      {ok, #redundancies{nodes = Redundancies}} ->
                          Nodes = [N || #redundant_node{node = N} <- Redundancies],
                          case lists:member(Node, Nodes) of
                              true ->
                                  ?MODULE:publish(?QUEUE_TYPE_PER_OBJECT,
                                                  AddrId, Key, ?ERR_TYPE_RECOVER_DATA);
                              false  ->
                                  void
                          end,
                          {ok, Acc};
                      _ ->
                          {ok, Acc}
                  end
          end,
    _ = leo_object_storage_api:fetch_by_addr_id(0, Fun),
    ok.

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
                          case leo_redundant_manager_api:get_redundancies_by_addr_id(put, AddrId) of
                              {ok, #redundancies{nodes = Redundancies}} ->
                                  Nodes = [N || #redundant_node{node = N} <- Redundancies],
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


%% @doc Remove a node from redundancies
%% @private
delete_node_from_redundancies([],_, Acc) ->
    {ok, lists:reverse(Acc)};
delete_node_from_redundancies([#redundant_node{node = Node}|Rest], Node, Acc) ->
    delete_node_from_redundancies(Rest, Node, Acc);
delete_node_from_redundancies([RedundatNode|Rest], Node, Acc) ->
    delete_node_from_redundancies(Rest, Node, [RedundatNode|Acc]).


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
    correct_redundancies_1(Key, AddrId, Redundancies, [], []).

%% correct_redundancies_1/5 - next.
%%
-spec(correct_redundancies_1(string(), integer(), list(), list(), list()) ->
             ok | {error, any()}).
correct_redundancies_1(_Key,_AddrId, [], [], _ErrorNodes) ->
    {error, not_found};

correct_redundancies_1(_Key,_AddrId, [], Metadatas, ErrorNodes) ->
    correct_redundancies_2(Metadatas, ErrorNodes);

correct_redundancies_1(Key, AddrId, [#redundant_node{node = Node}|T], Metadatas, ErrorNodes) ->
    %% NOTE:
    %% If remote-node status is NOT 'running',
    %%     this function cannot operate 'rpc-call'.
    case leo_redundant_manager_api:get_member_by_node(Node) of
        {ok, #member{state = ?STATE_RUNNING}} ->
            %% Retrieve a metadata from remote-node
            RPCKey = rpc:async_call(Node, leo_storage_handler_object, head, [AddrId, Key]),

            case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
                {value, {ok, Metadata}} ->
                    correct_redundancies_1(Key, AddrId, T, [{Node, Metadata}|Metadatas], ErrorNodes);
                _Error ->
                    correct_redundancies_1(Key, AddrId, T, Metadatas, [Node|ErrorNodes])
            end;
        {ok, #member{state = ?STATE_DETACHED}} ->
            correct_redundancies_1(Key, AddrId, T, Metadatas, ErrorNodes);
        _Other ->
            correct_redundancies_1(Key, AddrId, T, Metadatas, [Node|ErrorNodes])
    end.

%% correct_redundancies_2/3
%%
-spec(correct_redundancies_2(list(), list()) ->
             ok | {error, any()}).
correct_redundancies_2(ListOfMetadata, ErrorNodes) ->
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

    correct_redundancies_3(ErrorNodes ++ InconsistentNodes, CorrectNodes, Metadata).


%% correct_redundancies_3/4 - last.
%%
-spec(correct_redundancies_3(list(), list(), #metadata{}) ->
             ok | {error, any()}).
correct_redundancies_3([], _, _) ->
    ok;
correct_redundancies_3(_, [], _) ->
    {error, 'could not fix inconsistency'};
correct_redundancies_3(InconsistentNodes, [Node|Rest], Metadata) ->
    RPCKey = rpc:async_call(Node, leo_storage_api, synchronize,
                            [InconsistentNodes, Metadata]),
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
            correct_redundancies_3(InconsistentNodes, Rest, Metadata)
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


%% @doc Lookup rebalance counter
%% @private
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


%% @doc Increment rebalance counter
%% @private
increment_counter(Table, Key) ->
    catch ets:update_counter(Table, Key, 1),
    ok.

%% @doc Decrement rebalance counter
%% @private
decrement_counter(Table, Key) ->
    catch ets:update_counter(Table, Key, -1),
    ok.


%% @doc Retrieve queue's id by queue's type
%% @private
-spec(queue_id(queue_type()) ->
             queue_id()).
queue_id(?QUEUE_TYPE_SYNC_BY_VNODE_ID) ->
    ?QUEUE_ID_SYNC_BY_VNODE_ID;
queue_id(?QUEUE_TYPE_ASYNC_DELETION) ->
    ?QUEUE_ID_ASYNC_DELETION;
queue_id(?QUEUE_TYPE_PER_OBJECT) ->
    ?QUEUE_ID_PER_OBJECT;
queue_id(?QUEUE_TYPE_REBALANCE) ->
    ?QUEUE_ID_REBALANCE;
queue_id(?QUEUE_TYPE_RECOVERY_NODE) ->
    ?QUEUE_ID_RECOVERY_NODE.

