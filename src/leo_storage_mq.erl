%%====================================================================
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
%% -------------------------------------------------------------------
%% LeoFS Storage - MQ Client
%% @doc
%% @end
%%====================================================================
-module(leo_storage_mq).

-author('Yosuke Hara').

-behaviour(leo_mq_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_mq/include/leo_mq.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start/1, start/2,
         publish/2, publish/3, publish/4, publish/5]).
-export([init/0, handle_call/1, handle_call/3]).

-define(SLASH, "/").
-define(MSG_PATH_PER_OBJECT,        "1").
-define(MSG_PATH_SYNC_VNODE_ID,     "2").
-define(MSG_PATH_REBALANCE,         "3").
-define(MSG_PATH_ASYNC_DELETION,    "4").
-define(MSG_PATH_RECOVERY_NODE,     "5").
-define(MSG_PATH_SYNC_OBJ_WITH_DC,  "6").
-define(MSG_PATH_COMP_META_WITH_DC, "7").
-define(MSG_PATH_DEL_DIR,           "8").

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc create queues and launch mq-servers.
%%
-spec(start(RootPath) ->
             ok | {error, any()} when RootPath::binary()).
start(RootPath) ->
    start(leo_storage_sup, RootPath).

-spec(start(RefSup, RootPath) ->
             ok | {error, any()} when RefSup::pid(),
                                      RootPath::binary()).
start(RefSup, RootPath) ->
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
    RootPath_1 =
        case (string:len(RootPath) == string:rstr(RootPath, ?SLASH)) of
            true  -> RootPath;
            false -> RootPath ++ ?SLASH
        end,

    ?TBL_REBALANCE_COUNTER = ets:new(?TBL_REBALANCE_COUNTER,
                                     [named_table, public, {read_concurrency, true}]),
    start_1([{?QUEUE_ID_PER_OBJECT,        ?MSG_PATH_PER_OBJECT},
             {?QUEUE_ID_SYNC_BY_VNODE_ID,  ?MSG_PATH_SYNC_VNODE_ID},
             {?QUEUE_ID_REBALANCE,         ?MSG_PATH_REBALANCE},
             {?QUEUE_ID_ASYNC_DELETION,    ?MSG_PATH_ASYNC_DELETION},
             {?QUEUE_ID_RECOVERY_NODE,     ?MSG_PATH_RECOVERY_NODE},
             {?QUEUE_ID_SYNC_OBJ_WITH_DC,  ?MSG_PATH_SYNC_OBJ_WITH_DC},
             {?QUEUE_ID_COMP_META_WITH_DC, ?MSG_PATH_COMP_META_WITH_DC},
             {?QUEUE_ID_DEL_DIR, ?MSG_PATH_DEL_DIR}
            ], RefMqSup, RootPath_1).

%% @private
start_1([],_,_) ->
    ok;
start_1([{Id, Path}|Rest], Sup, Root) ->
    leo_mq_api:new(Sup, Id, [{?MQ_PROP_MOD, ?MODULE},
                             {?MQ_PROP_FUN, ?MQ_SUBSCRIBE_FUN},
                             {?MQ_PROP_ROOT_PATH, Root ++ Path},
                             {?MQ_PROP_DB_NAME,   ?env_mq_backend_db()},
                             {?MQ_PROP_DB_PROCS,  ?env_num_of_mq_procs()},
                             {?MQ_PROP_BATCH_MSGS_MAX,  ?env_mq_num_of_batch_process_max()},
                             {?MQ_PROP_BATCH_MSGS_REG,  ?env_mq_num_of_batch_process_reg()},
                             {?MQ_PROP_INTERVAL_MAX,  ?env_mq_interval_between_batch_procs_max()},
                             {?MQ_PROP_INTERVAL_REG,  ?env_mq_interval_between_batch_procs_reg()}
                            ]),
    start_1(Rest, Sup, Root).


%% @doc Input a message into the queue.
%%
-spec(publish(queue_type(), atom()|binary()) ->
             ok | {error, any()}).
publish(?QUEUE_TYPE_RECOVERY_NODE = Id, Node) ->
    Clock = leo_date:clock(),
    KeyBin = term_to_binary({Node, Clock}),
    MsgBin = term_to_binary(
               #recovery_node_message{id        = Clock,
                                      node      = Node,
                                      timestamp = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MsgBin);
publish(_,_) ->
    {error, badarg}.

-spec(publish(queue_type(), any(), any()) ->
             ok | {error, any()}).
publish(?QUEUE_TYPE_SYNC_BY_VNODE_ID = Id, VNodeId, Node) ->
    Clock = leo_date:clock(),
    KeyBin     = term_to_binary({VNodeId, Node, Clock}),
    MessageBin = term_to_binary(
                   #sync_unit_of_vnode_message{id        = Clock,
                                               vnode_id  = VNodeId,
                                               node      = Node,
                                               timestamp = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(?QUEUE_TYPE_ASYNC_DELETION = Id, AddrId, Key) ->
    Clock = leo_date:clock(),
    KeyBin     = term_to_binary({AddrId, Key, Clock}),
    MessageBin = term_to_binary(
                   #async_deletion_message{id        = Clock,
                                           addr_id   = AddrId,
                                           key       = Key,
                                           timestamp = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(?QUEUE_TYPE_SYNC_OBJ_WITH_DC, AddrId, Key) ->
    publish(?QUEUE_TYPE_SYNC_OBJ_WITH_DC, undefined, AddrId, Key);

publish(?QUEUE_TYPE_COMP_META_WITH_DC = Id, ClusterId, AddrAndKeyList) ->
    Clock = leo_date:clock(),
    KeyBin     = term_to_binary({ClusterId, Clock}),
    MessageBin = term_to_binary(
                   #comparison_metadata_with_dc{id         = Clock,
                                                cluster_id = ClusterId,
                                                list_of_addrid_and_key = AddrAndKeyList,
                                                timestamp = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(?QUEUE_TYPE_DEL_DIR = Id, Node, Keys) ->
    Clock = leo_date:clock(),
    KeyBin = term_to_binary({Node, Keys, Clock}),
    MsgBin = term_to_binary(
               #delete_dir{id   = Clock,
                           node = Node,
                           keys = Keys,
                           timestamp = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MsgBin);

publish(_,_,_) ->
    {error, badarg}.

-spec(publish(queue_type(), any(), any(), any()) ->
             ok | {error, any()}).
publish(?QUEUE_TYPE_PER_OBJECT = Id, AddrId, Key, ErrorType) ->
    Clock = leo_date:clock(),
    KeyBin = term_to_binary({ErrorType, Key, Clock}),
    MessageBin = term_to_binary(
                   #inconsistent_data_message{id        = Clock,
                                              type      = ErrorType,
                                              addr_id   = AddrId,
                                              key       = Key,
                                              timestamp = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(?QUEUE_TYPE_SYNC_OBJ_WITH_DC = Id, ClusterId, AddrId, Key) ->
    Clock = leo_date:clock(),
    KeyBin = term_to_binary({ClusterId, AddrId, Key, Clock}),
    MessageBin  = term_to_binary(
                    #inconsistent_data_with_dc{id         = Clock,
                                               cluster_id = ClusterId,
                                               addr_id    = AddrId,
                                               key        = Key,
                                               del        = 0,
                                               timestamp  = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(_,_,_,_) ->
    {error, badarg}.

-spec(publish(queue_type(), any(), any(), any(), any()) ->
             ok | {error, any()}).
publish(?QUEUE_TYPE_REBALANCE = Id, Node, VNodeId, AddrId, Key) ->
    Clock = leo_date:clock(),
    KeyBin = term_to_binary({Node, AddrId, Key, Clock}),
    MessageBin = term_to_binary(
                   #rebalance_message{id        = Clock,
                                      vnode_id  = VNodeId,
                                      addr_id   = AddrId,
                                      key       = Key,
                                      node      = Node,
                                      timestamp = leo_date:now()}),
    Table = ?TBL_REBALANCE_COUNTER,
    case ets_lookup(Table, VNodeId) of
        {ok, 0} ->
            ets:insert(Table, {VNodeId, 0});
        _Other ->
            void
    end,
    ok = increment_counter(Table, VNodeId),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(?QUEUE_TYPE_SYNC_OBJ_WITH_DC = Id, ClusterId, AddrId, Key, Del) ->
    Clock = leo_date:clock(),
    KeyBin = term_to_binary({ClusterId, AddrId, Key, Clock}),
    MessageBin  = term_to_binary(
                    #inconsistent_data_with_dc{id         = Clock,
                                               cluster_id = ClusterId,
                                               addr_id    = AddrId,
                                               key        = Key,
                                               del        = Del,
                                               timestamp  = leo_date:now()}),
    leo_mq_api:publish(queue_id(Id), KeyBin, MessageBin);

publish(_,_,_,_,_) ->
    {error, badarg}.


%% -------------------------------------------------------------------
%% Callbacks
%% -------------------------------------------------------------------
%% @doc Initializer
%%
-spec(init() -> ok | {error, any()}).
init() ->
    ok.


%% @doc Subscribe a message from the queue.
%%
-spec(handle_call({consume, any() | queue_id() , any() | binary()}) ->
             ok | {error, any()}).
handle_call({consume, ?QUEUE_ID_PER_OBJECT, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_PER_OBJECT",
                   [{cause, Cause}]),
            {error, Cause};
        #inconsistent_data_message{addr_id = AddrId,
                                   key     = Key,
                                   type    = ErrorType} ->
            case correct_redundancies(Key) of
                ok ->
                    ok;
                {error, Cause} ->
                    ?warn("handle_call/1 - consume",
                          [{addr_id, AddrId},
                           {key, Key}, {cause, Cause}]),
                    publish(?QUEUE_TYPE_PER_OBJECT, AddrId, Key, ErrorType),
                    {error, Cause}
            end;
        _ ->
            {error, ?ERROR_COULD_NOT_MATCH}
    end;

handle_call({consume, ?QUEUE_ID_SYNC_BY_VNODE_ID, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_SYNC_BY_VNODE_ID",
                   [{cause, Cause}]),
            {error, Cause};
        #sync_unit_of_vnode_message{vnode_id = ToVNodeId,
                                    node     = Node} ->
            {ok, Res} = leo_redundant_manager_api:range_of_vnodes(ToVNodeId),
            {ok, {CurRingHash, _PrevRingHash}} =
                leo_redundant_manager_api:checksum(?CHECKSUM_RING),
            ok = sync_vnodes(Node, CurRingHash, Res),
            notify_rebalance_message_to_manager(ToVNodeId);
        _ ->
            {error, ?ERROR_COULD_NOT_MATCH}
    end;

handle_call({consume, ?QUEUE_ID_REBALANCE, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_REBALANCE",
                   [{cause, Cause}]),
            {error, Cause};
        #rebalance_message{} = Msg ->
            rebalance_1(Msg);
        _Cause ->
            {error, ?ERROR_COULD_NOT_MATCH}
    end;

handle_call({consume, ?QUEUE_ID_ASYNC_DELETION, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_ASYNC_DELETION",
                   [{cause, Cause}]),
            {error, Cause};
        #async_deletion_message{addr_id  = AddrId,
                                key      = Key} ->
            case catch leo_storage_handler_object:delete(
                         #?OBJECT{addr_id   = AddrId,
                                  key       = Key,
                                  clock     = leo_date:clock(),
                                  timestamp = leo_date:now(),
                                  del       = ?DEL_TRUE
                                 }, 0, false) of
                ok ->
                    ok;
                {_,_Cause} ->
                    publish(?QUEUE_TYPE_ASYNC_DELETION, AddrId, Key)
            end;
        _ ->
            {error, ?ERROR_COULD_NOT_MATCH}
    end;

handle_call({consume, ?QUEUE_ID_RECOVERY_NODE, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_RECOVERY_NODE",
                   [{cause, Cause}]),
            {error, Cause};
        #recovery_node_message{node = Node} ->
            recover_node(Node);
        _ ->
            {error, ?ERROR_COULD_NOT_MATCH}
    end;

handle_call({consume, ?QUEUE_ID_SYNC_OBJ_WITH_DC, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_SYNC_OBJ_WITH_DC",
                   [{cause, Cause}]),
            {error, Cause};
        #inconsistent_data_with_dc{} = Msg ->
            fix_consistency_between_clusters(Msg);
        _ ->
            {error, ?ERROR_COULD_NOT_MATCH}
    end;

handle_call({consume, ?QUEUE_ID_COMP_META_WITH_DC, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_COMP_META_WITH_DC",
                   [{casue, Cause}]),
            {error, Cause};
        #comparison_metadata_with_dc{cluster_id = ClusterId,
                                     list_of_addrid_and_key = AddrAndKeyList} ->
            %% @doc - condition: if state of a remote-cluster is not 'running',
            %%                   then this queue is removed
            case leo_mdcr_tbl_cluster_stat:find_by_cluster_id(ClusterId) of
                {ok, #?CLUSTER_STAT{state = ?STATE_RUNNING}} ->
                    %% re-compare metadatas
                    leo_storage_handle_sync:send_addrid_and_key_to_remote(
                      ClusterId, AddrAndKeyList);
                _ ->
                    ok
            end;
        _ ->
            {error, ?ERROR_COULD_NOT_MATCH}
    end;

handle_call({consume, ?QUEUE_ID_DEL_DIR, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        {'EXIT', Cause} ->
            ?error("handle_call/1 - QUEUE_ID_DEL_DIR",
                   [{cause, Cause}]),
            {error, Cause};
        #delete_dir{keys  = Keys,
                    node = Node} ->
            Ref = make_ref(),
            leo_storage_handler_object:delete_objects_under_dir([Node], Ref, Keys)
    end.

handle_call(_,_,_) ->
    ok.


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS-1
%%--------------------------------------------------------------------
%% @doc synchronize by vnode-id.
%%
-spec(recover_node(atom()) ->
             ok).
recover_node(Node) ->
    Callback = recover_node_callback(Node),
    _ = leo_object_storage_api:fetch_by_addr_id(0, Callback),
    ok.

%% @private
-spec(recover_node_callback(atom()) ->
             any()).
recover_node_callback(Node) ->
    fun(K, V, Acc) ->
            Metadata_1 = binary_to_term(V),
            Metadata_2 = leo_object_storage_transformer:transform_metadata(Metadata_1),
            #?METADATA{addr_id = AddrId} = Metadata_2,

            %% Retrieve redundant-nodes from the redundant-manager(RING),
            %% then if the recovery-target-node and "this node"
            %% are included in retrieved redundant-nodes, "the file" will be recovered
            %% by the MQ.
            case leo_redundant_manager_api:get_redundancies_by_addr_id(put, AddrId) of
                {ok, #redundancies{nodes = Redundancies}} ->
                    RedundantNodes = [N || #redundant_node{node = N} <- Redundancies],
                    ok = recover_node_callback_1(AddrId, K, Node, RedundantNodes),
                    Acc;
                _Other ->
                    Acc
            end
    end.

%% @private
recover_node_callback_1(_,_,_,[]) ->
    ok;
recover_node_callback_1(AddrId, Key, Node, RedundantNodes) ->
    case lists:member(Node, RedundantNodes) of
        true ->
            case lists:member(erlang:node(), RedundantNodes) of
                true ->
                    ?MODULE:publish(?QUEUE_TYPE_PER_OBJECT,
                                    AddrId, Key, ?ERR_TYPE_RECOVER_DATA);
                false ->
                    ok
            end;
        false ->
            ok
    end.


%% @doc synchronize by vnode-id.
%%
-spec(sync_vnodes(atom(), integer(), list()) ->
             ok).
sync_vnodes(_, _, []) ->
    ok;
sync_vnodes(Node, RingHash, [{FromAddrId, ToAddrId}|T]) ->
    Callback = sync_vnodes_callback(Node, FromAddrId, ToAddrId),
    catch leo_object_storage_api:fetch_by_addr_id(FromAddrId, Callback),
    catch notify_message_to_manager(?env_manager_nodes(leo_storage), ToAddrId, erlang:node()),
    sync_vnodes(Node, RingHash, T).

%% @private
-spec(sync_vnodes_callback(atom(), pos_integer(), pos_integer()) ->
             any()).
sync_vnodes_callback(Node, FromAddrId, ToAddrId)->
    fun(K, V, Acc) ->
            %% Note: An object of copy is NOT equal current ring-hash.
            %%       Then a message in the rebalance-queue.
            #?METADATA{addr_id = AddrId} = binary_to_term(V),

            case (AddrId >= FromAddrId andalso
                  AddrId =< ToAddrId) of
                true ->
                    case catch leo_redundant_manager_api:get_redundancies_by_addr_id(put, AddrId) of
                        {'EXIT',_Cause} ->
                            Acc;
                        {ok, #redundancies{nodes = Redundancies}} ->
                            Nodes = [N || #redundant_node{node = N} <- Redundancies],
                            case lists:member(Node, Nodes) of
                                true ->
                                    VNodeId = ToAddrId,
                                    ?MODULE:publish(?QUEUE_TYPE_REBALANCE, Node,
                                                    VNodeId, AddrId, K),
                                    Acc;
                                false ->
                                    Acc
                            end,
                            Acc;
                        _ ->
                            Acc
                    end;
                false ->
                    Acc
            end
    end.


%% @doc Remove a node from redundancies
%% @private
-spec(delete_node_from_redundancies(list(#redundant_node{}), atom(), list(#redundant_node{})) ->
             {ok, list(#redundant_node{})}).
delete_node_from_redundancies([],_,Acc) ->
    {ok, lists:reverse(Acc)};
delete_node_from_redundancies([#redundant_node{node = Node}|Rest], Node, Acc) ->
    delete_node_from_redundancies(Rest, Node, Acc);
delete_node_from_redundancies([RedundantNode|Rest], Node, Acc) ->
    delete_node_from_redundancies(Rest, Node, [RedundantNode|Acc]).


%% @doc Find a node from redundancies
%% @private
-spec(find_node_from_redundancies(list(#redundant_node{}), atom()) ->
             boolean()).
find_node_from_redundancies([],_) ->
    false;
find_node_from_redundancies([#redundant_node{node = Node}|_], Node) ->
    true;
find_node_from_redundancies([_|Rest], Node) ->
    find_node_from_redundancies(Rest, Node).


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
                  ?warn("notify_message_to_manager/3",
                        [{vnode_id, VNodeId},
                         {node, Node}, {cause, Cause}]),
                  {error, Cause};
              timeout = Cause ->
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
-spec(correct_redundancies(binary()) ->
             ok | {error, any()}).
correct_redundancies(Key) ->
    case leo_redundant_manager_api:get_redundancies_by_key(Key) of
        {ok, #redundancies{nodes = Redundancies,
                           id    = AddrId}} ->
            Redundancies_1 = get_redundancies_with_replicas(
                               AddrId, Key, Redundancies),
            correct_redundancies_1(Key, AddrId, Redundancies_1, [], []);
        {error, Cause} ->
            ?warn("correct_redundancies/1",
                  [{key, Key}, {cause, Cause}]),
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCY}
    end.

%% correct_redundancies_1/5 - next.
%%
-spec(correct_redundancies_1(binary(), integer(), list(), list(), list()) ->
             ok | {error, any()}).
correct_redundancies_1(_Key,_AddrId, [], [], _ErrorNodes) ->
    {error, 'not_solved'};
correct_redundancies_1(_Key,_AddrId, [], Metadatas, ErrorNodes) ->
    correct_redundancies_2(Metadatas, ErrorNodes);
correct_redundancies_1(Key, AddrId, [#redundant_node{node = Node}|T], Metadatas, ErrorNodes) ->
    %% NOTE:
    %% If remote-node status is NOT 'running',
    %%     this function cannot operate 'rpc-call'.
    case leo_redundant_manager_api:get_member_by_node(Node) of
        {ok, #member{state = ?STATE_RUNNING}} ->
            %% Retrieve a metadata from remote-node
            %% invoke head with NO retry option
            RPCKey = rpc:async_call(Node, leo_storage_handler_object,
                                    head, [AddrId, Key, false]),

            case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
                {value, {ok, Metadata}} ->
                    correct_redundancies_1(Key, AddrId, T,
                                           [{Node, Metadata}|Metadatas], ErrorNodes);
                _Error ->
                    correct_redundancies_1(Key, AddrId, T,
                                           Metadatas, [Node|ErrorNodes])
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
    H = case (erlang:length(ListOfMetadata) == 1) of
            true ->
                erlang:hd(ListOfMetadata);
            false ->
                MaxClock = lists:max([M#?METADATA.clock
                                      || {_,M} <- ListOfMetadata]),
                {_,RetL} = lists:foldl(
                             fun({_,#?METADATA{clock = Clock}} = M, {MaxClock_1, Acc}) when Clock == MaxClock_1 ->
                                     {MaxClock_1, [M|Acc]};
                                (_, {MaxClock_1, Acc}) ->
                                     {MaxClock_1, Acc}
                             end, {MaxClock, []}, ListOfMetadata),
                erlang:hd(RetL)
        end,
    {_,Metadata} = H,

    {_Dest, CorrectNodes, InconsistentNodes} =
        lists:foldl(
          fun({Node,_Metadata}, {{DestNode, _Metadata} = Dest, C, R}) when Node =:= DestNode ->
                  {Dest, [Node|C], R};
             ({Node, #?METADATA{clock = Clock}}, {{DestNode, #?METADATA{clock = DestClock}} = Dest, C, R}) when Node  =/= DestNode,
                                                                                                                Clock =:= DestClock ->
                  {Dest, [Node|C], R};
             ({Node, #?METADATA{clock = Clock}}, {{DestNode, #?METADATA{clock = DestClock}} = Dest, C, R}) when Node  =/= DestNode,
                                                                                                                Clock =/= DestClock ->
                  {Dest, C, [Node|R]}
          end, {H, [], []}, ListOfMetadata),
    correct_redundancies_3(ErrorNodes ++ InconsistentNodes, CorrectNodes, Metadata).


%% correct_redundancies_3/4 - last.
%%
-spec(correct_redundancies_3(list(), list(), #?METADATA{}) ->
             ok | {error, any()}).
correct_redundancies_3([], _, _) ->
    ok;
correct_redundancies_3(_, [], _) ->
    {error, 'not_fix_inconsistency'};
correct_redundancies_3(InconsistentNodes, [Node|_] = NodeL, Metadata) ->
    RPCKey = rpc:async_call(Node, leo_storage_api, synchronize,
                            [InconsistentNodes, Metadata]),
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
              {value, ok} ->
                  ok;
              {value, {error, Cause}} ->
                  {error, Cause};
              {value, not_found = Cause} ->
                  {error, Cause};
              {value, {badrpc = Cause, _}} ->
                  {error, Cause};
              timeout = Cause->
                  {error, Cause}
          end,
    correct_redundancies_4(Ret, InconsistentNodes, NodeL, Metadata).

%% @private
correct_redundancies_4(ok,_InconsistentNodes,_NodeL,_Metadata) ->
    ok;
correct_redundancies_4({error, eof},_InconsistentNodes,_NodeL, Metadata) ->
    case (Metadata#?METADATA.dsize == 0) of
        true ->
            Obj = leo_object_storage_transformer:metadata_to_object(<<>>, Metadata),
            case leo_storage_handler_object:put(Obj, 0) of
                {ok,_} ->
                    ok;
                Error ->
                    Error
            end;
        false ->
            ?error("correct_redundancies_4/4",
                   [{metadata, Metadata},
                    {cause, 'broken_object'}]),
            ok
    end;
correct_redundancies_4({error, not_found = Why},_InconsistentNodes, [Node|_Rest], Metadata) ->
    ?warn("correct_redundancies_4/4",
          [{node, Node},
           {metadata, Metadata}, {cause, Why}]),
    ok;
correct_redundancies_4({error, Why}, InconsistentNodes, [Node|Rest], Metadata) ->
    ?warn("correct_redundancies_4/4",
          [{inconsistent_nodes, InconsistentNodes},
           {node, Node}, {metadata, Metadata}, {cause, Why}]),
    correct_redundancies_3(InconsistentNodes, Rest, Metadata).


%% @doc Relocate an object because of executed "rebalance"
%%      NOTE:
%%          If remote-node status is NOT 'running',
%%          this function cannot operate 'copy'.
%% @private
rebalance_1(#rebalance_message{node = Node,
                               vnode_id = VNodeId,
                               addr_id = AddrId,
                               key = Key} = Msg) ->
    case leo_redundant_manager_api:get_member_by_node(Node) of
        {ok, #member{state = ?STATE_RUNNING}} ->
            ok = decrement_counter(?TBL_REBALANCE_COUNTER, VNodeId),
            case leo_redundant_manager_api:get_redundancies_by_addr_id(get, AddrId) of
                {ok, #redundancies{nodes = Redundancies}} ->
                    Ret = delete_node_from_redundancies(Redundancies, Node, []),
                    rebalance_2(Ret, Msg);
                _ ->
                    ok = publish(?QUEUE_TYPE_PER_OBJECT,
                                 AddrId, Key, ?ERR_TYPE_REPLICATE_DATA),
                    {error, ?ERROR_COULD_NOT_GET_REDUNDANCY}
            end;
        {error, Cause} ->
            ?warn("rebalance_1/1",
                  [{node, Node}, {addr_id, AddrId},
                   {key, Key}, {cause, Cause}]),
            ok = publish(?QUEUE_TYPE_PER_OBJECT,
                         AddrId, Key, ?ERR_TYPE_REPLICATE_DATA),
            {error, inactive}
    end.

%% @private
rebalance_2({ok,[]},_) ->
    ok;
rebalance_2({ok, Redundancies}, #rebalance_message{node = Node,
                                                   addr_id = AddrId,
                                                   key = Key}) ->
    Redundancies_1 = get_redundancies_with_replicas(
                       AddrId, Key, Redundancies),
    case find_node_from_redundancies(Redundancies_1, erlang:node()) of
        true ->
            case leo_storage_handler_object:replicate([Node], AddrId, Key) of
                ok ->
                    ok;
                not_found = Cause ->
                    ?warn("rebalance_2/2",
                          [{addr_id, AddrId}, {key, Key},
                           {cause, Cause}]),
                    ok;
                Error ->
                    ok = publish(?QUEUE_TYPE_PER_OBJECT,
                                 AddrId, Key, ?ERR_TYPE_REPLICATE_DATA),
                    Error
            end;
        false ->
            ?warn("rebalance_2/2",
                  [{node, Node}, {addr_id, AddrId},
                   {key, Key}, {cause, 'node_not_found'}]),
            ok = publish(?QUEUE_TYPE_PER_OBJECT,
                         AddrId, Key, ?ERR_TYPE_REPLICATE_DATA),
            ok
    end.


%% @doc Retrieve redundancies with a number of replicas
%% @private
get_redundancies_with_replicas(AddrId, Key, Redundancies) ->
    %% Retrieve redundancies with a number of replicas
    case leo_object_storage_api:head({AddrId, Key}) of
        {ok, MetaBin} ->
            case binary_to_term(MetaBin) of
                #?METADATA{num_of_replicas = 0} ->
                    Redundancies;
                #?METADATA{num_of_replicas = NumOfReplicas} ->
                    lists:sublist(Redundancies, NumOfReplicas)
            end;
        _ ->
            Redundancies
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
            Fun = fun(_Manager, true) ->
                          void;
                     (Manager0, false = Res) ->
                          Manager1 = case is_atom(Manager0) of
                                         true  -> Manager0;
                                         false -> list_to_atom(Manager0)
                                     end,
                          case catch rpc:call(Manager1, leo_manager_api, notify,
                                              [rebalance, VNodeId,
                                               erlang:node(), NumOfMessages],
                                              ?DEF_REQ_TIMEOUT) of
                              ok ->
                                  true;
                              {_, Cause} ->
                                  ?error("notify_rebalance_message_to_manager/1",
                                         [{manager, Manager1},
                                          {vnode_id, VNodeId}, {cause, Cause}]),
                                  Res;
                              timeout ->
                                  Res
                          end
                  end,
            lists:foldl(Fun, false, ?env_manager_nodes(leo_storage)),
            ok;
        Error ->
            Error
    end.


%% @doc Fix consistency of an object between a local-cluster and remote-cluster(s)
%% @private
fix_consistency_between_clusters(#inconsistent_data_with_dc{
                                    addr_id = AddrId,
                                    key = Key,
                                    del = ?DEL_FALSE}) ->
    case leo_storage_handler_object:get(AddrId, Key, -1) of
        {ok, Metadata, Bin} ->
            Object = leo_object_storage_transformer:metadata_to_object(Metadata),
            leo_sync_remote_cluster:defer_stack(Object#?OBJECT{data = Bin});
        _ ->
            ok
    end;
fix_consistency_between_clusters(#inconsistent_data_with_dc{
                                    cluster_id = ClusterId,
                                    addr_id = AddrId,
                                    key = Key,
                                    del = ?DEL_TRUE}) ->
    Metadata = #?METADATA{cluster_id = ClusterId,
                          addr_id = AddrId,
                          key = Key,
                          dsize = 0,
                          del = ?DEL_FALSE},
    Object = leo_object_storage_transformer:metadata_to_object(Metadata),
    leo_sync_remote_cluster:stack(Object#?OBJECT{data = <<>>}).


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS-2
%%--------------------------------------------------------------------
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
    ?QUEUE_ID_RECOVERY_NODE;
queue_id(?QUEUE_TYPE_SYNC_OBJ_WITH_DC) ->
    ?QUEUE_ID_SYNC_OBJ_WITH_DC;
queue_id(?QUEUE_TYPE_COMP_META_WITH_DC) ->
    ?QUEUE_ID_COMP_META_WITH_DC;
queue_id(?QUEUE_TYPE_DEL_DIR) ->
    ?QUEUE_ID_DEL_DIR.
