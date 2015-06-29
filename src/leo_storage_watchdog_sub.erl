%%======================================================================
%%
%% Leo Storage
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
%% Leo Storage  - Watchdog Subscriber
%% @doc
%% @end
%%======================================================================
-module(leo_storage_watchdog_sub).

-author('Yosuke Hara').

-behaviour(leo_notify_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_watchdog/include/leo_watchdog.hrl").
-include_lib("eunit/include/eunit.hrl").

%% api
-export([start/0]).

%% callback
-export([handle_notify/3,
         handle_notify/4
        ]).

-define(WD_SUB_ID_1, 'leo_watchdog_sub_1').
-define(WD_SUB_ID_2, 'leo_watchdog_sub_2').
-define(DEF_WAIT_TIME, 100).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec(start() ->
             ok).
start() ->
    ok = leo_watchdog_sup:start_subscriber(?WD_SUB_ID_1, [leo_watchdog_cpu,
                                                          leo_watchdog_disk,
                                                          leo_watchdog_cluster,
                                                          leo_storage_watchdog_msgs
                                                         ], ?MODULE),
    ok = leo_watchdog_sup:start_subscriber(?WD_SUB_ID_2, [leo_storage_watchdog_fragment
                                                         ], ?MODULE),
    ok.


%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
-spec(handle_notify(Id, Alarm, Unixtime) ->
             ok | {error, any()} when Id::atom(),
                                      Alarm::term(),
                                      Unixtime::non_neg_integer()).
handle_notify(?WD_SUB_ID_1 = Id,_Alarm,_Unixtime) ->
    case is_active_watchdog() of
        true ->
            ?debug("handle_notify/3",
                   "received an alart - id:~p, alarm:~p", [Id, _Alarm]),
            leo_compact_fsm_controller:decrease(),
            leo_mq_api:decrease(?QUEUE_ID_PER_OBJECT),
            leo_mq_api:decrease(?QUEUE_ID_SYNC_BY_VNODE_ID),
            leo_mq_api:decrease(?QUEUE_ID_REBALANCE),
            leo_mq_api:decrease(?QUEUE_ID_ASYNC_DELETION),
            leo_mq_api:decrease(?QUEUE_ID_RECOVERY_NODE),
            leo_mq_api:decrease(?QUEUE_ID_SYNC_OBJ_WITH_DC),
            leo_mq_api:decrease(?QUEUE_ID_COMP_META_WITH_DC),
            leo_mq_api:decrease(?QUEUE_ID_DEL_DIR),
            ok;
        false ->
            ok
    end;
handle_notify(?WD_SUB_ID_2, #watchdog_alarm{state = #watchdog_state{
                                                       level = Level,
                                                       props = Props}},_Unixtime) ->
    %% Clear the current status
    _ = elarm:clear('leo_storage_watchdog_fragment', ?WD_ITEM_ACTIVE_SIZE_RATIO),
    %% Execution data-compacion or not
    case (Level >= ?WD_LEVEL_ERROR) of
        true ->
            case can_start_compaction() of
                true ->
                    timer:sleep(?DEF_WAIT_TIME),
                    ThisTime = leo_date:now(),
                    AutoCompactionInterval = ?env_auto_compaction_interval(),

                    case leo_compact_fsm_controller:state() of
                        {ok, #compaction_stats{status = ?ST_IDLING,
                                               pending_targets = PendingTargets,
                                               latest_exec_datetime = LastExecDataTime
                                              }} when PendingTargets /= [] andalso
                                                      (ThisTime - LastExecDataTime) >= AutoCompactionInterval ->
                            ProcsOfParallelProcessing = ?env_auto_compaction_parallel_procs(),

                            case leo_object_storage_api:compact_data(
                                   PendingTargets, ProcsOfParallelProcessing,
                                   fun leo_redundant_manager_api:has_charge_of_node/2) of
                                ok ->
                                    Ratio = leo_misc:get_value('ratio', Props),
                                    ?debug("handle_notify/3",
                                           "run-data-compaction - level:~w, ratio:~w%", [Level, Ratio]),
                                    ok;
                                _ ->
                                    ok
                            end;
                        _ ->
                            ok
                    end;
                false ->
                    ok
            end;
        false ->
            ok
    end;
handle_notify(_,_,_) ->
    ok.


-spec(handle_notify(Id, State, SafeTimes, Unixtime) ->
             ok | {error, any()} when Id::atom(),
                                      State::[{atom(), any()}],
                                      SafeTimes::non_neg_integer(),
                                      Unixtime::non_neg_integer()).
handle_notify(?WD_SUB_ID_1 = Id,_State,_SafeTimes,_Unixtime) ->
    case is_active_watchdog() of
        true ->
            ?debug("handle_notify/3",
                   "loosen_control_at_safe_count - id:~p, state:~p, times:~p",
                   [Id,_State,_SafeTimes]),
            leo_compact_fsm_controller:increase(),
            leo_mq_api:increase(?QUEUE_ID_PER_OBJECT),
            leo_mq_api:increase(?QUEUE_ID_SYNC_BY_VNODE_ID),
            leo_mq_api:increase(?QUEUE_ID_REBALANCE),
            leo_mq_api:increase(?QUEUE_ID_ASYNC_DELETION),
            leo_mq_api:increase(?QUEUE_ID_RECOVERY_NODE),
            leo_mq_api:increase(?QUEUE_ID_SYNC_OBJ_WITH_DC),
            leo_mq_api:increase(?QUEUE_ID_COMP_META_WITH_DC),
            leo_mq_api:increase(?QUEUE_ID_DEL_DIR),
            ok;
        false ->
            ok
    end;
handle_notify(?WD_SUB_ID_2,_State,_SafeTimes,_Unixtime) ->
    ok.


%%--------------------------------------------------------------------
%% Internal Function
%%--------------------------------------------------------------------
%% @private
is_active_watchdog() ->
    false == (?env_wd_cpu_enabled()  == false andalso
              ?env_wd_disk_enabled() == false).

%% @private
can_start_compaction() ->
    case leo_redundant_manager_api:get_members_by_status(?STATE_RUNNING) of
        {ok, Members} ->
            MaxNumOfNodes =
                case (erlang:round(erlang:length(Members)
                                   * ?env_auto_compaction_coefficient())) of
                    N when N < 1 ->
                        1;
                    N ->
                        N
                end,
            is_candidates(Members, MaxNumOfNodes, []);
        _ ->
            false
    end.


%% @doc Retrieve candidate nodes of data-compaction
%% @private
is_candidates([],_,[]) ->
    false;
is_candidates([],_,Acc) ->
    is_candidates_1(lists:reverse(Acc));
is_candidates(_, MaxNumOfNodes, Acc) when MaxNumOfNodes == erlang:length(Acc) ->
    is_candidates_1(lists:reverse(Acc));
is_candidates([#member{node = Node}|Rest], MaxNumOfNodes, Acc) ->
    Acc_1 =
        case rpc:call(Node, leo_object_storage_api,
                      du_and_compaction_stats, [], ?DEF_REQ_TIMEOUT) of
            {ok, []} ->
                Acc;
            {ok, RetL} ->
                DUState = leo_misc:get_value('du', RetL, []),
                CompactionState = leo_misc:get_value('compaction', RetL, []),

                %% Check du-state of the node
                {SumTotalSize, SumActiveSize, CompactionDate} =
                    lists:foldl(
                      fun({T,A,H}, {SumT, SumA, EndDataL}) ->
                              EndDate_1 = case H of
                                              [] ->
                                                  0;
                                              _ ->
                                                  lists:max([EndDate ||
                                                                #compaction_hist{
                                                                   end_datetime = EndDate}
                                                                    <- H])
                                          end,
                              {SumT + T, SumA + A, [EndDate_1|EndDataL]}
                      end, {0,0,[]},
                      [{TotalSize, ActiveSize, History} ||
                          #storage_stats{total_sizes = TotalSize,
                                         active_sizes = ActiveSize,
                                         compaction_hist = History} <- DUState]),

                %% Check compaction status of the node
                case CompactionState of
                    #compaction_stats{status = ?ST_RUNNING} ->
                        [{Node, ?ST_RUNNING}|Acc];
                    #compaction_stats{} ->
                        %% Check compaction's interval
                        MaxCompactionDate = lists:max(CompactionDate),
                        DiffCompactionDate_1 = leo_date:now() - MaxCompactionDate,
                        CompactionInterval = ?env_auto_compaction_interval(),

                        case (DiffCompactionDate_1 > CompactionInterval) of
                            true ->
                                %% Check fragmentation ratio
                                ActiveSizeRatio = erlang:round(SumActiveSize / SumTotalSize * 100),
                                ThresholdActiveSizeRatio = ?env_threshold_active_size_ratio(),
                                ActiveSizeRatioDiff = leo_math:floor(ThresholdActiveSizeRatio / 20),

                                case (ActiveSizeRatio =< (ThresholdActiveSizeRatio + ActiveSizeRatioDiff)) of
                                    true ->
                                        [{Node, ?ST_IDLING}|Acc];
                                    false ->
                                        Acc
                                end;
                            false ->
                                Acc
                        end;
                    _ ->
                        Acc
                end;
            _ ->
                Acc
        end,
    is_candidates(Rest, MaxNumOfNodes, Acc_1).

%% @private
is_candidates_1(Candidates) ->
    lists:member({erlang:node(), ?ST_IDLING}, Candidates).
