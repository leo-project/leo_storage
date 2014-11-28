%%======================================================================
%%
%% Leo Storage
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


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec(start() ->
             ok).
start() ->
    ok = leo_watchdog_sup:start_subscriber(?WD_SUB_ID_1, [?WD_ITEM_CPU_UTIL,
                                                          ?WD_ITEM_DISK_UTIL,
                                                          ?WD_ITEM_DISK_IO], ?MODULE),
    ok = leo_watchdog_sup:start_subscriber(?WD_SUB_ID_2, [?WD_ITEM_ACTIVE_SIZE_RATIO], ?MODULE),
    ok.


%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
-spec(handle_notify(Id, Alarm, Unixtime) ->
                 ok | {error, any()} when Id::atom(),
                                          Alarm::term(),
                                          Unixtime::non_neg_integer()).
handle_notify(?WD_SUB_ID_1,_Alarm,_Unixtime) ->
    %% Increase waiting time of data-compaction/batch-proc
    ok = leo_compact_fsm_controller:incr_waiting_time(),
    ok;
handle_notify(?WD_SUB_ID_2, #watchdog_alarm{state = #watchdog_state{
                                                       level = Level,
                                                       props = Props}},_Unixtime) ->
    case (Level >= ?WD_LEVEL_ERROR) of
        true ->
            case leo_compact_fsm_controller:state() of
                {ok, #compaction_stats{status = ?ST_IDLING,
                                       pending_targets = PendingTargets}} when PendingTargets /= [] ->
                    ok =leo_object_storage_api:compact_data(
                          PendingTargets, ?DEF_MAX_COMPACTION_PROCS,
                          fun leo_redundant_manager_api:has_charge_of_node/2),
                    Ratio = leo_misc:get_value('ratio', Props),
                    ?info("handle_notify/3",
                          "run-data-compaction - level:~w, ratio:~w%", [Level, Ratio]),
                    ok;
                _ ->
                    ok
            end;
        false ->
            ok
    end.

-spec(handle_notify(Id, State, SafeTimes, Unixtime) ->
             ok | {error, any()} when Id::atom(),
                                      State::[{atom(), any()}],
                                      SafeTimes::non_neg_integer(),
                                      Unixtime::non_neg_integer()).
handle_notify(?WD_SUB_ID_1,_State,_SafeTimes,_Unixtime) ->
    ok = leo_compact_fsm_controller:decr_waiting_time(),
    ok;
handle_notify(?WD_SUB_ID_2,_State,_SafeTimes,_Unixtime) ->
    ok.
