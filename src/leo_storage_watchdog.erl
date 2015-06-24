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
%% @doc Watchdog for Storage
%% @reference
%% @end
%%======================================================================
-module(leo_storage_watchdog).

-author('Yosuke Hara').

-behaviour(leo_watchdog_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_watchdog/include/leo_watchdog.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/4,
         stop/0
        ]).
-export([state/0]).

%% Callback
-export([init/1,
         update_property/3,
         handle_call/2,
         handle_fail/2]).

-record(state, {
          warn_active_size_ratio      = ?DEF_WARN_ACTIVE_SIZE_RATIO      :: pos_integer(),
          threshold_active_size_ratio = ?DEF_THRESHOLD_ACTIVE_SIZE_RATIO :: pos_integer(),
          threshold_num_of_notified_msgs = ?DEF_THRESHOLD_NUM_OF_NOTIFIED_MSGS :: pos_integer(),
          interval = timer:seconds(1) :: pos_integer()
         }).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Start the server
-spec(start_link(WarnActiveSizeRatio, ThresholdActiveSizeRatio,
                 ThresholdNumOfNotifiedMsgs, Interval) ->
             {ok,Pid} |
             ignore |
             {error,Error} when WarnActiveSizeRatio::pos_integer(),
                                ThresholdActiveSizeRatio::pos_integer(),
                                ThresholdNumOfNotifiedMsgs::pos_integer(),
                                Interval::pos_integer(),
                                Pid::pid(),
                                Error::{already_started,Pid} | term()).
start_link(WarnActiveSizeRatio, ThresholdActiveSizeRatio,
           ThresholdNumOfNotifiedMsgs, Interval) ->
    State = #state{warn_active_size_ratio = WarnActiveSizeRatio,
                   threshold_active_size_ratio = ThresholdActiveSizeRatio,
                   threshold_num_of_notified_msgs = ThresholdNumOfNotifiedMsgs,
                   interval = Interval},
    leo_watchdog:start_link(?MODULE, ?MODULE, State, Interval).


%% @doc Stop the server
-spec(stop() ->
             ok).
stop() ->
    leo_watchdog:stop(?MODULE).


%% @doc Retrieve state of the watchdog
-spec(state() ->
             not_found).
state() ->
    not_found.


%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------
%% @doc Initialize this process
-spec(init(State) ->
             ok | {error, Cause} when State::any(),
                                      Cause::any()).
init(_State) ->
    ok.


%% @doc Update a property
-spec(update_property(Item, Value, State) ->
             ok | {error, any()} when Item::atom(),
                                      Value::any(),
                                      State::#state{}).
update_property(_,_,_) ->
    ok.


%% @dog Call execution of the watchdog
-spec(handle_call(Id, State) ->
             {ok, State} |
             {{error,Error}, State} when Id::atom(),
                                         State::#state{},
                                         Error::any()).
handle_call(Id, #state{warn_active_size_ratio = WarningThreshold,
                       threshold_active_size_ratio = AlartThreshold,
                       threshold_num_of_notified_msgs = NumOfNotifiedMsgs} = State) ->
    ok = handle_ratio_of_fragment(Id, WarningThreshold, AlartThreshold),
    ok = handle_notified_messages(Id, NumOfNotifiedMsgs),
    {ok, State}.


%% @dog Call execution failed
-spec(handle_fail(Id, Cause) ->
             ok | {error,Error} when Id::atom(),
                                     Cause::any(),
                                     Error::any()).
handle_fail(_Id,_Cause) ->
    ok.


%%--------------------------------------------------------------------
%% Internal Function
%%--------------------------------------------------------------------
%% @doc Handle object-storage's fragment ratio for the data-compaction
%% @private
handle_ratio_of_fragment(Id, WarningThreshold, AlartThreshold) ->
    {ok, Stats} = leo_object_storage_api:stats(),
    {TotalSize, ActiveSize} =
        lists:foldl(fun(#storage_stats{total_sizes  = TSize,
                                       active_sizes = ASize},
                        {TSize_1, ASize_1}) ->
                            {TSize + TSize_1,
                             ASize + ASize_1};
                       (_, Acc) ->
                            Acc
                    end, {0,0}, Stats),

    Ratio = case (TotalSize > 0) of
                true ->
                    leo_math:ceiling(ActiveSize / TotalSize * 100);
                false ->
                    0
            end,


    case ((Ratio > 0.00 orelse (ActiveSize == 0 andalso TotalSize > 0))
          andalso Ratio =< WarningThreshold) of
        true when Ratio =< AlartThreshold ->
            %% raise error
            elarm:raise(Id, ?WD_ITEM_ACTIVE_SIZE_RATIO,
                        #watchdog_state{id = Id,
                                        level = ?WD_LEVEL_ERROR,
                                        src   = ?WD_ITEM_ACTIVE_SIZE_RATIO,
                                        props = [{ratio, Ratio}
                                                ]}),
            ok;
        true ->
            %% raise warning
            elarm:raise(Id, ?WD_ITEM_ACTIVE_SIZE_RATIO,
                        #watchdog_state{id = Id,
                                        level = ?WD_LEVEL_WARN,
                                        src   = ?WD_ITEM_ACTIVE_SIZE_RATIO,
                                        props = [{ratio, Ratio}
                                                ]}),
            ok;
        false ->
            elarm:clear(Id, ?WD_ITEM_ACTIVE_SIZE_RATIO),
            ok
    end.


%% @doc Handle a number of notified messages (timeout, slow-operation)
%% @private
handle_notified_messages(Id, NumOfNotifiedMsgs) ->
    case leo_storage_msg_collector:get() of
        {ok, []} ->
            elarm:clear(Id, ?WD_ITEM_NOTIFIED_MSGS);
        {ok, Msgs} ->
            WarnNumOfNotifiedMsgs = leo_math:ceiling(NumOfNotifiedMsgs / 2),
            try
                Len = erlang:length(leo_misc:get_value(?MSG_ITEM_TIMEOUT, Msgs, []))
                    + erlang:length(leo_misc:get_value(?MSG_ITEM_SLOW_OP, Msgs, [])),
                case (Len >= NumOfNotifiedMsgs) of
                    true ->
                        %% raise error
                        elarm:raise(Id, ?WD_ITEM_ACTIVE_SIZE_RATIO,
                                    #watchdog_state{id = Id,
                                                    level = ?WD_LEVEL_ERROR,
                                                    src   = ?WD_ITEM_NOTIFIED_MSGS,
                                                    props = [{num_of_notified_msgs, Len}
                                                            ]}),
                        ok;
                    false when Len >= WarnNumOfNotifiedMsgs ->
                        %% raise warning
                        elarm:raise(Id, ?WD_ITEM_ACTIVE_SIZE_RATIO,
                                    #watchdog_state{id = Id,
                                                    level = ?WD_LEVEL_WARN,
                                                    src   = ?WD_ITEM_NOTIFIED_MSGS,
                                                    props = [{num_of_notified_msgs, Len}
                                                            ]}),
                        ok;
                    false ->
                        elarm:clear(Id, ?WD_ITEM_NOTIFIED_MSGS)
                end
            catch
                _:_ ->
                    ok
            after
                leo_storage_msg_collector:clear()
            end
    end.
