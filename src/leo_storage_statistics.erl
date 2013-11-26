%%======================================================================
%%
%% Leo Storage
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
%% ---------------------------------------------------------------------
%% Leo Storage  - Statistics
%% @doc
%% @end
%%======================================================================
-module(leo_storage_statistics).

-author('Yosuke Hara').

-behaviour(leo_statistics_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/1]).
-export([init/0, handle_call/1]).

-define(SNMP_MSG_REPLICATE,  'num-of-msg-replicate').
-define(SNMP_MSG_SYNC_VNODE, 'num-of-msg-sync-vnode').
-define(SNMP_MSG_REBALANCE,  'num-of-msg-rebalance').

-define(SNMP_MSG_ACTIVE_SIZE, 'storage-active-objects-sizes').
-define(SNMP_MSG_ACTIVE_OBJS, 'storage-active-objects').
-define(SNMP_MSG_TOTAL_SIZE,  'storage-total-objects-sizes').
-define(SNMP_MSG_TOTAL_OBJS,  'storage-total-objects').

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
start_link(Interval) ->
    ok = leo_statistics_api:start_link(?MODULE, Interval),
    ok.

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
%% @doc Initialize metrics.
%%
-spec(init() -> ok).
init() ->
    ok.


%% @doc Synchronize values.
%%
-spec(handle_call({sync, ?STAT_INTERVAL_1M | ?STAT_INTERVAL_5M}) ->
             ok).
handle_call({sync, ?STAT_INTERVAL_1M}) ->
    {ok, {Res1, _}} = leo_mq_api:status(?QUEUE_ID_PER_OBJECT),
    {ok, {Res2, _}} = leo_mq_api:status(?QUEUE_ID_SYNC_BY_VNODE_ID),
    {ok, {Res3, _}} = leo_mq_api:status(?QUEUE_ID_REBALANCE),

    catch snmp_generic:variable_set(?SNMP_MSG_REPLICATE,  Res1),
    catch snmp_generic:variable_set(?SNMP_MSG_SYNC_VNODE, Res2),
    catch snmp_generic:variable_set(?SNMP_MSG_REBALANCE,  Res3),

    {ok, Ret4} = leo_object_storage_api:stats(),
    {TSize2, ASize2, TObjs2, AObjs2} =
        lists:foldl(fun({ok, #storage_stats{total_sizes  = TSize0,
                                            active_sizes = ASize0,
                                            total_num    = TObjs0,
                                            active_num   = AObjs0}}, {TSize1, ASize1, TObjs1, AObjs1}) ->
                            {TSize0 + TSize1,
                             ASize0 + ASize1,
                             TObjs0 + TObjs1,
                             AObjs0 + AObjs1};
                       (_, Acc) ->
                            Acc
                    end, {0,0,0,0}, Ret4),

    catch snmp_generic:variable_set(?SNMP_MSG_TOTAL_SIZE,  erlang:round(TSize2)),
    catch snmp_generic:variable_set(?SNMP_MSG_ACTIVE_SIZE, erlang:round(ASize2)),
    catch snmp_generic:variable_set(?SNMP_MSG_TOTAL_OBJS,  TObjs2),
    catch snmp_generic:variable_set(?SNMP_MSG_ACTIVE_OBJS, AObjs2),
    ok;

handle_call({sync, ?STAT_INTERVAL_5M}) ->
    ok.

