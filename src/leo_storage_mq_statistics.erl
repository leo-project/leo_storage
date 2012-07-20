%%======================================================================
%%
%% Leo Storage
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
%% ---------------------------------------------------------------------
%% Leo Storage  - Statistics
%% @doc
%% @end
%%======================================================================
-module(leo_storage_mq_statistics).

-author('Yosuke Hara').

-behaviour(leo_statistics_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([init/0, sync/1]).

-define(SNMP_MSG_REPLICATE,  'num-of-msg-replicate').
-define(SNMP_MSG_SYNC_VNODE, 'num-of-msg-sync-vnode').
-define(SNMP_MSG_REBALANCE,  'num-of-msg-rebalance').

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Initialize metrics.
%%
-spec(init() -> ok).
init() ->
    ?debugVal(init),
    ok.


%% @doc Synchronize values.
%%
-spec(sync(?STAT_INTERVAL_1M | ?STAT_INTERVAL_5M) ->
             ok).
sync(?STAT_INTERVAL_1M) ->
    {ok, {Res0, _}} = leo_mq_api:status(?QUEUE_ID_REPLICATE_MISS),
    {ok, {Res1, _}} = leo_mq_api:status(?QUEUE_ID_INCONSISTENT_DATA),
    {ok, {Res2, _}} = leo_mq_api:status(?QUEUE_ID_SYNC_BY_VNODE_ID),
    {ok, {Res3, _}} = leo_mq_api:status(?QUEUE_ID_REBALANCE),

    catch snmp_generic:variable_set(?SNMP_MSG_REPLICATE,  Res0 + Res1),
    catch snmp_generic:variable_set(?SNMP_MSG_SYNC_VNODE, Res2),
    catch snmp_generic:variable_set(?SNMP_MSG_REBALANCE,  Res3),
    ok;

sync(?STAT_INTERVAL_5M) ->
    ok.

