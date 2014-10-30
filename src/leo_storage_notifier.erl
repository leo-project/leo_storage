%%======================================================================
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
%% ---------------------------------------------------------------------
%% @doc
%% @end
%%======================================================================
-module(leo_storage_notifier).

-author('Yosuke Hara').

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_watchdog/include/leo_watchdog.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(leo_notify_behaviour).

-export([notify/3]).


%%-----------------------------------------------------------------------
%% API
%%-----------------------------------------------------------------------
-spec(notify(Id, Level, State) ->
             ok | {error, any()} when Id::atom(),
                                      Level::watchdog_level(),
                                      State::[{atom(), any()}]).
notify('leo_watchdog_cpu', ?WD_LEVEL_ERROR, State) ->
    ?error("notify/2", "state:~p", [State]),
    ok;
notify('leo_watchdog_cpu', ?WD_LEVEL_WARN, State) ->
    ?warn("notify/2", "state:~p", [State]),
    ok;
notify('leo_watchdog_io', ?WD_LEVEL_ERROR, State) ->
    ?error("notify/2", "state:~p", [State]),
    ok;
notify('leo_watchdog_io', ?WD_LEVEL_WARN, State) ->
    ?warn("notify/2", "state:~p", [State]),
    ok;
notify(_,_,_) ->
    ok.
