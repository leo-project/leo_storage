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
%% LeoFS - Supervisor.
%% @doc
%% @end
%%======================================================================
-module(leo_storage_sup).

-author('Yosuke Hara').

-behaviour(supervisor).

-include("leo_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0,
         stop/0]).

-export([init/1]).

%%-----------------------------------------------------------------------
%% External API
%%-----------------------------------------------------------------------
%% @spec () -> ok
%% @doc start link...
%% @end
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec () -> ok |
%%             not_started
%% @doc stop process.
%% @end
stop() ->
    ok.


%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @spec (Params) -> ok
%% @doc stop process.
%% @end
%% @private
init([]) ->
    %% Watchdog for rex's binary usage
    CheckInterval = ?env_watchdog_check_interval(),
    Watchdogs_1 = case ?env_watchdog_rex_enabled() of
                      true ->
                          MaxMemCapacity = ?env_watchdog_max_mem_capacity(),
                          [{leo_watchdog_rex,
                            {leo_watchdog_rex, start_link,
                             [MaxMemCapacity,
                              CheckInterval
                             ]},
                            permanent,
                            ?SHUTDOWN_WAITING_TIME,
                            worker,
                            [leo_watchdog_rex]}];
                      false ->
                          []
                  end,

    %% Wachdog for CPU
    Watchdogs_2 = case ?env_watchdog_cpu_enabled() of
                      true ->
                          MaxCPULoadAvg = ?env_watchdog_max_cpu_load_avg(),
                          MaxCPUUtil    = ?env_watchdog_max_cpu_util(),
                          [{leo_watchdog_cpu,
                            {leo_watchdog_cpu, start_link,
                             [MaxCPULoadAvg,
                              MaxCPUUtil,
                              CheckInterval
                             ]},
                            permanent,
                            ?SHUTDOWN_WAITING_TIME,
                            worker,
                            [leo_watchdog_cpu]} | Watchdogs_1];
                      false ->
                          Watchdogs_1
                  end,

    %% Wachdog for IO
    Watchdogs_3 = case ?env_watchdog_io_enabled() of
                      true ->
                          MaxInput  = ?env_watchdog_max_input_for_interval(),
                          MaxOutput = ?env_watchdog_max_output_for_interval(),
                          [{leo_watchdog_io,
                            {leo_watchdog_io, start_link,
                             [MaxInput,
                              MaxOutput,
                              CheckInterval
                             ]},
                            permanent,
                            ?SHUTDOWN_WAITING_TIME,
                            worker,
                            [leo_watchdog_io]} | Watchdogs_2 ];
                      false ->
                          Watchdogs_2
                  end,
    {ok, {_SupFlags = {one_for_one, ?MAX_RESTART, ?MAX_TIME}, Watchdogs_3}}.



