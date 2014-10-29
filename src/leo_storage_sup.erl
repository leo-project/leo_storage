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
    MaxMemCapacity = ?env_watchdog_max_mem_capacity(),
    MaxCPULoadAvg  = ?env_watchdog_max_cpu_load_avg(),
    MaxCPUUtil     = ?env_watchdog_max_cpu_util(),
    MaxInput       = ?env_watchdog_max_input_for_interval(),
    MaxOutput      = ?env_watchdog_max_output_for_interval(),
    _MaxDiskUtil   = ?env_watchdog_max_disk_util(),
    CheckInterval  = ?env_watchdog_check_interval(),
    _TargetPaths = lists:map(
                     fun(Item) ->
                             {ok, Curr} = file:get_cwd(),
                             Path = leo_misc:get_value(path, Item),
                             Path1 = case Path of
                                         "/"   ++ _Rest -> Path;
                                         "../" ++ _Rest -> Path;
                                         "./"  ++  Rest -> Curr ++ "/" ++ Rest;
                                         _              -> Curr ++ "/" ++ Path
                                     end,
                             Path2 = case (string:len(Path1) == string:rstr(Path1, "/")) of
                                         true  -> Path1;
                                         false -> Path1 ++ "/"
                                     end,
                             Path2
                     end, ?env_storage_device()),

    WatchDogs = [
                 %% Watchdog for rex's binary usage
                 {leo_watchdog_rex,
                  {leo_watchdog_rex, start_link,
                   [MaxMemCapacity,
                    CheckInterval
                   ]},
                  permanent,
                  ?SHUTDOWN_WAITING_TIME,
                  worker,
                  [leo_watchdog_rex]},

                 %% Wachdog for CPU
                 {leo_watchdog_cpu,
                  {leo_watchdog_cpu, start_link,
                   [MaxCPULoadAvg,
                    MaxCPUUtil,
                    CheckInterval
                   ]},
                  permanent,
                  ?SHUTDOWN_WAITING_TIME,
                  worker,
                  [leo_watchdog_cpu]},

                 %% Wachdog for IO
                 {leo_watchdog_io,
                  {leo_watchdog_io, start_link,
                   [MaxInput,
                    MaxOutput,
                    CheckInterval
                   ]},
                  permanent,
                  ?SHUTDOWN_WAITING_TIME,
                  worker,
                  [leo_watchdog_io]}

                 %% @PENDING
                 %% Wachdog for Disk
                 %% {leo_watchdog_disk,
                 %%  {leo_watchdog_disk, start_link,
                 %%   [TargetPaths,
                 %%    MaxDiskUtil,
                 %%    CheckInterval
                 %%   ]},
                 %%  permanent,
                 %%  ?SHUTDOWN_WAITING_TIME,
                 %%  worker,
                 %%  [leo_watchdog_disk]}
                ],
    {ok, {_SupFlags = {one_for_one, ?MAX_RESTART, ?MAX_TIME}, WatchDogs}}.
