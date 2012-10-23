%%======================================================================
%%
%% LeoFS Storage
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
    case whereis(?MODULE) of
        Pid when is_pid(Pid) == true ->
            exit(Pid, shutdown),
            ok;
        _ -> not_started
    end.


%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @spec (Params) -> ok
%% @doc stop process.
%% @end
%% @private
init([]) ->
    ChildProcs = [
                  {leo_storage_read_repairer_sup,
                   {leo_storage_read_repairer_sup, start_link, []},
                   permanent,
                   ?SHUTDOWN_WAITING_TIME,
                   supervisor,
                   [leo_storage_read_repairer_sup]}
                 ],
    {ok, {_SupFlags = {one_for_one, ?MAX_RESTART, ?MAX_TIME}, ChildProcs}}.

