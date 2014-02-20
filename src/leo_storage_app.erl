%%======================================================================
%%
%% LeoFS Storage
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
%% LeoFS Storage - Application
%% @doc
%% @end
%%======================================================================
-module(leo_storage_app).

-author('Yosuke Hara').

-behaviour(application).

-include("leo_storage.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Application and Supervisor callbacks
-export([start/2, stop/1]).

%%----------------------------------------------------------------------
%% Application behaviour callbacks
%%----------------------------------------------------------------------
start(_Type, _Args) ->
    Res = leo_storage_sup:start_link(),
    after_proc(Res).


stop(_State) ->
    ok.


%%----------------------------------------------------------------------
%% INNER FUNCTION
%%----------------------------------------------------------------------
%% @private
ensure_started(Id, Module, Method, Type, Timeout) ->
    case whereis(Id) of
        undefined ->
            ChildSpec = {Id, {Module, Method, []},
                         permanent, Timeout, Type, [Module]},
            {ok, _} = supervisor:start_child(kernel_sup, ChildSpec);
        Pid -> Pid
    end.

%% @private
after_proc({ok, Pid}) ->
    ensure_started(inet_db, inet_db, start_link, worker, 2000),
    ensure_started(net_sup, erl_distribution, start_link, supervisor, infinity),

    %% Launch servers
    QueueDir = ?env_queue_dir(leo_storage),
    Managers = ?env_manager_nodes(leo_storage),
    ok = launch_logger(),
    ok = launch_object_storage(Pid),
    ok = launch_redundant_manager(Pid, Managers, QueueDir),
    ok = leo_ordning_reda_api:start(),

    Intervals = ?env_mq_consumption_intervals(),
    ok = leo_storage_mq_client:start(Pid, Intervals, QueueDir),

    %% After processing
    ensure_started(rex, rpc, start_link, worker, 2000),
    ok = leo_storage_api:register_in_monitor(first),

    %% Launch metric-servers
    ok = leo_statistics_api:start_link(leo_storage),
    ok = leo_statistics_api:create_tables(ram_copies, [node()]),
    ok = leo_metrics_vm:start_link(?SNMP_SYNC_INTERVAL_10S),
    ok = leo_metrics_req:start_link(?SNMP_SYNC_INTERVAL_60S),
    ok = leo_storage_statistics:start_link(?SNMP_SYNC_INTERVAL_60S),
    {ok, Pid};

after_proc(Error) ->
    Error.


%% @doc Launch Logger
%%
launch_logger() ->
    DefLogDir = "./log/",
    LogDir    = case application:get_env(leo_storage, log_appender) of
                    {ok, [{file, Options}|_]} ->
                        leo_misc:get_value(path, Options, DefLogDir);
                    _ ->
                        DefLogDir
                end,
    LogLevel  = ?env_log_level(leo_storage),
    leo_logger_client_message:new(LogDir, LogLevel, log_file_appender()).


%% @doc Launch Object-Storage
%%
launch_object_storage(RefSup) ->
    ObjStoageInfo = case ?env_storage_device() of
                        [] -> [];
                        Devices ->
                            lists:map(fun(Item) ->
                                              Containers = leo_misc:get_value(num_of_containers, Item),
                                              Path       = leo_misc:get_value(path,              Item),
                                              {Containers, Path}
                                      end, Devices)
                    end,

    ChildSpec = {leo_object_storage_sup,
                 {leo_object_storage_sup, start_link, [ObjStoageInfo]},
                 permanent, 2000, supervisor, [leo_object_storage_sup]},
    {ok, _} = supervisor:start_child(RefSup, ChildSpec),
    ok.


%% @doc Launch redundnat-manager
%% @private
launch_redundant_manager(RefSup, Managers, QueueDir) ->
    ChildSpec = {leo_redundant_manager_sup,
                 {leo_redundant_manager_sup, start_link, [storage, Managers, QueueDir]},
                 permanent, 2000, supervisor, [leo_redundant_manager_sup]},
    {ok, _} = supervisor:start_child(RefSup, ChildSpec),
    ok.


%% @doc Retrieve log-appneder(s)
%% @private
-spec(log_file_appender() ->
             list()).
log_file_appender() ->
    case application:get_env(leo_storage, log_appender) of
        undefined   -> log_file_appender([], []);
        {ok, Value} -> log_file_appender(Value, [])
    end.

log_file_appender([], []) ->
    [{?LOG_ID_FILE_INFO,  ?LOG_APPENDER_FILE},
     {?LOG_ID_FILE_ERROR, ?LOG_APPENDER_FILE}];
log_file_appender([], Acc) ->
    lists:reverse(Acc);
log_file_appender([{Type, _}|T], Acc) when Type == file ->
    log_file_appender(T, [{?LOG_ID_FILE_ERROR, ?LOG_APPENDER_FILE}|[{?LOG_ID_FILE_INFO, ?LOG_APPENDER_FILE}|Acc]]).
