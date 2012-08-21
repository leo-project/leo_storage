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
after_proc({ok, Pid}) ->
    QueueDir = ?env_queue_dir(leo_storage),
    Managers  = ?env_manager_nodes(leo_storage),

    ok = launch_logger(),
    ok = launch_object_storage(),
    ok = launch_replicator(),
    ok = launch_repairer(),
    ok = leo_storage_mq_client:start(QueueDir),
    ok = leo_statistics_api:start(leo_storage_sup, leo_storage,
                                  [{snmp, [leo_statistics_metrics_vm,
                                           leo_statistics_metrics_req,
                                           leo_storage_mq_statistics
                                          ]},
                                   {stat, [leo_statistics_metrics_vm]}]),
    ok = leo_redundant_manager_api:start(storage, Managers, QueueDir),
    ok = leo_ordning_reda_api:start(),
    ok = leo_storage_api:register_in_monitor(first),
    {ok, Pid};

after_proc(Error) ->
    Error.


%% @doc Launch Logger
%%
launch_logger() ->
    DefLogDir = "./log/",
    LogDir    = case application:get_env(leo_storage, log_appender) of
                    {ok, [{file, Options}|_]} ->
                        proplists:get_value(path, Options, DefLogDir);
                    _ ->
                        DefLogDir
                end,
    LogLevel  = ?env_log_level(leo_storage),
    leo_logger_client_message:new(LogDir, LogLevel, log_file_appender()).


%% @doc Launch Object-Storage
%%
launch_object_storage() ->
    Device     = ?env_storage_device(),
    Containers = proplists:get_value(num_of_containers, Device),
    Path       = proplists:get_value(path,              Device),
    leo_object_storage_api:start(Containers, Path).


%% @doc Launch Replicator
%%
launch_replicator() ->
    lists:foreach(fun(N) ->
                          supervisor:start_child(leo_storage_replicator_sup,
                                                 [list_to_atom(?PFIX_REPLICATOR ++ integer_to_list(N))])
                  end, lists:seq(0, ?env_num_of_replicators() - 1)),
    ok.


%% @doc Launch Repairer
%%
launch_repairer() ->
    lists:foreach(fun(N) ->
                          supervisor:start_child(leo_storage_read_repairer_sup,
                                                 [list_to_atom(?PFIX_REPAIRER   ++ integer_to_list(N))])
                  end, lists:seq(0, ?env_num_of_repairers() - 1)),
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
    log_file_appender(T, [{?LOG_ID_FILE_ERROR, ?LOG_APPENDER_FILE}|[{?LOG_ID_FILE_INFO, ?LOG_APPENDER_FILE}|Acc]]);
%% @TODO
log_file_appender([{Type, _}|T], Acc) when Type == zmq ->
    log_file_appender(T, [{?LOG_ID_ZMQ, ?LOG_APPENDER_ZMQ}|Acc]).
