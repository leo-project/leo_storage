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
-vsn('0.9.1').

-behaviour(supervisor).

-include("leo_storage.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
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
    ok  = setup_mnesia(),

    case (Ret = supervisor:start_link({local, ?MODULE}, ?MODULE, [])) of
        {ok, _} ->
            lists:foldl(fun({{volume, Path}, {num_of_containers, NumOfContainers}}, Index) ->
                                leo_object_storage_api:new(Index, NumOfContainers, Path),
                                Index + 1
                        end, 0, ?env_storage_device()),
            lists:foreach(fun(N) ->
                                  supervisor:start_child(leo_storage_replicator_sup,
                                                         [list_to_atom(?PFIX_REPLICATOR ++ integer_to_list(N))])
                          end, lists:seq(0, ?env_num_of_replicators() - 1)),
            lists:foreach(fun(N) ->
                                  supervisor:start_child(leo_storage_read_repairer_sup,
                                                         [list_to_atom(?PFIX_REPAIRER   ++ integer_to_list(N))])
                          end, lists:seq(0, ?env_num_of_repairers() - 1)),

            %% Launch Logger
            App = leo_storage,
            DefLogDir = "./log/",
            LogDir    = case application:get_env(App, log_appender) of
                            {ok, [{file, Options}|_]} ->
                                proplists:get_value(path, Options, DefLogDir);
                            _ ->
                                DefLogDir
                        end,
            ok = leo_logger_client_message:new(
                   LogDir, ?env_log_level(App), log_file_appender()),

            %% Launch MQ
            QDBRootPath = ?env_queue_dir(App),
            ok = leo_storage_mq_client:start(QDBRootPath),

            %% Launch Statistics
            ok = leo_statistics_api:start(?MODULE, App,
                                          [{snmp, [leo_statistics_metrics_vm,
                                                   leo_statistics_metrics_req,
                                                   leo_storage_mq_statistics
                                                  ]},
                                           {stat, [leo_statistics_metrics_vm]}]),

            %% Launch Redundant-Manager
            Managers = ?env_manager_nodes(App),
            ok = leo_redundant_manager_api:start(storage, Managers, QDBRootPath),

            %% Register in THIS-Process
            ok = leo_storage_api:register_in_monitor(first),
            Ret;
        Error ->
            Error
    end.


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
                  {leo_storage_replicator_sup,
                   {leo_storage_replicator_sup, start_link, []},
                   permanent,
                   ?SHUTDOWN_WAITING_TIME,
                   supervisor,
                   [leo_storage_replicator_sup]},

                  {leo_storage_read_repairer_sup,
                   {leo_storage_read_repairer_sup, start_link, []},
                   permanent,
                   ?SHUTDOWN_WAITING_TIME,
                   supervisor,
                   [leo_storage_read_repairer_sup]}
                 ],
    {ok, {_SupFlags = {one_for_one, ?MAX_RESTART, ?MAX_TIME}, ChildProcs}}.

%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
setup_mnesia() ->
    ok = application:start(mnesia),

    MnesiaMode = disc_copies,
    mnesia:change_table_copy_type(schema, node(), MnesiaMode),
    leo_redundant_manager_mnesia:create_members(MnesiaMode),
    mnesia:wait_for_tables([members], 30000),
    timer:sleep(1000),
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
log_file_appender([{Type, _}|T], Acc) when Type == zmq ->
    %% @TODO
    log_file_appender(T, [{?LOG_ID_ZMQ, ?LOG_APPENDER_ZMQ}|Acc]).

