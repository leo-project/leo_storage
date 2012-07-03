%%====================================================================
%%
%% LeoFS Storage
%%
%% Copyright (c) 2012
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
%% -------------------------------------------------------------------
%% LeoFS Storage - Constant/Macro/Record
%%
%%====================================================================
-author('yosuke hara').
-vsn('0.9.0').

%% @doc default-values.
%%
-define(SHUTDOWN_WAITING_TIME, 2000).
-define(MAX_RESTART,              5).
-define(MAX_TIME,                60).

-ifdef(TEST).
-define(DEF_TIMEOUT,     1000).
-define(DEF_REQ_TIMEOUT, 1000).
-else.
-define(DEF_TIMEOUT,      3000). %%  3 sec
-define(DEF_REQ_TIMEOUT, 30000). %% 30 sec
-endif.

%% @doc operationg-methods.
%%
-define(CMD_GET,    get).
-define(CMD_PUT,    put).
-define(CMD_DELETE, delete).
-define(CMD_HEAD,   head).

%% @doc data-types.
%%
-define(TYPE_DIR,     directory).
-define(TYPE_OBJ,     object).
-define(REP_TYPE_DIR, 'directory').
-define(REP_TYPE_OBJ, 'data').

%% @doc prefix of process.
%%
-define(PFIX_REPLICATOR, "replicator_").
-define(PFIX_REPAIRER,   "repairer_").

%% @doc queue-related.
%%
-define(QUEUE_ID_REPLICATE_MISS,      'replicate_miss_queue').
-define(QUEUE_ID_INCONSISTENT_DATA,   'inconsistent_data_queue').
-define(QUEUE_ID_SYNC_BY_VNODE_ID,    'sync_by_vnode_id_queue').
-define(QUEUE_ID_DIRECTORY,           'directory_queue').
-define(QUEUE_ID_REBALANCE,           'rebalance_queue').

-define(QUEUE_TYPE_REPLICATION_MISS,  'replication_miss').
-define(QUEUE_TYPE_INCONSISTENT_DATA, 'inconsistent_data').
-define(QUEUE_TYPE_SYNC_BY_VNODE_ID,  'sync_by_vnode_id').
-define(QUEUE_TYPE_REBALANCE,         'rebalance').

-define(ERR_TYPE_REPLICATE_DATA,      'replicate_data').
-define(ERR_TYPE_RECOVER_DATA,        'recover_data').
-define(ERR_TYPE_DELETE_DATA,         'delete_data').
-define(ERR_TYPE_REPLICATE_INDEX,     'replicate_index').
-define(ERR_TYPE_RECOVER_INDEX,       'recover_index').
-define(ERR_TYPE_DELETE_INDEX,        'delete_index').

-define(TBL_REBALANCE_COUNTER,        'rebalance_counter').

%% @doc error messages.
%%
-define(ERROR_COULD_NOT_GET_DATA,       "could not get data").
-define(ERROR_COULD_NOT_GET_META,       "could not get metadata").
-define(ERROR_RECOVER_FAILURE,          "recover failure").
-define(ERROR_REPLICATE_FAILURE,        "replicate failure").
-define(ERROR_META_NOT_FOUND,           "metadata not found").
-define(ERROR_COULD_NOT_GET_REDUNDANCY, "could not get redundancy").
-define(ERROR_COULD_NOT_CONNECT,        "could not connect").


%% @doc Queue's Message.
%%
-record(inconsistent_data_message, {
          id = 0                :: integer(),
          type                  :: atom(),
          addr_id               :: integer(),
          key                   :: any(),
          meta                  :: tuple(),
          timestamp             :: integer(),
          times = 0             :: integer()}).

-record(inconsistent_index_message, {
          id = 0                :: integer(),
          type                  :: atom(),
          addr_id               :: integer(),
          key                   :: any(),
          timestamp             :: integer(),
          times = 0             :: integer()}).

-record(sync_unit_of_vnode_message, {
          id = 0                :: integer(),
          vnode_id              :: integer(),
          node                  :: atom(),
          timestamp             :: integer(),
          times = 0             :: integer()
         }).

-record(rebalance_message, {
          id = 0                :: integer(),
          vnode_id              :: integer(),
          addr_id               :: integer(),
          key                   :: string(),
          node                  :: atom(),
          timestamp             :: integer(),
          times = 0             :: integer()
         }).

%% @doc macros.
%%
-define(env_storage_device(),
        case application:get_env(leo_storage, obj_containers) of
            {ok, EnvStorageDevice} -> EnvStorageDevice;
            _ -> []
        end).

%% -define(env_directory_db_path(),
%%         case application:get_env(leo_storage, directory_db_path) of
%%             {ok, DirectoryDBPath} -> DirectoryDBPath;
%%             _ -> ""
%%         end).

%% -define(env_num_of_dir_backend_db_procs(),
%%         case application:get_env(leo_storage, directory_db_procs) of
%%             {ok, DirectoryDBProcs} -> DirectoryDBProcs;
%%             _ -> 8
%%         end).

-define(env_num_of_replicators(),
        case application:get_env(leo_storage, num_of_replicators) of
            {ok, NumOfReplicators} -> NumOfReplicators;
            _ -> 8
        end).

-define(env_num_of_repairers(),
        case application:get_env(leo_storage, num_of_repairers) of
            {ok, NumOfRepairers} -> NumOfRepairers;
            _ -> 8
        end).

-define(env_num_of_mq_procs(),
        case application:get_env(leo_storage, num_of_mq_procs) of
            {ok, NumOfMQProcs} -> NumOfMQProcs;
            _ -> 3
        end).

