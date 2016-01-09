%%====================================================================
%%
%% LeoFS Storage
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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

%% @doc default-values.
-define(SHUTDOWN_WAITING_TIME, 2000).
-define(MAX_RESTART, 5).
-define(MAX_TIME, 60).
-define(RETRY_TIMES, 5).

-ifdef(TEST).
-define(TIMEOUT, timer:seconds(1)).
-define(DEF_REQ_TIMEOUT, timer:seconds(1)).
-else.
-define(TIMEOUT, timer:seconds(5)).
-define(DEF_REQ_TIMEOUT, timer:seconds(30)).
-endif.

%% @doc operationg-methods.
-define(CMD_GET, get).
-define(CMD_PUT, put).
-define(CMD_DELETE, delete).
-define(CMD_HEAD, head).
-type(request_verb() :: ?CMD_GET |
                        ?CMD_PUT |
                        ?CMD_DELETE |
                        ?CMD_HEAD).

%% @doc queue-related.
-define(QUEUE_ID_PER_OBJECT,        'leo_per_object_queue').
-define(QUEUE_ID_SYNC_BY_VNODE_ID,  'leo_sync_by_vnode_id_queue').
-define(QUEUE_ID_DIRECTORY,         'leo_directory_queue').
-define(QUEUE_ID_REBALANCE,         'leo_rebalance_queue').
-define(QUEUE_ID_ASYNC_DELETE_OBJ,  'leo_async_deletion_queue').
-define(QUEUE_ID_RECOVERY_NODE,     'leo_recovery_node_queue').
-define(QUEUE_ID_SYNC_OBJ_WITH_DC,  'leo_sync_obj_with_dc_queue').
-define(QUEUE_ID_COMP_META_WITH_DC, 'leo_comp_meta_with_dc_queue').
-define(QUEUE_ID_ASYNC_DELETE_DIR,  'leo_async_delete_dir_queue').
-define(QUEUE_ID_ASYNC_RECOVER_DIR, 'leo_async_recover_dir_queue').
-define(QUEUE_ID_RECOVERY_FRAGMENT, 'leo_recovery_fragment_queue').
-define(QUEUE_ID_TRANS_FRAGMENT,    'leo_trans_fragment_queue').

-define(ERR_TYPE_REPLICATE_DATA,      'error_msg_replicate_data').
-define(ERR_TYPE_RECOVER_DATA,        'error_msg_recover_data').
-define(ERR_TYPE_DELETE_DATA,         'error_msg_delete_data').
-define(ERR_TYPE_REPLICATE_INDEX,     'error_msg_replicate_index').
-define(ERR_TYPE_RECOVER_INDEX,       'error_msg_recover_index').
-define(ERR_TYPE_DELETE_INDEX,        'error_msg_delete_index').
-define(ERR_TYPE_STORE_FRAGMENT,      'error_msg_store_fragment').

-define(TBL_REBALANCE_COUNTER,        'leo_rebalance_counter').

-define(SYNC_TARGET_OBJ, 'object').
-define(SYNC_TARGET_DIR, 'directory').
-type(obj_sync() :: ?SYNC_TARGET_OBJ |
                    ?SYNC_TARGET_DIR).

-define(DIR_ASYNC, 'async').
-define(DIR_SYNC, 'sync').
-type(dir_sync_option() :: ?DIR_ASYNC |
                           ?DIR_SYNC).


%% @doc error messages.
%%
-define(ERROR_COULD_NOT_GET_DATA, "Could not get data").
-define(ERROR_COULD_NOT_GET_META, "Could not get a metadata").
-define(ERROR_COULD_NOT_GET_METADATAS, "Could not get metadatas").
-define(ERROR_COULD_NOT_GET_SYSTEM_CONF,"Could not get the system configuration").
-define(ERROR_RECOVER_FAILURE, "Recover failure").
-define(ERROR_REPLICATE_FAILURE, "Replicate failure").
-define(ERROR_WRITE_FAILURE, "Write failure").
-define(ERROR_COULD_NOT_GET_REDUNDANCY, "Could not get redundancy").
-define(ERROR_COULD_NOT_CONNECT, "Could not connect").
-define(ERROR_COULD_NOT_MATCH, "Could not match").
-define(ERROR_COULD_SEND_OBJ, "Could not send an object to a remote cluster").
-define(ERROR_NOT_SATISFY_QUORUM, "Could not satisfy the quorum of the consistency level").
-define(ERROR_SYSTEM_HIGH_LOAD, "System High load").
-define(ERROR_COULD_NOT_UPDATE_LOG_LEVEL, "Could not update a log-level").


%% @doc notified message items
%%
-define(MSG_ITEM_TIMEOUT, 'timeout').
-define(MSG_ITEM_SLOW_OP, 'slow_op').


%% @doc request parameter for READ
%%
-record(read_parameter, {
          ref             :: reference(),
          addr_id         :: integer(),
          key = <<>>      :: binary(),
          etag = 0        :: non_neg_integer(),
          start_pos = -1  :: integer(),
          end_pos   = -1  :: integer(),
          quorum = 0      :: non_neg_integer(),
          req_id = 0      :: non_neg_integer()
         }).
-record(read_parameter_1, {
          ref             :: reference(),
          addr_id         :: integer(),
          key = <<>>      :: binary(),
          etag = 0        :: non_neg_integer(),
          start_pos = -1  :: integer(),
          end_pos   = -1  :: integer(),
          num_of_replicas :: pos_integer(),
          quorum = 0      :: non_neg_integer(),
          req_id = 0      :: non_neg_integer()
         }).
-define(READ_PARAMETER, 'read_parameter_1').


%% @doc Queue's Message.
%%
-record(inconsistent_data_message, {
          id = 0 :: non_neg_integer(),
          type :: atom(),
          addr_id = 0 :: non_neg_integer(),
          key :: any(),
          meta :: tuple(),
          timestamp = 0 :: non_neg_integer(),
          times = 0 :: non_neg_integer()}).

-record(inconsistent_index_message, {
          id = 0 :: non_neg_integer(),
          type :: atom(),
          addr_id = 0 :: non_neg_integer(),
          key :: any(),
          timestamp = 0 :: non_neg_integer(),
          times = 0 :: non_neg_integer()}).

-record(sync_unit_of_vnode_message, {
          id = 0 :: non_neg_integer(),
          vnode_id = 0 :: non_neg_integer(),
          node :: atom(),
          timestamp = 0 :: non_neg_integer(),
          times = 0 :: non_neg_integer()
         }).

-record(rebalance_message, {
          id = 0 :: non_neg_integer(),
          vnode_id = 0 :: non_neg_integer(),
          addr_id = 0 :: non_neg_integer(),
          key = <<>> :: binary(),
          node :: atom(),
          timestamp = 0 :: non_neg_integer(),
          times = 0 :: non_neg_integer()
         }).

-record(async_deletion_message, {
          id = 0 :: non_neg_integer(),
          addr_id = 0 :: non_neg_integer(),
          key :: any(),
          timestamp = 0 :: non_neg_integer(),
          times = 0 :: non_neg_integer()}).

-record(recovery_node_message, {
          id = 0 :: non_neg_integer(),
          node :: atom(),
          timestamp = 0 :: non_neg_integer(),
          times = 0 :: non_neg_integer()}).

-record(inconsistent_data_with_dc, {
          id = 0 :: non_neg_integer(),
          cluster_id :: atom(),
          addr_id = 0 :: non_neg_integer(),
          key :: binary(),
          del = 0 :: non_neg_integer(), %% del:[0:false, 1:true]
          timestamp = 0 :: non_neg_integer(),
          times = 0 :: non_neg_integer()}).

-record(comparison_metadata_with_dc, {
          id = 0 :: non_neg_integer(),
          cluster_id :: atom(),
          list_of_addrid_and_key :: list(),
          timestamp = 0 :: non_neg_integer()
         }).

-record(delete_dir, {
          id = 0 :: non_neg_integer(),
          node = undefined :: atom(),
          keys = [] :: [binary()|undefined],
          timestamp = 0 :: non_neg_integer()
         }).

-record(miss_storing_fragment, {
          id = 0 :: non_neg_integer(),
          addr_id = 0 :: non_neg_integer(),
          key = <<>> :: binary(),
          fragment_id_list = [] :: [non_neg_integer()],
          timestamp = 0 :: non_neg_integer()
         }).


%% @doc macros.
%%
-define(env_storage_device(),
        case application:get_env(leo_storage, obj_containers) of
            {ok, EnvStorageDevice} ->
                EnvStorageDevice;
            _ ->
                []
        end).

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

-define(env_queue_dir(),
        case application:get_env(leo_storage, queue_dir) of
            {ok, EnvQueueDir} -> EnvQueueDir;
            _ -> "./work/queue"
        end).

-define(env_mq_backend_db(),
        case application:get_env(leo_storage, mq_backend_db) of
            {ok, EnvMQBackendDB} ->
                EnvMQBackendDB;
            _ ->
                'bitcask'
        end).

-define(env_num_of_mq_procs(),
        case application:get_env(leo_storage, num_of_mq_procs) of
            {ok, NumOfMQProcs} -> NumOfMQProcs;
            _ -> 3
        end).

%% [MQ.interval beween batch processes]
-define(env_mq_interval_between_batch_procs_min(),
        case application:get_env(leo_storage, mq_interval_between_batch_procs_min) of
            {ok, MQWaitingTimeMin} -> MQWaitingTimeMin;
            _ -> 10
        end).
-define(env_mq_interval_between_batch_procs_max(),
        case application:get_env(leo_storage, mq_interval_between_batch_procs_max) of
            {ok, MQWaitingTimeMax} -> MQWaitingTimeMax;
            _ -> 1000
        end).

-define(env_mq_interval_between_batch_procs_reg(),
        case application:get_env(leo_storage, mq_interval_between_batch_procs_reg) of
            {ok, MQWaitingTimeReg} -> MQWaitingTimeReg;
            _ -> 100
        end).

-define(env_mq_interval_between_batch_procs_step(),
        case application:get_env(leo_storage, mq_interval_between_batch_procs_step) of
            {ok, MQWaitingTimeMax} -> MQWaitingTimeMax;
            _ -> 100
        end).

%% [MQ.batch prcoc of messages]
-define(env_mq_num_of_batch_process_min(),
        case application:get_env(leo_storage, mq_num_of_batch_process_min) of
            {ok, MQBatchProcsMin} -> MQBatchProcsMin;
            _ -> 100
        end).
-define(env_mq_num_of_batch_process_max(),
        case application:get_env(leo_storage, mq_num_of_batch_process_max) of
            {ok, MQBatchProcsMax} -> MQBatchProcsMax;
            _ -> 10000
        end).
-define(env_mq_num_of_batch_process_reg(),
        case application:get_env(leo_storage, mq_num_of_batch_process_reg) of
            {ok, MQBatchProcsReg} -> MQBatchProcsReg;
            _ -> 5000
        end).
-define(env_mq_num_of_batch_process_step(),
        case application:get_env(leo_storage, mq_num_of_batch_process_step) of
            {ok, MQBatchProcsStep} -> MQBatchProcsStep;
            _ -> 1000
        end).


-define(env_size_of_stacked_objs(),
        case application:get_env(leo_storage, size_of_stacked_objs) of
            {ok, SizeOfStackedObjs} -> SizeOfStackedObjs;
            _ -> (1024 * 1024) %% 1MB
        end).

-define(env_stacking_timeout(),
        case application:get_env(leo_storage, stacking_timeout) of
            {ok, StackingTimeout} -> timer:seconds(StackingTimeout);
            _ -> 1000 %% 1sec
        end).

-define(env_grp_level_1(),
        case application:get_env(leo_storage, grp_level_1) of
            {ok, _GrpLevel1} -> _GrpLevel1;
            _ -> []
        end).

-define(env_grp_level_2(),
        case application:get_env(leo_storage, grp_level_2) of
            {ok, _GrpLevel2} -> _GrpLevel2;
            _ -> []
        end).

-define(env_num_of_vnodes(),
        case application:get_env(leo_storage, num_of_vnodes) of
            {ok, _NumOfVNodes} -> _NumOfVNodes;
            _ -> 168
        end).


-define(DEF_MQ_NUM_OF_BATCH_PROC, 1).
-define(DEF_MQ_INTERVAL_MAX, 32).
-define(DEF_MQ_INTERVAL_MIN,  8).

%% @doc Retrieve a quorum bv a method for the replication method
-define(quorum(_Method,_W,_D),
        case _Method of
            ?CMD_DELETE ->
                _D;
            _ ->
                _W
        end).

%% @doc Retrieve a quorum bv a method for the erasure-coding method
-define(quorum_of_fragments(_Method,_N,_W,_D,_M,_TotalFragments),
        begin
            _Q = case _Method of
                     ?CMD_DELETE ->
                         leo_math:ceiling(_TotalFragments * _D/_N);
                     _ ->
                         leo_math:ceiling(_TotalFragments * _W/_N)
                 end,
            case _Q < _M of
                true ->
                    _M;
                false ->
                    _Q
            end
        end).


%% For Directory
-define(CONTAINER_BUF_SIZE, 524288). %% 512kb
-ifdef(TEST).
-define(CONTAINER_TIMEOUT,  1).
-define(CONTAINER_REMOVED_COUNT, 5).
-else.
-define(CONTAINER_TIMEOUT,  256).
-define(CONTAINER_REMOVED_COUNT, 64).
-endif.

-define(DIR_DB_PROP_PROCS, 'dir_db_num_of_procs').
-define(DIR_DB_PROP_NAME,  'dir_db_name').
-define(DIR_DB_PROP_PATH,  'dir_db_path').
-define(DIR_CONT_PROP_EXP_TIME, 'dir_cont_expiration_time').
-define(DIR_CONT_PROP_BUF_SIZE, 'dir_cont_buffer_size').

-define(DIR_DB_ID,   'leo_directory_db').
-define(DIR_DB_PROCS, 8).
-define(DIR_DB_NAME, 'leveldb').
-define(DIR_DB_PATH, "./avs/directory/").

-define(DEF_MAX_CACHE_DIR_METADATA, 10000).

-define(env_dir_db_procs(),
        case application:get_env(leo_storage, ?DIR_DB_PROP_PROCS) of
            {ok, _EnvDirDBProcs} -> _EnvDirDBProcs;
            _ -> ?DIR_DB_PROCS
        end).

-define(env_dir_db_name(),
        case application:get_env(leo_storage, ?DIR_DB_PROP_NAME) of
            {ok, _EnvDirDBName} -> _EnvDirDBName;
            _ -> ?DIR_DB_NAME
        end).

-define(env_dir_db_path(),
        case application:get_env(leo_storage, ?DIR_DB_PROP_PATH) of
            {ok, _EnvDirDBPath} -> _EnvDirDBPath;
            _ -> ?DIR_DB_PATH
        end).

-define(env_dir_cont_buffer_size(),
        case application:get_env(leo_storage, ?DIR_CONT_PROP_BUF_SIZE) of
            {ok, _EnvDirContBufSize} -> _EnvDirContBufSize;
            _ -> ?CONTAINER_BUF_SIZE
        end).

-define(env_dir_cont_expiration_time(),
        case application:get_env(leo_storage, ?DIR_CONT_PROP_EXP_TIME) of
            {ok, _EnvDirContExpTime} -> _EnvDirContExpTime;
            _ -> ?CONTAINER_TIMEOUT
        end).

-record(metadata_cache, {
          dir = <<>> :: binary(),
          rows = 0 :: non_neg_integer(),
          metadatas = [] :: [tuple()],
          created_at = 0 :: pos_integer()
         }).


%% For the multi-DC Replication and synchronization of data
%%
-define(DEF_PREFIX_MDCR_SYNC_PROC_1, "leo_mdcr_sync_w1_").
-define(DEF_PREFIX_MDCR_SYNC_PROC_2, "leo_mdcr_sync_w2_").
-define(DEF_MDCR_SYNC_PROC_BUFSIZE, 1024 * 1024 * 32).  %% 32MB
-define(DEF_MDCR_SYNC_PROC_TIMEOUT, timer:seconds(30)). %% 30sec
-define(DEF_MDCR_REQ_TIMEOUT,       timer:seconds(30)). %% 30sec
-define(DEF_MDCR_SYNC_PROCS, 1).
-define(DEF_RPC_LISTEN_PORT, 13075).
-define(DEF_MAX_RETRY_TIMES, 3).

-define(DEF_BIN_CID_SIZE,  16).     %% clusterid-size
-define(DEF_BIN_META_SIZE, 16).     %% metadata-size
-define(DEF_BIN_DIR_SIZE,  16).     %% directory-name-size
-define(DEF_BIN_OBJ_SIZE,  32).     %% object-size
-define(DEF_BIN_PADDING, <<0:64>>). %% footer


-ifdef(TEST).
-define(env_mdcr_sync_proc_buf_size(), 1024).
-define(env_mdcr_sync_proc_timeout(),    30).
-define(env_mdcr_req_timeout(),       30000).
-define(env_num_of_mdcr_sync_procs(),     1).
-define(env_rpc_port(), ?DEF_RPC_LISTEN_PORT).

-else.
-define(env_mdcr_sync_proc_buf_size(),
        case application:get_env(leo_storage, mdcr_size_of_stacked_objs) of
            {ok, _MDCRSyncProcBufSize} -> _MDCRSyncProcBufSize;
            _ -> ?DEF_MDCR_SYNC_PROC_BUFSIZE
        end).
-define(env_mdcr_sync_proc_timeout(),
        case application:get_env(leo_storage, mdcr_stacking_timeout) of
            {ok, _MDCRSyncProcTimeout} ->  timer:seconds(_MDCRSyncProcTimeout);
            _ -> ?DEF_MDCR_SYNC_PROC_TIMEOUT
        end).
-define(env_mdcr_req_timeout(),
        case application:get_env(leo_storage, mdcr_req_timeout) of
            {ok, _MDCRReqTimeout} -> _MDCRReqTimeout;
            _ -> ?DEF_MDCR_REQ_TIMEOUT
        end).
-define(env_num_of_mdcr_sync_procs(),
        case application:get_env(leo_storage, mdcr_stacking_procs) of
            {ok, _NumOfMDCRSyncProcs} -> _NumOfMDCRSyncProcs;
            _ -> ?DEF_MDCR_SYNC_PROCS
        end).
-define(env_rpc_port(),
        case application:get_env(leo_rpc, listen_port) of
            {ok, _ListenPort} -> _ListenPort;
            _ -> ?DEF_RPC_LISTEN_PORT
        end).
-endif.

-type(queue_id()   :: ?QUEUE_ID_PER_OBJECT |
                      ?QUEUE_ID_SYNC_BY_VNODE_ID |
                      ?QUEUE_ID_REBALANCE |
                      ?QUEUE_ID_ASYNC_DELETE_OBJ |
                      ?QUEUE_ID_RECOVERY_NODE |
                      ?QUEUE_ID_SYNC_OBJ_WITH_DC |
                      ?QUEUE_ID_COMP_META_WITH_DC |
                      ?QUEUE_ID_ASYNC_DELETE_DIR |
                      ?QUEUE_ID_ASYNC_RECOVER_DIR
                      ).

-define(mq_id_and_alias, [
                          {?QUEUE_ID_PER_OBJECT,        "recover inconsistent objs"},
                          {?QUEUE_ID_SYNC_BY_VNODE_ID,  "sync objs by vnode-id"},
                          {?QUEUE_ID_REBALANCE,         "rebalance objs"},
                          {?QUEUE_ID_ASYNC_DELETE_OBJ,  "async deletion of objs"},
                          {?QUEUE_ID_RECOVERY_NODE,     "recovery objs of node"},
                          {?QUEUE_ID_SYNC_OBJ_WITH_DC,  "sync objs w/remote-node"},
                          {?QUEUE_ID_COMP_META_WITH_DC, "compare metadata w/remote-node"},
                          {?QUEUE_ID_ASYNC_DELETE_DIR,  "delete directories"},
                          {?QUEUE_ID_ASYNC_RECOVER_DIR, "async metadata of directories recovery"}
                         ]).


%% For the watchdog
%%
%% @doc Threshold active size ratio:
%%    * round(active-size/total-size)
%%    * default:50%
-ifdef(TEST).
-define(DEF_WARN_ACTIVE_SIZE_RATIO, 95).
-define(DEF_THRESHOLD_ACTIVE_SIZE_RATIO, 90).
-define(DEF_THRESHOLD_NUM_OF_NOTIFIED_MSGS, 10).
-define(DEF_STORAGE_WATCHDOG_INTERVAL, timer:seconds(3)).
-else.
-define(DEF_WARN_ACTIVE_SIZE_RATIO, 55).
-define(DEF_THRESHOLD_ACTIVE_SIZE_RATIO, 50).
-define(DEF_THRESHOLD_NUM_OF_NOTIFIED_MSGS, 10).
-define(DEF_STORAGE_WATCHDOG_INTERVAL, timer:seconds(180)).
-endif.

-define(WD_ITEM_ACTIVE_SIZE_RATIO, 'active_size_ratio').
-define(WD_ITEM_NOTIFIED_MSGS, 'notified_msgs').
-define(WD_EXCLUDE_ITEMS, ['leo_storage_watchdog_fragment', 'leo_watchdog_cluster']).
-define(DEF_MAX_COMPACTION_PROCS, 1).
-define(DEF_AUTOCOMPACTION_INTERVAL, 3600). %% 3600sec (60min)

%% @doc for auto-compaction:
%%      <a number of data-compaction nodes at the same time>
%%          = <a active number of nodes> x <coefficient>
%%  -   high: 0.1
%%  - middle: 0.075
%%  -    low: 0.05
-define(DEF_COMPACTION_COEFFICIENT_MID, 0.075).

-define(env_warn_active_size_ratio(),
        case application:get_env(leo_storage, warn_active_size_ratio) of
            {ok, EnvWarnActiveSizeRatio} ->
                EnvWarnActiveSizeRatio;
            _ ->
                ?DEF_WARN_ACTIVE_SIZE_RATIO
        end).

-define(env_threshold_active_size_ratio(),
        case application:get_env(leo_storage, threshold_active_size_ratio) of
            {ok, EnvThresholdActiveSizeRatio} ->
                EnvThresholdActiveSizeRatio;
            _ ->
                ?DEF_THRESHOLD_ACTIVE_SIZE_RATIO
        end).

-define(env_threshold_num_of_notified_msgs(),
        case application:get_env(leo_storage, threshold_num_of_notified_msgs) of
            {ok, EnvThresholdNumOfNotifiedMsgs} ->
                EnvThresholdNumOfNotifiedMsgs;
            _ ->
                ?DEF_THRESHOLD_NUM_OF_NOTIFIED_MSGS
        end).

-define(env_storage_watchdog_interval(),
        begin
            _Time = erlang:phash2(erlang:node(), ?DEF_STORAGE_WATCHDOG_INTERVAL),
            case (_Time < timer:seconds(60)) of
                true ->
                    ?DEF_STORAGE_WATCHDOG_INTERVAL
                        - erlang:phash2(erlang:node(), timer:seconds(30));
                false ->
                    _Time
            end
        end).

%% @doc For the autonomic-operation
-define(env_auto_compaction_enabled(),
        case application:get_env(leo_storage, auto_compaction_enabled) of
            {ok, EnvAutoCompactionEnabled} ->
                EnvAutoCompactionEnabled;
            _ ->
                false
        end).

-define(env_auto_compaction_parallel_procs(),
        case application:get_env(leo_storage, auto_compaction_parallel_procs) of
            {ok, EnvAutoCompactionParallelProcs} ->
                EnvAutoCompactionParallelProcs;
            _ ->
                ?DEF_MAX_COMPACTION_PROCS
        end).

-define(env_auto_compaction_interval(),
        case application:get_env(leo_storage, auto_compaction_interval) of
            {ok, EnvAutoCompactionInterval} ->
                EnvAutoCompactionInterval;
            _ ->
                ?DEF_AUTOCOMPACTION_INTERVAL
        end).

-define(env_auto_compaction_coefficient(),
        case application:get_env(leo_storage, auto_compaction_coefficient) of
            {ok, EnvAutoCompactionCoefficient} ->
                EnvAutoCompactionCoefficient;
            _ ->
                ?DEF_COMPACTION_COEFFICIENT_MID
        end).


%% @doc For the erasure-coding support
-define(DEF_ERASURE_CODE_LIB, 'vandrs').
-define(DEF_ERASURE_CODE_MIN_OBJ_SIZE, 262144).

-define(env_erasure_code_lib(),
        case application:get_env(leo_storage, erasure_code_lib) of
            {ok, EnvErasureCodeLib} ->
                EnvErasureCodeLib;
            _ ->
                ?DEF_ERASURE_CODE_LIB
        end).

-define(env_erasure_code_min_object_size(),
        case application:get_env(leo_storage, min_object_size_for_ec) of
            {ok, EnvErasureCondingMinObjSize} ->
                EnvErasureCondingMinObjSize;
            _ ->
                ?DEF_ERASURE_CODE_MIN_OBJ_SIZE
        end).


%% Misc.
-define(DEF_SEEKING_METADATA_TIMEOUT, 10). %% default:50ms
-define(env_seeking_timeout_per_metadata(),
        case application:get_env(leo_storage, seeking_timeout_per_metadata) of
            {ok, EnvSeekingMetadataTimeout} ->
                EnvSeekingMetadataTimeout;
            _ ->
                ?DEF_SEEKING_METADATA_TIMEOUT
        end).
