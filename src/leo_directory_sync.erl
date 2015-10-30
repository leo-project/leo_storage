%%======================================================================
%%
%% Leo Storage
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
%% ---------------------------------------------------------------------
%% @doc Directory synchronizer
%% @reference https://github.com/leo-project/leo_storage/blob/master/src/leo_directory_sync.erl
%% @end
%%======================================================================
-module(leo_directory_sync).

-author('Yosuke Hara').

-behaviour(leo_ordning_reda_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_ordning_reda/include/leo_ordning_reda.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start/0,
         add_container/1, remove_container/1,
         append/1, append/2,
         bulk_append/2,
         create_directories/1,
         replicate_metadatas/1, replicate_metadatas/2,
         replicate_del_dir/2, replicate_del_dir/3,
         store/2,
         delete/1, delete/2,
         delete_sub_dir/2, delete_sub_dir/3,
         recover/1, recover/2
        ]).
-export([handle_send/3,
         handle_fail/2]).
-export([get_directories/1,
         get_directory_from_key/1
        ]).

-define(BIN_SLASH, <<"/">>).


%%--------------------------------------------------------------------
%% API-1
%%--------------------------------------------------------------------
%% @doc Launch the directory's db(s)
%%
-spec(start() ->
             ok | {error, any()}).
start() ->
    Sup = leo_backend_db_sup,
    case whereis(Sup) of
        undefined ->
            {error, {no_procs, Sup}};
        SupRef ->
            DirDBProcs = ?env_dir_db_procs(),
            DirDBName  = ?env_dir_db_name(),
            DirDBPath  = ?env_dir_db_path(),
            DirDBPath_1 = case DirDBProcs of
                              1 ->
                                  filename:join([DirDBPath, "1"]);
                              _ ->
                                  DirDBPath
                          end,
            leo_backend_db_sup:start_child(
              SupRef, ?DIR_DB_ID, DirDBProcs, DirDBName, DirDBPath_1)
    end.


%% @doc Add a container into the supervisor
%%
-spec(add_container(DestNode) ->
             ok | {error, any()} when DestNode::atom()).
add_container(DestNode) ->
    ContBufferSize = ?env_dir_cont_buffer_size(),
    ContExpTime = ?env_dir_cont_expiration_time(),
    leo_ordning_reda_api:add_container(
      {metadata, DestNode}, [{?PROP_ORDRED_MOD,      ?MODULE},
                             {?PROP_ORDRED_BUF_SIZE, ContBufferSize},
                             {?PROP_ORDRED_IS_COMP,  false},
                             {?PROP_ORDRED_TIMEOUT,  ContExpTime},
                             {?PROP_REMOVED_COUNT,   ?CONTAINER_REMOVED_COUNT}
                            ]).


%% @doc Remove a container from the supervisor
%%
-spec(remove_container(DestNode) ->
             ok | {error, any()} when DestNode::atom()).
remove_container(DestNode) ->
    leo_ordning_reda_api:remove_container({metadata, DestNode}).


%% @doc Append a object into the ordning&reda
-spec(append(Metadata) ->
             ok when Metadata::#?METADATA{}).
append(#?METADATA{key = <<>>}) ->
    ok;
append(Metadata) ->
    append(Metadata, ?DIR_ASYNC).

-spec(append(Metadata, SyncOption) ->
             ok when Metadata::#?METADATA{},
                     SyncOption::dir_sync_option()).
append(#?METADATA{key = Key} = Metadata, SyncOption) ->
    %% Retrieve a directory from the key
    case get_directory_from_key(Key) of
        <<>> ->
            ok;
        Dir ->
            %% Retrieve destination nodes
            case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
                {ok, #redundancies{nodes = RedundantNodes}} ->
                    %% Store/Remove the metadata into the metadata-cluster
                    append_1(Dir, Metadata, RedundantNodes, SyncOption);
                {error,_Cause} ->
                    enqueue(Metadata)
            end
    end.

%% @private
-spec(append_1(Dir, Metadata, RedundantNodes, SyncOption) ->
             ok | {error, any()} when Dir::binary(),
                                      Metadata::#?METADATA{},
                                      RedundantNodes::[#redundant_node{}],
                                      SyncOption::dir_sync_option()).
append_1(_,_,[],_) ->
    ok;
append_1(Dir, Metadata, [#redundant_node{available = false}|Rest], SyncOption) ->
    ok = enqueue(Metadata),
    append_1(Dir, Metadata, Rest, SyncOption);
append_1(Dir, Metadata, [#redundant_node{available = true,
                                         node = Node}|Rest], ?DIR_SYNC = SyncOption) ->
    #?METADATA{addr_id = AddrId,
               key = Key,
               del = Del} = Metadata,
    StackedInfo = [{AddrId, Dir, Key, Del}],
    Bin = get_dir_and_metadata_bin(Dir, Metadata),
    case handle_send({metadata, Node}, StackedInfo, Bin) of
        ok ->
            ok;
        {error,_Cause} ->
            enqueue(Metadata)
    end,
    append_1(Dir, Metadata, Rest, SyncOption);
append_1(Dir, Metadata, [#redundant_node{available = true,
                                         node = Node}|Rest], ?DIR_ASYNC = SyncOption) ->
    append_2(Node, Dir, Metadata),
    append_1(Dir, Metadata, Rest, SyncOption).

%% @private
-spec(append_2(DestNodes, Dir, Metadata) ->
             ok | {error, any()} when DestNodes::[atom()],
                                      Dir::binary(),
                                      Metadata::#?METADATA{}).
append_2(DestNode, Dir, #?METADATA{addr_id = AddrId,
                                   key = Key,
                                   del = Del} = Metadata) ->
    case leo_ordning_reda_api:has_container({metadata, DestNode}) of
        true ->
            Data = get_dir_and_metadata_bin(Dir, Metadata),
            leo_ordning_reda_api:stack({metadata, DestNode},
                                       {AddrId, Dir, Key, Del}, Data);
        false ->
            case add_container(DestNode) of
                ok ->
                    append_2(DestNode, Dir, Metadata);
                Error ->
                    error_logger:info_msg("~p,~p,~p,~p~n",
                                          [{module, ?MODULE_STRING},
                                           {function, "append/3"},
                                           {line, ?LINE}, {body, Error}]),
                    enqueue(Metadata)
            end
    end.


%% @doc Append metadatas in bulk
-spec(bulk_append(Metadatas, SyncOption) ->
             [any()] when Metadatas::[#?METADATA{}],
                          SyncOption::dir_sync_option()).
bulk_append(Metadatas, SyncOption) ->
    bulk_append_1(Metadatas, SyncOption, {[],[]}).

%% @private
bulk_append_1([],_, SoFar) ->
    SoFar;
bulk_append_1([#?METADATA{key = Key} = Metadata|Rest], SyncOption, {ResL, Errors}) ->
    Acc = case append(Metadata, SyncOption) of
              ok ->
                  {[{ok, Key}|ResL], Errors};
              Error ->
                  {ResL, [{Key, Error}|Errors]}
          end,
    bulk_append_1(Rest, SyncOption, Acc).


%% @doc Retrieve binary of a directory and a metadata
%% @private
get_dir_and_metadata_bin(Dir, Metadata) ->
    DirSize = byte_size(Dir),
    MetaBin = term_to_binary(Metadata),
    MetaSize = byte_size(MetaBin),
    << DirSize:?DEF_BIN_DIR_SIZE,
       Dir/binary,
       MetaSize:?DEF_BIN_META_SIZE,
       MetaBin/binary,
       ?DEF_BIN_PADDING/binary >>.


%% @doc Create directories
-spec(create_directories(Dir) ->
             ok when Dir::[binary()]).
create_directories(DirL) ->
    create_directories(DirL, dict:new()).

%% @private
create_directories([], Dict) ->
    case dict:to_list(Dict) of
        [] ->
            ok;
        RetL ->
            [replicate_metadatas(Node, MetadataL) || {Node, MetadataL} <- RetL],
            ok
    end;
create_directories([Dir|Rest], Dict) ->
    Metadata = #?METADATA{key = Dir,
                          ksize = byte_size(Dir),
                          dsize = -1,
                          clock = leo_date:clock(),
                          timestamp = leo_date:now()},
    Parent = get_directory_from_key(Dir),
    case leo_redundant_manager_api:get_redundancies_by_key(Parent) of
        {ok, #redundancies{id = AddrId,
                           nodes = Nodes}} ->
            Metadata_1 = Metadata#?METADATA{addr_id = AddrId},
            Nodes_1 = [{N, A} || #redundant_node{node = N,
                                                 available = A} <- Nodes],
            Dict_1 = create_directories_1(Nodes_1, Metadata_1, Dict),
            create_directories(Rest, Dict_1);
        _ ->
            ok = enqueue(Metadata),
            create_directories(Rest, Dict)
    end.

%% @private
create_directories_1([],_, Dict) ->
    Dict;
create_directories_1([{_Node, false}|Rest], Metadata, Dict) ->
    ok = enqueue(Metadata),
    create_directories_1(Rest, Metadata, Dict);
create_directories_1([{Node, true}|Rest], Metadata, Dict) ->
    Dict_1 = dict:append(Node, Metadata, Dict),
    create_directories_1(Rest, Metadata, Dict_1).


%% @doc Replicate metadata(s)
%%
-spec(replicate_metadatas(MetadataL) ->
             ok when MetadataL::[#?METADATA{}]).
replicate_metadatas(MetadataL) ->
    replicate_metadatas_1(MetadataL).

-spec(replicate_metadatas(Node, MetadataL) ->
             ok when Node::atom(),
                     MetadataL::[#?METADATA{}]).
replicate_metadatas(Node, MetadataL) ->
    RPCKey = rpc:async_call(Node, ?MODULE, replicate_metadatas, [MetadataL]),
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
              {value, ok} ->
                  ok;
              {value, {_, Cause}} ->
                  {error, Cause};
              timeout = Cause ->
                  {error, Cause};
              Other ->
                  {error, Other}
          end,
    case Ret of
        ok ->
            ok;
        {error,_} = Error ->
            [enqueue(M) || M <- MetadataL],
            Error
    end.

%% @private
-spec(replicate_metadatas_1([#?METADATA{}]) ->
             ok).
replicate_metadatas_1([]) ->
    ok;
replicate_metadatas_1([#?METADATA{key = Key} = Metadata|Rest]) ->
    Parent = get_directory_from_key(Key),
    KeyBin = << Parent/binary, "\t", Key/binary >>,

    case leo_backend_db_api:put(?DIR_DB_ID,
                                KeyBin, term_to_binary(Metadata)) of
        ok ->
            leo_directory_cache:append(Parent, Metadata);
        {error, Cause} ->
            error_logger:info_msg("~p,~p,~p,~p~n",
                                  [{module, ?MODULE_STRING},
                                   {function, "replicate_1/1"},
                                   {line, ?LINE}, {body, Cause}]),
            enqueue(Metadata)
    end,
    replicate_metadatas_1(Rest).


%% @doc Store received objects into the directory's db
%%
-spec(store(Ref, StackedBin) ->
             ok | {error, any()} when Ref::reference(),
                                      StackedBin::binary()).
store(Ref, StackedBin) ->
    {Ret, DirL} = slice_and_store(StackedBin, dict:new()),
    ok = restore_dir_metadata(DirL),
    ok = leo_directory_cache:merge(),
    {Ret, Ref}.


%% @doc Remove the directory and objects under the directory
%%
-spec(delete(Dir) ->
             ok | not_found | {error, any()} when Dir::binary()).
delete(Dir) ->
    delete(Dir, []).

-spec(delete(Dir, KeyL) ->
             ok | not_found | {error, any()} when Dir::binary(),
                                                  KeyL::[binary()]).
delete(Dir, KeyL) ->
    case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
        {ok, #redundancies{nodes = RedundantNodes}} ->
            delete_1(Dir, KeyL, RedundantNodes);
        Error ->
            Error
    end.

%% @private
delete_1(_,_,[]) ->
    ok;
delete_1(Dir, KeyL, [#redundant_node{available = false}|Rest]) ->
    %% Enqueue a message to fix an inconsistent dir
    _ = leo_storage_mq:publish(
          ?QUEUE_TYPE_ASYNC_DELETE_DIR, [Dir]),
    delete_1(Dir, KeyL, Rest);
delete_1(Dir, KeyL, [#redundant_node{available = true,
                                     node = Node}|Rest]) ->
    _ = replicate_del_dir(Node, Dir, KeyL),
    delete_1(Dir, KeyL, Rest).


%% @doc Replicate deletion of a dir
%%
-spec(replicate_del_dir(Dir, KeyL) ->
             ok when Dir::binary(),
                     KeyL::[binary()]).
replicate_del_dir(Dir, KeyL) ->
    replicate_del_dir_1([Dir|KeyL]).

-spec(replicate_del_dir(Node, Dir, KeyL) ->
             ok when Node::atom(),
                     Dir::binary(),
                     KeyL::[binary()]).
replicate_del_dir(Node, Dir, KeyL) ->
    RPCKey = rpc:async_call(Node, ?MODULE, replicate_del_dir, [Dir, KeyL]),
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
              {value, ok} ->
                  ok;
              {value, {_, Cause}} ->
                  {error, Cause};
              timeout = Cause ->
                  {error, Cause};
              Other ->
                  {error, Other}
          end,
    case Ret of
        ok ->
            ok;
        {error, Reason} = Error ->
            ?warn("replicate_del_dir/2", [{cause, Reason}]),
            leo_storage_mq:publish(
              ?QUEUE_TYPE_ASYNC_DELETE_DIR, [Dir]),
            Error
    end.

%% @private
-spec(replicate_del_dir_1(KeyL) ->
             ok when KeyL::[binary()]).
replicate_del_dir_1([]) ->
    ok;
replicate_del_dir_1([Path|Rest]) ->
    Parent = get_directory_from_key(Path),
    KeyBin = << Parent/binary, "\t", Path/binary >>,

    %% Remove the directory from a backend-db
    case leo_backend_db_api:delete(?DIR_DB_ID, KeyBin) of
        ok ->
            %% Remove the directory from a cache-server
            case catch binary:part(Path, (byte_size(Path) - 1), 1) of
                ?BIN_SLASH ->
                    %% Remove the dir from the parent cache
                    ok = delete_sub_dir(Parent, Path),
                    %% Remove the dir from the cache
                    leo_directory_cache:delete(Path);
                _ ->
                    void
            end;
        {error, Cause} ->
            ?warn("replicate_del_dir_1/1", [{cause, Cause}]),
            leo_storage_mq:publish(
              ?QUEUE_TYPE_ASYNC_DELETE_DIR, [Path])
    end,
    replicate_del_dir_1(Rest).


%% @doc Remove a sub directory from a parent dir
%%
-spec(delete_sub_dir(ParentDir, Dir) ->
             ok when ParentDir::binary(),
                     Dir::binary()).
delete_sub_dir(ParentDir, Dir) ->
    case leo_redundant_manager_api:get_redundancies_by_key(ParentDir) of
        {ok, #redundancies{nodes = RedundantNodes}} ->
            delete_sub_dir_1(RedundantNodes, ParentDir, Dir);
        {error, Cause} ->
            %% @TODO: enqueue replication miss of dir-deletion
            ?warn("replicate_del_dir_1/1", [{cause, Cause}]),
            ok
    end.

-spec(delete_sub_dir(Ref, ParentDir, Dir) ->
             {ok, Ref} | {error, {Ref, Cause}} when Ref::reference(),
                                                    ParentDir::binary(),
                                                    Dir::binary(),
                                                    Cause::any()).
delete_sub_dir(Ref, ParentDir, Dir) ->
    case leo_cache_api:get(ParentDir) of
        {ok, CacheBin} ->
            Cache = binary_to_term(CacheBin),

            case lists:keyfind(Dir, 2, Cache) of
                false ->
                    {ok, Ref};
                _Tuple ->
                    %% update the cache
                    Ret = case lists:keydelete(Dir, 2, Cache) of
                              [] ->
                                  leo_cache_api:delete(ParentDir);
                              Cache_1 ->
                                  leo_cache_api:put(
                                    ParentDir, term_to_binary(Cache_1))
                          end,
                    case Ret of
                        ok ->
                            {ok, Ref};
                        {error, Cause} ->
                            {ok, {Ref, Cause}}
                    end
            end;
        not_found ->
            {ok, Ref};
        {error, Cause} ->
            {error, {Ref, Cause}}
    end.


%% @private
delete_sub_dir_1([],_ParentDir,_Dir) ->
    ok;
delete_sub_dir_1([#redundant_node{node = Node,
                                  available = false}|Rest], ParentDir, Dir) ->
    %% @TODO: enqueue replication miss of dir-deletion
    ?warn("replicate_del_dir_1/1", [{node, Node}, {cause, not_available}]),
    delete_sub_dir_1(Rest, ParentDir, Dir);
delete_sub_dir_1([#redundant_node{node = Node}|Rest], ParentDir, Dir) ->
    Ref = make_ref(),
    RPCKey = rpc:async_call(Node, ?MODULE, delete_sub_dir, [Ref, ParentDir, Dir]),
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
              {value, {ok, Ref}} ->
                  ok;
              {value, {error, {Ref, Cause}}} ->
                  {error, Cause};
              timeout = Cause ->
                  {error, Cause};
              Other ->
                  {error, Other}
          end,

    case Ret of
        ok ->
            void;
        {error, Why} ->
            %% @TODO: enqueue replication miss of dir-deletion
            ?warn("replicate_del_dir_1/1", [{node, Node}, {cause, Why}])
    end,
    delete_sub_dir_1(Rest, ParentDir, Dir).


%% @doc Restore a dir's metadata
%% @private
-spec(restore_dir_metadata([{Dir, {Clock, Timestamp}}]) ->
             ok when Dir::binary(),
                     Clock::non_neg_integer(),
                     Timestamp::non_neg_integer()).
restore_dir_metadata([]) ->
    ok;
restore_dir_metadata([{Dir, {Clock, Timestamp}}|Rest]) ->
    Parent = get_directory_from_key(Dir),
    KeyBin = << Parent/binary, "\t", Dir/binary >>,
    Metadata = #?METADATA{key = Dir,
                          ksize = byte_size(Dir),
                          dsize = -1,
                          clock = Clock,
                          timestamp = Timestamp},
    case leo_backend_db_api:put(?DIR_DB_ID,
                                KeyBin, term_to_binary(Metadata)) of
        ok ->
            leo_directory_cache:append(Parent, Metadata);
        {error, Cause} ->
            error_logger:info_msg("~p,~p,~p,~p~n",
                                  [{module, ?MODULE_STRING},
                                   {function, "restore_dir_metadata/1"},
                                   {line, ?LINE}, {body, Cause}]),
            enqueue(Metadata)
    end,
    restore_dir_metadata(Rest).


%% @doc Retrieve values from a stacked-bin,
%%      then store restored value into the metadata-db
%% @private
slice_and_store(<<>>, Acc) ->
    {ok, lists:sort(dict:to_list(Acc))};
slice_and_store(StackedBin, Acc) ->
    case slice(StackedBin) of
        {ok, {DirBin, #?METADATA{key = Key,
                                 clock = Clock,
                                 del = Del} = Metadata, StackedBin_1}} ->
            KeyBin = << DirBin/binary, "\t", Key/binary >>,
            ValBin = term_to_binary(Metadata),
            CanPutVal =
                case leo_backend_db_api:get(?DIR_DB_ID, KeyBin) of
                    {ok, ValBin_1} ->
                        case catch binary_to_term(ValBin_1) of
                            #?METADATA{clock = Clock_1} when Clock >= Clock_1 ->
                                true;
                            _Metadata ->
                                false
                        end;
                    not_found when Del == ?DEL_TRUE ->
                        false;
                    not_found when Del == ?DEL_FALSE ->
                        true;
                    {error,_Cause} ->
                        ok = enqueue(Metadata),
                        false
                end,

            %% Store and Remove a metadata from the dir's metadata-db
            %% and append a dir into the set
            Acc_1 =
                case CanPutVal of
                    true when Del == ?DEL_FALSE ->
                        case leo_backend_db_api:put(?DIR_DB_ID, KeyBin, ValBin) of
                            ok ->
                                append_dir(DirBin, Metadata, Acc);
                            {error, Cause} ->
                                error_logger:info_msg("~p,~p,~p,~p~n",
                                                      [{module, ?MODULE_STRING},
                                                       {function, "slice_and_store/1"},
                                                       {line, ?LINE}, {body, Cause}]),
                                enqueue(Metadata),
                                Acc
                        end;
                    true when Del == ?DEL_TRUE ->
                        case leo_backend_db_api:delete(?DIR_DB_ID, KeyBin) of
                            ok ->
                                Acc;
                            {error, Cause} ->
                                error_logger:info_msg("~p,~p,~p,~p~n",
                                                      [{module, ?MODULE_STRING},
                                                       {function, "slice_and_store/1"},
                                                       {line, ?LINE}, {body, Cause}]),
                                enqueue(Metadata),
                                Acc
                        end;
                    false ->
                        Acc
                end,

            %% Append a metadata into the cache-manager
            case (dict:size(Acc_1) > 0) of
                true ->
                    ok = leo_directory_cache:append(DirBin, Metadata);
                false ->
                    void
            end,
            slice_and_store(StackedBin_1, Acc_1);
        _ ->
            {{error, invalid_format},[]}
    end.


%% @doc Append a dir
%% @private
append_dir(_,#?METADATA{del = ?DEL_TRUE}, Acc) ->
    Acc;
append_dir(DirBin, #?METADATA{key = Key} = Metadata, Acc) ->
    append_dir_1(binary:last(Key), DirBin, Metadata, Acc).

%% @private
append_dir_1(TailBin, DirBin, #?METADATA{clock = Clock,
                                         timestamp = Timestamp}, Acc) ->
    %% Seek and put the directory
    Current = {Clock, Timestamp},
    case TailBin of
        16#2f ->
            Acc;
        _ ->
            case dict:is_key(DirBin, Acc) of
                true ->
                    case dict:find(DirBin, Acc) of
                        {ok, {Clock_1, Timestamp_1}} when Clock < Clock_1 ->
                            dict:store(DirBin, {Clock_1, Timestamp_1}, Acc);
                        error ->
                            dict:store(DirBin, Current, Acc);
                        _ ->
                            Acc
                    end;
                false ->
                    dict:store(DirBin, Current, Acc)
            end
    end.


%% @doc Retrieve a value from a stacked-bin
%% @private
slice(Bin) ->
    try
        %% Retrieve metadata
        << DirSize:?DEF_BIN_DIR_SIZE, Rest_1/binary >> = Bin,
        DirBin = binary:part(Rest_1, {0, DirSize}),
        Rest_2 = binary:part(Rest_1, {DirSize, byte_size(Rest_1) - DirSize}),
        %% Retrieve object
        << MetaSize:?DEF_BIN_META_SIZE, Rest_3/binary >> = Rest_2,
        MetaBin = binary:part(Rest_3, {0, MetaSize}),
        Rest_4  = binary:part(Rest_3, {MetaSize, byte_size(Rest_3) - MetaSize}),
        %% Retrieve footer
        <<_Fotter:64, Rest_5/binary>> = Rest_4,

        Metadata = binary_to_term(MetaBin),
        {ok, {DirBin, Metadata, Rest_5}}
    catch
        _:Cause ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "slice/1"},
                                      {line, ?LINE}, {body, Cause}]),
            {error, invalid_format}
    end.


%% @doc Recover a metadata
-spec(recover(Metadata) ->
             ok | {error, any()} when Metadata::#?METADATA{}).
recover(#?METADATA{key = <<>>}) ->
    {error, invalid_data};
recover(#?METADATA{} = Metadata) ->
    append(Metadata);
recover(_) ->
    {error, invalid_data}.


-spec(recover(AddrId, Key) ->
             ok | {error, any()} when AddrId :: integer(),
                                      Key :: binary()).
recover(AddrId, Key) ->
    %% Retrieve a metadata
    KeyBin = term_to_binary({AddrId, Key}),
    case leo_backend_db_api:get(?DIR_DB_ID, KeyBin) of
        {ok, ValBin} ->
            case catch binary_to_term(ValBin) of
                #?METADATA{} = Metadata ->
                    recover(Metadata);
                _ ->
                    {error, invalid_data}
            end;
        Other ->
            Other
    end.


%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------
%% @doc Handle send object to a remote-node.
%%
handle_send({metadata, DestNode}, StackedInfo, CompressedObjs) ->
    %% Retrieve and stack directories of part of a key
    case get_directories_from_stacked_info(StackedInfo, ordsets:new()) of
        [] ->
            void;
        Dirs ->
            create_directories(Dirs)
    end,

    %% Store and replicate directories into the cluster
    Ref = make_ref(),
    RPCKey = rpc:async_call(DestNode, ?MODULE, store, [Ref, CompressedObjs]),
    case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
        {value, {ok, Ref}} ->
            ok;
        {value, {_, Cause}} ->
            {error, Cause};
        timeout = Cause ->
            {error, Cause};
        Other ->
            {error, Other}
    end;
handle_send(_,_,_) ->
    ok.


%% @doc Handle a fail process
%%
-spec(handle_fail(atom(), list({integer(), string()})) ->
             ok | {error, any()}).
handle_fail(_Unit, []) ->
    ok;
handle_fail({metadata, _} = Unit, [{_AddrId,_Dir, Key}|Rest]) ->
    ok = enqueue(#?METADATA{key = Key}),
    handle_fail(Unit, Rest);
handle_fail(Unit,[_|Rest]) ->
    handle_fail(Unit, Rest).


%% @doc Retrieve a directory from the key
-spec(get_directory_from_key(Key) ->
             binary() when Key::binary()).
get_directory_from_key(<<>>) ->
    <<>>;
get_directory_from_key(Key) ->
    BinSlash = <<"/">>,
    case catch binary:last(Key) of
        {'EXIT',_Cause} ->
            <<>>;
        %% "/"
        16#2f ->
            case binary:matches(Key, [BinSlash],[]) of
                [] ->
                    <<>>;
                RetMatches ->
                    case (length(RetMatches) > 1) of
                        true ->
                            [_,{Pos,_}|_] = lists:reverse(RetMatches),
                            binary:part(Key, 0, Pos + 1);
                        false ->
                            <<>>
                    end
            end;
        _Other ->
            case binary:matches(Key, [BinSlash],[]) of
                [] ->
                    <<>>;
                RetL ->
                    {Len,_} = lists:last(RetL),
                    Bin = binary:part(Key, 0, Len),
                    << Bin/binary, BinSlash/binary >>
            end
    end.


%% @doc Retrieve a directories from the metadata
-spec(get_directories(Metadata) ->
             [binary()] when Metadata::#?METADATA{}).
get_directories(#?METADATA{key = Key} = Metadata) ->
    BinSlash = <<"/">>,
    case leo_misc:binary_tokens(Key, BinSlash) of
        [] ->
            [];
        Tokens ->
            case binary:matches(Key, [BinSlash],[]) of
                [] ->
                    [];
                RetL ->
                    Pos = byte_size(Key) - 1,
                    case lists:last(RetL) of
                        {Len,_} when Pos == Len ->
                            get_directories_1(Tokens, Metadata, true, [], <<>>);
                        _ ->
                            Tokens_1 = lists:sublist(Tokens, length(Tokens)-1),
                            get_directories_1(Tokens_1, Metadata, false, [], <<>>)
                    end
            end
    end.

%% @private
get_directories_1([], Metadata,_IsDir, Acc,_Sofar) ->
    get_directories_2(Acc, Metadata, ordsets:new());
get_directories_1([H|T], Metadata, IsDir, Acc, Sofar) ->
    Bin = << Sofar/binary, H/binary, "/" >>,
    get_directories_1(T, Metadata, IsDir, [Bin|Acc], Bin).

%% @private
get_directories_2([],_Metadata, []) ->
    [];
get_directories_2([],_Metadata, Acc) ->
    ordsets:to_list(Acc);
get_directories_2([H|T], #?METADATA{clock = Clock,
                                    timestamp = Timestamp} = Metadata, Acc) ->
    get_directories_2(T, Metadata, ordsets:add_element(#?METADATA{key = H,
                                                                  ksize = byte_size(H),
                                                                  clock = Clock,
                                                                  timestamp = Timestamp}, Acc)).


%% @doc Retrieve directories from the stacked info
%% @private
-spec(get_directories_from_stacked_info(StackedInfo, Acc) ->
             [binary()] when StackedInfo::[{integer(), binary(), binary(), integer()}],
                             Acc::any()).
get_directories_from_stacked_info([], Acc) ->
    ordsets:to_list(Acc);
get_directories_from_stacked_info([{_AddrId,_Dir,_Key, ?DEL_TRUE}|Rest], Acc) ->
    get_directories_from_stacked_info(Rest, Acc);
get_directories_from_stacked_info([{_AddrId,_Dir, Key, ?DEL_FALSE}|Rest], Acc) ->
    DirL = case get_directories(#?METADATA{key = Key,
                                           ksize = byte_size(Key)}) of
               [] ->
                   [];
               RetL ->
                   [Key_1 || #?METADATA{key = Key_1} <- RetL]
           end,
    Acc_1 = get_directories_from_stacked_info_1(DirL, Acc),
    get_directories_from_stacked_info(Rest, Acc_1).

%% @private
get_directories_from_stacked_info_1([], Acc) ->
    Acc;
get_directories_from_stacked_info_1([Dir|Rest], Acc) ->
    Acc_1 = ordsets:add_element(Dir, Acc),
    get_directories_from_stacked_info_1(Rest, Acc_1).


%% @doc enqueue a message of a miss-replication into the queue
%% @private
enqueue(#?METADATA{key = Key} = Metadata) ->
    case leo_redundant_manager_api:get_redundancies_by_key(Key) of
        {ok, #redundancies{id = AddrId}} ->
            case catch leo_storage_mq:publish(?QUEUE_TYPE_ASYNC_RECOVER_DIR,
                                              Metadata#?METADATA{addr_id = AddrId}) of
                {'EXIT', Cause} ->
                    ?error("enqueue/1",
                           [{key, Key}, {cause, Cause}]);
                _ ->
                    void
            end,
            ok;
        {error, Cause} ->
            ?error("enqueue/1",
                   [{key, Key}, {cause, Cause}]),
            ok
    end.
