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
         append/1, append/2, append/3,
         append_metadatas/2,
         create_directories/1,
         replicate/1, replicate/2,
         store/2,
         cache_dir_metadata/1,
         recover/1, recover/2
        ]).
-export([handle_send/3,
         handle_fail/2]).
-export([get_directories/1,
         get_directory_from_key/1
        ]).


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
    leo_ordning_reda_api:add_container(
      {metadata, DestNode}, [{?PROP_ORDRED_MOD,      ?MODULE},
                             {?PROP_ORDRED_BUF_SIZE, ?CONTAINER_BUF_SIZE},
                             {?PROP_ORDRED_IS_COMP,  false},
                             {?PROP_ORDRED_TIMEOUT,  ?CONTAINER_TIMEOUT}]).


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
    append(async, Metadata).
append(SyncMode, #?METADATA{addr_id = AddrId,
                            key = Key} = Metadata) ->
    %% Retrieve a directory from the key
    case get_directory_from_key(Key) of
        <<>> ->
            ok;
        Dir ->
            %% Retrieve destination nodes
            case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
                {ok, #redundancies{nodes = RedundantNodes}} ->
                    %% Store/Remove the metadata into the metadata-cluster
                    case [Node || #redundant_node{available = true,
                                                  node = Node} <- RedundantNodes] of
                        [] ->
                            enqueue(Metadata);
                        ActiveNodes when SyncMode == sync ->
                            StackedInfo = [{AddrId, Dir, Key}],
                            Bin = get_dir_and_metadata_bin(Dir, Metadata),
                            handle_send({metadata, hd(ActiveNodes)}, StackedInfo, Bin);
                        ActiveNodes when SyncMode == async ->
                            lists:foreach(fun(N) ->
                                                  append(N, Dir, Metadata)
                                          end, ActiveNodes),
                            ok
                    end;
                {error,_Cause} ->
                    enqueue(Metadata)
            end
    end.

%% @private
-spec(append(DestNodes, Dir, Metadata) ->
             ok | {error, any()} when DestNodes::[atom()],
                                      Dir::binary(),
                                      Metadata::#?METADATA{}).
append(DestNode, Dir, #?METADATA{addr_id = AddrId,
                                 key = Key} = Metadata) ->
    case leo_ordning_reda_api:has_container({metadata, DestNode}) of
        true ->
            Data = get_dir_and_metadata_bin(Dir, Metadata),
            leo_ordning_reda_api:stack({metadata, DestNode}, {AddrId, Dir, Key}, Data);
        false ->
            case add_container(DestNode) of
                ok ->
                    append(DestNode, Dir, Metadata);
                Error ->
                    error_logger:info_msg("~p,~p,~p,~p~n",
                                          [{module, ?MODULE_STRING}, {function, "append/3"},
                                           {line, ?LINE}, {body, Error}]),
                    ok = enqueue(Metadata),
                    Error
            end
    end.


%% @doc Append metadatas in bulk
-spec(append_metadatas(SyncMode, Metadatas) ->
             [any()] when SyncMode::async|sync,
                          Metadatas::[#?METADATA{}]).
append_metadatas(SyncMode, Metadatas) ->
    append_metadatas_1(SyncMode, Metadatas, {[],[]}).

%% @private
append_metadatas_1(_, [], SoFar) ->
    SoFar;
append_metadatas_1(SyncMode, [#?METADATA{key = Key} = Metadata|Rest], {ResL, Errors}) ->
    Acc = case append(SyncMode, Metadata) of
              ok ->
                  {[{ok, Key}|ResL], Errors};
              Error ->
                  {ResL, [{Key, Error}|Errors]}
          end,
    append_metadatas_1(SyncMode, Rest, Acc).


%% @doc Retrieve binary of a directory and a metadata
%% @private
get_dir_and_metadata_bin(Dir, Metadata) ->
    DirSize  = byte_size(Dir),
    MetaBin  = term_to_binary(Metadata),
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
            ?debugVal(RetL),
            [replicate(Node, MetadataL) || {Node, MetadataL} <- RetL],
            ok
    end;
create_directories([Dir|Rest], Dict) ->
    Metadata = #?METADATA{key = Dir,
                          ksize = byte_size(Dir),
                          dsize = -1,
                          clock = leo_date:clock(),
                          timestamp = leo_date:now()},
    case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
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


%% @doc Replicate a metadata
%%
-spec(replicate(MetadataL) ->
             ok when MetadataL::[#?METADATA{}]).
replicate(MetadataL) ->
    replicate_1(MetadataL).

-spec(replicate(Node, MetadataL) ->
             ok when Node::atom(),
                     MetadataL::[#?METADATA{}]).
replicate(Node, MetadataL) ->
    RPCKey = rpc:async_call(Node, ?MODULE, replicate, [MetadataL]),
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
-spec(replicate_1([#?METADATA{}]) ->
             ok).
replicate_1([]) ->
    ok;
replicate_1([#?METADATA{key = Key} = Metadata|Rest]) ->
    Parent = get_directory_from_key(Key),
    KeyBin = << Parent/binary, "\t", Key/binary >>,
    case leo_backend_db_api:put(?DIR_DB_ID,
                                KeyBin, term_to_binary(Metadata)) of
        ok ->
            ok;
        {error, Cause} ->
            error_logger:info_msg("~p,~p,~p,~p~n",
                                  [{module, ?MODULE_STRING},
                                   {function, "replicate_1/1"},
                                   {line, ?LINE}, {body, Cause}]),
            enqueue(Metadata)
    end,
    replicate_1(Rest).


%% @doc Store received objects into the directory's db
%%
-spec(store(Ref, StackedBin) ->
             ok | {error, any()} when Ref::reference(),
                                      StackedBin::binary()).
store(Ref, StackedBin) ->
    {Ret, DirL} = slice_and_store(StackedBin, dict:new()),
    ok = restore_dir_metadata(DirL),
    ok = cache_dir_metadata(DirL),
    {Ret, Ref}.


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
            ok;
        {error, Cause} ->
            error_logger:info_msg("~p,~p,~p,~p~n",
                                  [{module, ?MODULE_STRING},
                                   {function, "restore_dir_metadata/1"},
                                   {line, ?LINE}, {body, Cause}]),
            enqueue(Metadata)
    end,
    restore_dir_metadata(Rest).


%% @doc Cache metadatas under the dir
%% @private
-spec(cache_dir_metadata([{Dir, {Clock, Timestamp}}]) ->
             ok when Dir::binary(),
                     Clock::non_neg_integer(),
                     Timestamp::non_neg_integer()).
cache_dir_metadata([]) ->
    ok;
cache_dir_metadata([{Dir,_}|Rest]) ->
    %% Re-cache metadatas under the directory
    case leo_cache_api:delete(Dir) of
        ok ->
            case leo_storage_handler_directory:find_by_parent_dir(
                   Dir, [], <<>>, ?DEF_MAX_CACHE_DIR_METADATA) of
                {ok, MetadataList} when MetadataList /= [] ->
                    leo_cache_api:put(Dir, term_to_binary(
                                             #metadata_cache{dir = Dir,
                                                             rows = length(MetadataList),
                                                             metadatas = MetadataList,
                                                             created_at = leo_date:now()
                                                            }));
                _ ->
                    void
            end;
        {error, Cause} ->
            ?error("cache_dir_metadata/1",
                   "dir:~p, cause:~p", [Dir, Cause])
    end,
    cache_dir_metadata(Rest).


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
                                append_dir(DirBin, Metadata, Acc);
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
            slice_and_store(StackedBin_1, Acc_1);
        _ ->
            {{error, invalid_format},[]}
    end.


%% @doc Append a dir
%% @private
append_dir(DirBin, #?METADATA{key = Key} = Metadata, Acc) ->
    append_dir_1(binary:last(Key), DirBin, Metadata, Acc).

%% @private
append_dir_1(16#2f, DirBin, #?METADATA{key = Key} = Metadata, Acc) ->
    case leo_cache_api:get(DirBin) of
        {ok, CacheBin} ->
            case binary_to_term(CacheBin) of
                #metadata_cache{
                   metadatas = MetaList} = MetaCache ->
                    case lists:keyfind(Key, 2, MetaList) of
                        false ->
                            OrdSets = ordsets:from_list(MetaList),
                            MetaList_1 = ordsets:to_list(ordsets:add_element(Metadata, OrdSets)),
                            leo_cache_api:put(
                              DirBin, term_to_binary(MetaCache#metadata_cache{
                                                       rows = length(MetaList_1),
                                                       metadatas = MetaList_1,
                                                       created_at = leo_date:now()}));
                        _ ->
                            void
                    end;
                _ ->
                    void
            end;
        _ ->
            void
    end,
    Acc;
append_dir_1(_,DirBin, #?METADATA{clock = Clock,
                                  timestamp = Timestamp}, Acc) ->
    Current = {Clock, Timestamp},
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
handle_send({_,DestNode}, StackedInfo, CompressedObjs) ->
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
    end.


%% @doc Handle a fail process
%%
-spec(handle_fail(atom(), list({integer(), string()})) ->
             ok | {error, any()}).
handle_fail(_Unit, []) ->
    ok;
handle_fail(_Unit, [{_AddrId,_Dir, Key}|Rest]) ->
    ok = enqueue(#?METADATA{key = Key}),
    handle_fail(_Unit, Rest).


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


%% @doc
%% @private
-spec(get_directories_from_stacked_info(StackedInfo, Acc) ->
             [binary()] when StackedInfo::[{integer(), binary(), binary()}],
                             Acc::any()).
get_directories_from_stacked_info([], Acc) ->
    ordsets:to_list(Acc);
get_directories_from_stacked_info([{_AddrId,_Dir, Key}|Rest], Acc) ->
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
                           "key:~p, cause:~p", [Key, Cause]);
                _ ->
                    void
            end,
            ok;
        {error, Cause} ->
            ?error("enqueue/1",
                   "key:~p, cause:~p", [Key, Cause]),
            ok
    end.
