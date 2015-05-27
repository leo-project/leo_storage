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
         create_directories/1,
         store/2,
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
create_directories([]) ->
    ok;
create_directories([Dir|Rest]) ->
    Metadata = #?METADATA{key = Dir,
                          ksize = byte_size(Dir),
                          dsize = -1,
                          clock = leo_date:clock(),
                          timestamp = leo_date:now()},

    case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
        {ok, #redundancies{id = AddrId}} ->
            append(Metadata#?METADATA{addr_id = AddrId});
        _ ->
            enqueue(Metadata)
    end,
    create_directories(Rest).


%% @doc Store received objects into the directory's db
%%
-spec(store(Ref, StackedBin) ->
             ok | {error, any()} when Ref::reference(),
                                      StackedBin::binary()).
store(Ref, StackedBin) ->
    {Ret, Dirs} = slice_and_store(StackedBin, ordsets:new()),
    ok = cache_dir_metadata(Dirs),
    {Ret, Ref}.


%% @doc Cache metadatas under the dir
%% @private
cache_dir_metadata([]) ->
    ok;
cache_dir_metadata([Dir|Rest]) ->
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
    {ok, ordsets:to_list(Acc)};
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
append_dir_1(_,DirBin,_,Acc) ->
    ordsets:add_element(DirBin, Acc).


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
            ok = create_directories(Dirs)
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
                    [Metadata];
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
    Sets = ordsets:new(),
    get_directories_2(Acc, Metadata, ordsets:add_element(Metadata, Sets));

get_directories_1([H|T], Metadata, IsDir, Acc, Sofar) ->
    Bin = << Sofar/binary, H/binary, "/" >>,
    get_directories_1(T, Metadata, IsDir, [Bin|Acc], Bin).

%% @private
get_directories_2([], Metadata, []) ->
    [Metadata];
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
get_directories_from_stacked_info([{_AddrId, Dir, Dir}|Rest], Acc) ->
    get_directories_from_stacked_info(Rest, Acc);
get_directories_from_stacked_info([{_AddrId, Dir,_Key}|Rest], Acc) ->
    get_directories_from_stacked_info(Rest, ordsets:add_element(Dir, Acc)).


%% @doc enqueue a message of a miss-replication into the queue
%% @private
enqueue(#?METADATA{key = Key} = Metadata) ->
    case leo_redundant_manager_api:get_redundancies_by_key(Key) of
        {ok, #redundancies{id = AddrId}} ->
            leo_directory_mq:publish(Metadata#?METADATA{addr_id = AddrId});
        {error, Cause} ->
            ?error("enqueue/1",
                   "key:~p, cause:~p", [Key, Cause]),
            ok
    end.
