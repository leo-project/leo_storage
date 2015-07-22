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
%% @doc Metadata handler for the metadata-cluster
%% @reference https://github.com/leo-project/leo_storage/blob/master/src/leo_storage_handler_directory.erl
%% @end
%%======================================================================
-module(leo_storage_handler_directory).

-author('Yosuke Hara').

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([is_dir/1, ask_is_dir/1,
         add/1,
         delete/1,
         delete_objects_under_dir/1,
         delete_objects_under_dir/2,
         delete_objects_under_dir/3,
         prefix_search_and_remove_objects/1,
         find_uploaded_objects_by_key/1,
         get/1, get/2,
         find_by_parent_dir/4,
         ask_to_find_by_parent_dir/3
        ]).

-define(DEF_MAX_KEYS, 1000).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Find by index from the backenddb.
%%
-spec(is_dir(Dir) ->
             boolean() when Dir::binary()).
is_dir(Dir) ->
    case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
        {ok, #redundancies{nodes = Nodes}} ->
            is_dir_1(Nodes, Dir);
        Error ->
            Error
    end.

%% @private
is_dir_1([],_Dir) ->
    false;
is_dir_1([#redundant_node{available = false}|Rest], Dir) ->
    is_dir_1(Rest, Dir);
is_dir_1([#redundant_node{node = Node}|Rest], Dir) ->
    RPCKey = rpc:async_call(Node, ?MODULE, ask_is_dir, [Dir]),
    Reply = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
                {value, Ret} ->
                    Ret;
                {badrpc, Cause} ->
                    {error, Cause};
                timeout = Cause ->
                    {error, Cause}
            end,
    case Reply of
        {ok,_} ->
            erlang:element(2, Reply);
        {error, Reason} ->
            ?error("is_dir_1/2", "dir:~p, cause:~p", [Dir, Reason]),
            is_dir_1(Rest, Dir)
    end.


-spec(ask_is_dir(Dir) ->
             boolean() when Dir::binary()).
ask_is_dir(Dir) ->
    ParentDir = leo_directory_sync:get_directory_from_key(Dir),
    Dir_1 = << ParentDir/binary, "\t", Dir/binary >>,
    case leo_backend_db_api:get(?DIR_DB_ID, Dir_1) of
        {ok,_} ->
            {ok, true};
        not_found ->
            {ok, false};
        Error ->
            Error
    end.


%% @doc Add a directory
-spec(add(Dir) ->
             ok | {error, any()} when Dir::binary()).
add(Dir) ->
    leo_directory_sync:create_directories([Dir]).


%% @doc Remove a directory
-spec(delete(Dir) ->
             ok | {error, any()} when Dir::binary()).
delete(Dir) ->
    delete_objects_under_dir(#?OBJECT{key = Dir}).


%% Deletion object related constants
-define(BIN_SLASH, <<"/">>).
-define(BIN_NL,    <<"\n">>).

%% @doc Remove objects of the under directory
-spec(delete_objects_under_dir(Object) ->
             ok when Object::#?OBJECT{}).
delete_objects_under_dir(Object) ->
    Key = Object#?OBJECT.key,
    KSize = byte_size(Key),

    case catch binary:part(Key, (KSize - 1), 1) of
        {'EXIT',_} ->
            ok;
        ?BIN_SLASH = Bin ->
            %% for metadata-layer
            ok = leo_directory_sync:append(sync, #?METADATA{key = Key,
                                                            ksize = KSize,
                                                            dsize = -1,
                                                            clock = leo_date:clock(),
                                                            timestamp = leo_date:now(),
                                                            del = ?DEL_TRUE}),
            _ = leo_cache_api:delete(Key),

            %% for remote storage nodes
            Targets = [Bin, undefined],
            Ref = make_ref(),
            case leo_redundant_manager_api:get_members_by_status(?STATE_RUNNING) of
                {ok, RetL} ->
                    Nodes = [N||#member{node = N} <- RetL],
                    spawn(
                      fun() ->
                              {ok, Ref} = delete_objects_under_dir(
                                            Nodes, Ref, Targets)
                      end),
                    ok;
                _ ->
                    leo_storage_mq:publish(
                      ?QUEUE_TYPE_ASYNC_DELETE_DIR, [Key])
            end;
        _ ->
            ok
    end.

%% @doc Remove objects of the under directory for remote-nodes
-spec(delete_objects_under_dir(Ref, Keys) ->
             {ok, Ref} when Ref::reference(),
                            Keys::[binary()|undefined]).
delete_objects_under_dir(Ref, []) ->
    {ok, Ref};
delete_objects_under_dir(Ref, [undefined|Rest]) ->
    delete_objects_under_dir(Ref, Rest);
delete_objects_under_dir(Ref, [Key|Rest]) ->
    ok = prefix_search_and_remove_objects(Key),
    delete_objects_under_dir(Ref, Rest).

-spec(delete_objects_under_dir(Nodes, Ref, Keys) ->
             {ok, Ref} when Nodes::[atom()],
                            Ref::reference(),
                            Keys::[binary()|undefined]).
delete_objects_under_dir([], Ref,_Keys) ->
    {ok, Ref};
delete_objects_under_dir([Node|Rest], Ref, Keys) ->
    RPCKey = rpc:async_call(Node, ?MODULE,
                            delete_objects_under_dir, [Ref, Keys]),
    case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
        {value, {ok, Ref}} ->
            ok;
        _Other ->
            %% Enqueue a failed message into the mq
            ok = leo_storage_mq:publish(
                   ?QUEUE_TYPE_ASYNC_DELETE_DIR, Keys)
    end,
    delete_objects_under_dir(Rest, Ref, Keys).


%% @doc Retrieve object of deletion from object-storage by key
-spec(prefix_search_and_remove_objects(ParentDir) ->
             ok when ParentDir::undefined|binary()).
prefix_search_and_remove_objects(undefined) ->
    ok;
prefix_search_and_remove_objects(ParentDir) ->
    Fun = fun(Key, V, Acc) ->
                  Metadata = binary_to_term(V),
                  AddrId = Metadata#?METADATA.addr_id,
                  IsUnderDir = case binary:match(Key, [ParentDir]) of
                                   {Pos, _} when Pos == 0->
                                       true;
                                   _ ->
                                       false
                               end,
                  case IsUnderDir of
                      true when Metadata#?METADATA.del == ?DEL_FALSE ->
                          %% Remove the dir's info from the metdata-layer
                          KSize = byte_size(Key),
                          case catch binary:part(Key, (KSize - 1), 1) of
                              ?BIN_SLASH ->
                                  ok = leo_directory_sync:append(
                                         sync, #?METADATA{key = Key,
                                                          ksize = KSize,
                                                          dsize = -1,
                                                          clock = leo_date:clock(),
                                                          timestamp = leo_date:now(),
                                                          del = ?DEL_TRUE}),
                                  _ = leo_cache_api:delete(Key);
                              _ ->
                                  void
                          end,

                          %% Remove the metadata info from the object-storage
                          QId = ?QUEUE_TYPE_ASYNC_DELETE_OBJ,
                          case leo_storage_mq:publish(QId, AddrId, Key) of
                              ok ->
                                  void;
                              {error, Cause} ->
                                  ?warn("prefix_search_and_remove_objects/1",
                                        "qid:~p, addr-id:~p, key:~p, cause:~p",
                                        [QId, AddrId, Key, Cause])
                          end;
                      _ ->
                          void
                  end,
                  Acc
          end,
    leo_object_storage_api:fetch_by_key(ParentDir, Fun),
    ok.


%% @doc Find already uploaded objects by original-filename
-spec(find_uploaded_objects_by_key(OriginalKey) ->
             {ok, list()} | not_found when OriginalKey::binary()).
find_uploaded_objects_by_key(OriginalKey) ->
    Fun = fun(Key, V, Acc) ->
                  Metadata       = binary_to_term(V),

                  case (nomatch /= binary:match(Key, <<"\n">>)) of
                      true ->
                          Pos_1 = case binary:match(Key, [OriginalKey]) of
                                      nomatch   -> -1;
                                      {Pos, _} -> Pos
                                  end,
                          case (Pos_1 == 0) of
                              true ->
                                  [Metadata|Acc];
                              false ->
                                  Acc
                          end;
                      false ->
                          Acc
                  end
          end,
    leo_object_storage_api:fetch_by_key(OriginalKey, Fun).


%% @doc Retrieve a metadata of a dir
get(Dir) ->
    case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
        {ok, #redundancies{nodes = Nodes}} ->
            get_1(Nodes, Dir, []);
        Error ->
            Error
    end.

%% @doc Requested from a remote storage-node
get(Ref, Dir) ->
    ParentDir = leo_directory_sync:get_directory_from_key(Dir),
    Dir_1 = << ParentDir/binary, "\t", Dir/binary >>,
    case leo_backend_db_api:get(?DIR_DB_ID, Dir_1) of
        {ok, Metadata} ->
            {ok, {Ref, Metadata}};
        not_found = Cause ->
            {error, {Ref, Cause}};
        {error, Cause} ->
            {error, {Ref, Cause}}
    end.


%% @private
get_1([],_Dir,[]) ->
    not_found;
get_1([],_Dir, Errors) ->
    hd(Errors);
get_1([#redundant_node{available = false}|Rest], Dir, Errors) ->
    get_1(Rest, Dir, Errors);
get_1([#redundant_node{node = Node}|Rest], Dir, Errors) ->
    Ref = make_ref(),
    RPCKey = rpc:async_call(Node, ?MODULE, get, [Ref, Dir]),
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
              {value, {ok, {Ref, Metadata}}} ->
                  {ok, Metadata};
              {value, {error, {Ref,Cause}}} ->
                  {error, Cause};
              {badrpc, Cause} ->
                  {error, Cause};
              timeout = Cause ->
                  {error, Cause}
          end,
    case Ret of
        {ok,_} ->
            Ret;
        Error ->
            get_1(Rest, Dir, [Error|Errors])
    end.


%% @doc Find by index from the backenddb.
%%
-spec(find_by_parent_dir(Dir, Delimiter, Marker, MaxKeys) ->
             {ok, list()} |
             {error, any()} when Dir::binary(),
                                 Delimiter::binary()|null,
                                 Marker::binary()|null,
                                 MaxKeys::integer()).

find_by_parent_dir(Dir, _Delimiter, Marker, MaxKeys) when is_binary(Marker) == false ->
    find_by_parent_dir(Dir, _Delimiter, <<>>, MaxKeys);
find_by_parent_dir(Dir, _Delimiter, Marker, MaxKeys) when is_integer(MaxKeys) == false ->
    find_by_parent_dir(Dir, _Delimiter, Marker, ?DEF_MAX_KEYS);
find_by_parent_dir(Dir, _Delimiter, Marker, MaxKeys) ->
    %% Retrieve charge of nodes
    case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
        {ok, #redundancies{nodes = Nodes,
                           vnode_id_to = AddrId}} ->
            Dir_1 = get_managed_dir_name(Dir),
            find_by_parent_dir_1(Nodes, AddrId, Dir_1, Marker, MaxKeys);
        Error ->
            Error
    end.


%% @doc Retrieve metadatas under the directory
%% @TODO:
%%    * Need to handle marker and maxkeys to respond the correct list
%%    * Re-cache metadatas when the directory has many metadatas over maxkeys
-spec(ask_to_find_by_parent_dir(Dir, Marker, MaxKeys) ->
             {ok, list()} |
             {error, any()} when Dir::binary(),
                                 Marker::binary()|null,
                                 MaxKeys::integer()).
ask_to_find_by_parent_dir(Dir, <<>> = Marker, MaxKeys) ->
    Dir_1 = hd(leo_misc:binary_tokens(Dir, <<"\t">>)),
    Ret = case leo_cache_api:get(Dir_1) of
              {ok, MetaBin} ->
                  case binary_to_term(MetaBin) of
                      #metadata_cache{dir = Dir_1,
                                      rows = _Rows,
                                      metadatas = MetadataList} ->
                          {ok, MetadataList};
                      _ ->
                          not_found
                  end;
              _ ->
                  not_found
          end,
    case Ret of
        not_found ->
            case ask_to_find_by_parent_dir_1(Dir, Marker, MaxKeys) of
                {ok, MetadataList_1} = Reply ->
                    Rows = length(MetadataList_1),
                    leo_cache_api:put(Dir_1,
                                      term_to_binary(
                                        #metadata_cache{dir = Dir_1,
                                                        rows = Rows,
                                                        metadatas = MetadataList_1,
                                                        created_at = leo_date:now()
                                                       })),
                    Reply;
                Error ->
                    Error
            end;
        _ ->
            Ret
    end;
ask_to_find_by_parent_dir(Dir, Marker, MaxKeys) ->
    ask_to_find_by_parent_dir_1(Dir, Marker, MaxKeys).

%% @private
ask_to_find_by_parent_dir_1(Dir, Marker, MaxKeys) ->
    Fun = fun(_K, V, Acc) ->
                  case catch binary_to_term(V) of
                      #?METADATA{key = Key,
                                 del = ?DEL_FALSE} = Metadata ->
                          case Marker of
                              Key ->
                                  Acc;
                              <<>> ->
                                  [Metadata|Acc];
                              _ ->
                                  case lists:sort([Marker, Key]) of
                                      [Marker|_] ->
                                          [Metadata|Acc];
                                      _Other ->
                                          Acc
                                  end
                          end;
                      _ ->
                          Acc
                  end
          end,
    leo_backend_db_api:fetch(?DIR_DB_ID, Dir, Fun, MaxKeys).


%% @doc Retrieve metadatas under the directory
%% @private
find_by_parent_dir_1([],_,_,_,_) ->
    {error, ?ERROR_COULD_NOT_GET_META};
find_by_parent_dir_1([#redundant_node{available = false}|Rest], AddrId, Dir, Marker, MaxKeys) ->
    find_by_parent_dir_1(Rest, AddrId, Dir, Marker, MaxKeys);
find_by_parent_dir_1([#redundant_node{node = Node}|Rest], AddrId, Dir, Marker, MaxKeys) ->
    RPCKey = rpc:async_call(Node, ?MODULE,
                            ask_to_find_by_parent_dir, [Dir, Marker, MaxKeys]),
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
              {value, {ok, RetL}} ->
                  {ok, lists:sublist(lists:sort(RetL), MaxKeys)};
              {value, not_found} ->
                  {ok, []};
              {value, {error, Cause}} ->
                  {error, Cause};
              {badrpc, Cause} ->
                  {error, Cause};
              timeout = Cause ->
                  {error, Cause}
          end,
    case Ret of
        {ok,_} ->
            Ret;
        {error,_} ->
            find_by_parent_dir_1(Rest, AddrId, Dir, Marker, MaxKeys)
    end.


%% @doc Retrieve a managed dir name from the actual dir's name
%% @private
-spec(get_managed_dir_name(Dir) ->
             NewDir when Dir::binary(),
                         NewDir::binary()).
get_managed_dir_name(Dir) ->
    case binary:last(Dir) of
        %% "/"
        16#2f ->
            << Dir/binary, "\t" >>;
        %% "*"
        16#2a ->
            binary:part(Dir, {0, byte_size(Dir) - 1});
        _Other ->
            << Dir/binary, "/\t" >>
    end.
