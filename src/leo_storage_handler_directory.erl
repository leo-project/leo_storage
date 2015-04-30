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
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([find_by_parent_dir/4
        ]).

-define(DEF_MAX_KEYS, 1000).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
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
            Dir_1 = case binary:last(Dir) of
                        %% "/"
                        16#2f ->
                            << Dir/binary, "\t" >>;
                        %% "*"
                        16#2a ->
                            binary:part(Dir, {0, byte_size(Dir) - 1});
                        _Other ->
                            << Dir/binary, "/\t" >>
                    end,
            find_by_parent_dir_1(Nodes, AddrId, Dir_1, Marker, MaxKeys);
        Error ->
            Error
    end.

%% @doc Retrieve metadatas under the directory
%% @private
find_by_parent_dir_1([],_,_,_,_) ->
    {error, ?ERROR_COULD_NOT_GET_META};
find_by_parent_dir_1([#redundant_node{available = false}|Rest], AddrId, Dir, Marker, MaxKeys) ->
    find_by_parent_dir_1(Rest, AddrId, Dir, Marker, MaxKeys);
find_by_parent_dir_1([#redundant_node{node = Node}|Rest], AddrId, Dir, Marker, MaxKeys) ->
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

    RPCKey = rpc:async_call(Node, leo_backend_db_api, fetch, [?DIR_DB_ID, Dir, Fun, MaxKeys]),
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
              {value, {ok, RetL}} ->
                  {ok, lists:sort(RetL)};
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
