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
%% LeoFS Storage - Directory Handler
%% @doc
%% @end
%%======================================================================
-module(leo_storage_handler_directory).

-author('Yosuke Hara').

-include("leo_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([find_by_parent_dir/4]).

-define(DEF_DELIMITER, "/").
-define(DEF_MAX_KEYS,  1000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Find by index from the backenddb.
%%
-spec(find_by_parent_dir(string(), string()|null, string()|null, integer()) ->
             {ok, list()} | {error, any()}).
find_by_parent_dir(ParentDir, Delimiter, Marker, MaxKeys) ->
    NewDlmtr   = case is_list(Delimiter) of
                     true when length(Delimiter) > 0 ->
                         Delimiter;
                     true  -> ?DEF_DELIMITER;
                     false -> ?DEF_DELIMITER
                 end,
    NewMaxKeys = case is_integer(MaxKeys) of
                     true  -> MaxKeys;
                     false -> ?DEF_MAX_KEYS
                 end,
    NewMarker  = case is_list(Marker) of
                     true  -> Marker;
                     false -> []
                 end,

    {ok, Members} = leo_redundant_manager_api:get_members(),
    Nodes = lists:foldl(fun(#member{node  = Node,
                                    state = ?STATE_RUNNING}, Acc) ->
                                [Node|Acc];
                           (_, Acc) ->
                                Acc
                        end, [], Members),

    {ResL0, _BadNodes} = rpc:multicall(Nodes, leo_storage_handler_object, prefix_search,
                                       [ParentDir, NewDlmtr, NewMarker, NewMaxKeys]),

    case lists:foldl(fun({ok, List}, Acc) ->
                             Acc ++ List
                     end, [], ResL0) of
        [] ->
            {ok, []};
        List ->
            {ok, lists:sublist(ordsets:from_list(lists:flatten(List)), NewMaxKeys)}
    end.

