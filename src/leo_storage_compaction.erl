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
%% Leo Storage  - data-compaction behaiviour
%% @doc
%% @end
%%======================================================================
-module(leo_storage_compaction).

-behaviour(leo_compact_callback).

-include("leo_storage.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% callback
-export([has_charge_of_node/2,
         update_metadata/3
        ]).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Check the owner of the object by key
-spec(has_charge_of_node(Metadata::#?METADATA{}, NumOfReplicas::pos_integer()) ->
             boolean()).
has_charge_of_node(#?METADATA{key = Key,
                              redundancy_method = RedMethod,
                              cindex = CIndex,
                              ec_params = ECParams}, NumOfReplicas) ->
    case RedMethod of
        %% for an encoded object
        ?RED_ERASURE_CODE when CIndex > 0 ->
            ParentKey = begin
                            {Pos,_} = lists:last(binary:matches(Key, [<<"\n">>], [])),
                            binary:part(Key, 0, Pos)
                        end,
            case ECParams of
                {ECParams_K, ECParams_M} ->
                    TotalFragments = ECParams_K + ECParams_M,

                    case leo_redundant_manager_api:collect_redundancies_by_key(
                           ParentKey, TotalFragments, ECParams_M) of
                        {ok, {_Option, RedNodeL}} when erlang:length(RedNodeL) == TotalFragments ->
                            case lists:nth(CIndex, RedNodeL) of
                                #redundant_node{node = Node} when Node == erlang:node() ->
                                    true;
                                _ ->
                                    false
                            end;
                        _ ->
                            true
                    end;
                _ ->
                    true
            end;
        %% for a replicated object
        %% OR a parent object of chunks
        _ ->
            leo_redundant_manager_api:has_charge_of_node(Key, NumOfReplicas)
    end.


%% @doc Update a metadata (during the data-compaction processing)
-spec(update_metadata(Method::put|delete, Key::binary(), Metadata::#?METADATA{}) ->
             ok | {error, any()}).
update_metadata(_Method,_Key, Metadata) ->
    leo_directory_sync:append(Metadata, ?DIR_ASYNC).
