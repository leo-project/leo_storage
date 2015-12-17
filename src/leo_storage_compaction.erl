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
has_charge_of_node(#?METADATA{addr_id = _AddrId,
                              key = Key,
                              redundancy_method = RedMethod,
                              ec_params = _ECParams,
                              cindex = CIndex}, NumOfReplicas) ->
    case RedMethod of
        %% for chunk-object
        ?RED_ERASURE_CODE when CIndex > 0 ->
            %% @TODO:
            true;
        _ ->
            leo_redundant_manager_api:has_charge_of_node(Key, NumOfReplicas)
    end.


%% @doc Update a metadata (during the data-compaction processing)
-spec(update_metadata(Method::put|delete, Key::binary(), Metadata::#?METADATA{}) ->
                 ok | {error, any()}).
update_metadata(_Method,_Key, Metadata) ->
    leo_directory_sync:append(Metadata, ?DIR_ASYNC).
