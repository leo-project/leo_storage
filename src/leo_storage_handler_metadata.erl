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
%% @reference https://github.com/leo-project/leo_storage/blob/master/src/leo_storage_handler_metadata.erl
%% @end
%%======================================================================
-module(leo_storage_handler_metadata).
-author('Yosuke Hara').

-behaviour(leo_ordning_reda_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_ordning_reda/include/leo_ordning_reda.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/1, stop/1, stack/3]).
-export([handle_send/3,
         handle_fail/2]).

-define(METADATA_CONTAINER_BUF_SIZE, 1024 * 256). %% 256kb
-define(METADATA_CONTAINER_TIMEOUT,  timer:seconds(1)). %% 1sec(1000msec)


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Add a container into the supervisor
%%
-spec(start_link(DestNode) ->
             ok | {error, any()} when DestNode::atom()).
start_link(DestNode) ->
    leo_ordning_reda_api:add_container({metadata, DestNode}, [{?PROP_ORDRED_MOD,      ?MODULE},
                                                              {?PROP_ORDRED_BUF_SIZE, ?METADATA_CONTAINER_BUF_SIZE},
                                                              {?PROP_ORDRED_IS_COMP,  false},
                                                              {?PROP_ORDRED_TIMEOUT,  ?METADATA_CONTAINER_TIMEOUT}]).

%% @doc Remove a container from the supervisor
%%
-spec(stop(DestNode) ->
             ok | {error, any()} when DestNode::atom()).
stop(DestNode) ->
    leo_ordning_reda_api:remove_container({metadata, DestNode}).


%% @doc Stack a object into the ordning&reda
%%
-spec(stack(DestNodes, Dir, Metadata) ->
             ok |
             {error, any()} when DestNodes::[atom()],
                                 Dir::binary(),
                                 Metadata::#?METADATA{}).
stack(DestNode, Dir, #?METADATA{addr_id = AddrId,
                                key = Key} = Metadata) ->
    case leo_ordning_reda_api:has_container({metadata, DestNode}) of
        true ->
            DirSize  = byte_size(Dir),
            MetaBin  = term_to_binary(Metadata),
            MetaSize = byte_size(MetaBin),
            Data = << DirSize:?DEF_BIN_DIR_SIZE,
                      Dir/binary,
                      MetaSize:?DEF_BIN_META_SIZE,
                      MetaBin/binary,
                      ?DEF_BIN_PADDING/binary >>,
            leo_ordning_reda_api:stack({metadata, DestNode}, {AddrId, Key}, Data);
        false ->
            case start_link(DestNode) of
                ok ->
                    stack(DestNode, Dir, Metadata);
                Error ->
                    ?debugVal(Error),
                    Error
            end
    end.


%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------
%% @doc Handle send object to a remote-node.
%%
handle_send(Unit,_StackedInfo, CompressedObjs) ->
    %% @TODO
    ?debugVal({Unit, byte_size(CompressedObjs)}),
    ok.


%% @doc Handle a fail process
%%
-spec(handle_fail(atom(), list({integer(), string()})) ->
             ok | {error, any()}).
handle_fail(_,_) ->
    ok.


%%--------------------------------------------------------------------
%% INNER FUNCTION
%%--------------------------------------------------------------------
