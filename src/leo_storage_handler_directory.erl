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

-behaviour(leo_ordning_reda_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_ordning_reda/include/leo_ordning_reda.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/1, stop/1,
         append/3, store/2]).
-export([find_by_parent_dir/4,
         delete_objects_in_parent_dir/1
        ]).
-export([handle_send/3,
         handle_fail/2]).

-define(METADATA_CONTAINER_BUF_SIZE, 1024 * 256). %% 256kb
-define(METADATA_CONTAINER_TIMEOUT,  timer:seconds(1)). %% 1sec(1000msec)
-define(DEF_MAX_KEYS, 1000).


%%--------------------------------------------------------------------
%% API-1
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


%% @doc Append a object into the ordning&reda
%%
-spec(append(DestNodes, Dir, Metadata) ->
             ok |
             {error, any()} when DestNodes::[atom()],
                                 Dir::binary(),
                                 Metadata::#?METADATA{}).
append(DestNode, Dir, #?METADATA{addr_id = AddrId,
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
                    append(DestNode, Dir, Metadata);
                Error ->
                    ?debugVal(Error),
                    Error
            end
    end.


%% @doc Store received objects into the directory's db
%%
-spec(store(Ref, StackedBin) ->
             ok | {error, any()} when Ref::reference(),
                                      StackedBin::binary()).
store(Ref, StackedBin) ->
    Ret = slice_and_store(StackedBin),
    {Ret, Ref}.

%% @private
slice_and_store(<<>>) ->
    ok;
slice_and_store(StackedBin) ->
    case slice(StackedBin) of
        {ok, {DirBin, #?METADATA{key = Key} = Metadata, StackedBin_1}} ->
            ?debugVal({DirBin, Metadata#?METADATA.key, byte_size(StackedBin_1)}),

            KeyBin = term_to_binary({DirBin, Key}),
            ValBin = term_to_binary(Metadata),
            case leo_backend_db_api:put(?DIRECTORY_DATA_ID, KeyBin, ValBin) of
                ok ->
                    ok;
                {error, Cause} ->
                    %% @TODO: enqueue a message
                    ?error("slice/1","cause:~p",[Cause]),
                    ok
            end,
            slice_and_store(StackedBin_1);
        _ ->
            {error, invalid_format}
    end.

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
            ?error("slice/1","cause:~p",[Cause]),
            {error, invalid_format}
    end.


%%--------------------------------------------------------------------
%% API-2
%%--------------------------------------------------------------------
%% @doc Find by index from the backenddb.
%%
-spec(find_by_parent_dir(ParentDir, Delimiter, Marker, MaxKeys) ->
             {ok, list()} |
             {error, any()} when ParentDir::binary(),
                                 Delimiter::binary()|null,
                                 Marker::binary()|null,
                                 MaxKeys::integer()).
find_by_parent_dir(ParentDir, _Delimiter, Marker, MaxKeys) ->
    NewMaxKeys = case is_integer(MaxKeys) of
                     true  -> MaxKeys;
                     false -> ?DEF_MAX_KEYS
                 end,
    NewMarker  = case is_binary(Marker) of
                     true  -> Marker;
                     false -> <<>>
                 end,

    {ok, Members} = leo_redundant_manager_api:get_members(),
    Nodes = lists:foldl(fun(#member{node  = Node,
                                    state = ?STATE_RUNNING}, Acc) ->
                                [Node|Acc];
                           (_, Acc) ->
                                Acc
                        end, [], Members),

    {ResL0, _BadNodes} = rpc:multicall(Nodes, leo_storage_handler_object, prefix_search,
                                       [ParentDir, NewMarker, NewMaxKeys], ?DEF_REQ_TIMEOUT),

    case lists:foldl(
           fun({ok, List}, Acc0) ->
                   lists:foldl(
                     fun(#?METADATA{key = Key} = Meta0, Acc1) ->
                             case lists:keyfind(Key, 2, Acc1) of
                                 false ->
                                     [Meta0|Acc1];
                                 #?METADATA{clock = Clock} when Meta0#?METADATA.clock > Clock ->
                                     Acc2 = lists:keydelete(Key, 2, Acc1),
                                     [Meta0|Acc2];
                                 _ ->
                                     Acc1
                             end
                     end, Acc0, List);
              (_, Acc0) ->
                   Acc0
           end, [], ResL0) of
        [] ->
            {ok, []};
        List ->
            {ok, lists:sublist(ordsets:from_list(lists:flatten(List)), NewMaxKeys)}
    end.


%% @doc Remove objects in the parent directory - request from Gateway
%%
-spec(delete_objects_in_parent_dir(ParentDir) ->
             {ok, [_]} | not_found when ParentDir::binary()).
delete_objects_in_parent_dir(ParentDir) ->
    leo_storage_handler_object:prefix_search_and_remove_objects(ParentDir).


%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------
%% @doc Handle send object to a remote-node.
%%
handle_send({_,DestNode},_StackedInfo, CompressedObjs) ->
    ?debugVal({DestNode, byte_size(CompressedObjs)}),

    Ref = make_ref(),
    RPCKey = rpc:async_call(DestNode, ?MODULE, store, [Ref, CompressedObjs]),
    case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
        {value, {ok, Ref}} ->
            ?debugVal({ok, DestNode, Ref}),
            ok;
        _Other ->
            %% @TODO:enqueu a fail message into the mq
            _Other
    end.


%% @doc Handle a fail process
%%
-spec(handle_fail(atom(), list({integer(), string()})) ->
             ok | {error, any()}).
handle_fail(_,_) ->
    ok.


%%--------------------------------------------------------------------
%% INNER FUNCTION
%%--------------------------------------------------------------------
