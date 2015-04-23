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
-export([get_directories/1]).


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
            leo_backend_db_sup:start_child(
              SupRef, ?DIR_DB_ID, DirDBProcs, DirDBName, DirDBPath)
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
append(#?METADATA{key = Key} = Metadata) ->
    append(#?METADATA{key = Key} = Metadata, false).

-spec(append(Metadata, IsDir) ->
             ok when Metadata::#?METADATA{},
                     IsDir::boolean()).
append(#?METADATA{key = Key} = Metadata, IsDir) ->
    %% Retrieve a directory from the key
    Dir = case IsDir of
              true ->
                  Key;
              false ->
                  get_dirirecory_from_key(Key)
          end,

    %% Retrieve destination nodes
    case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
        {ok, #redundancies{nodes = RedundantNodes,
                           vnode_id_to = AddrId}} ->
            Metadata_1 = Metadata#?METADATA{addr_id = AddrId},

            %% Store/Remove the metadata into the metadata-cluster
            case [Node || #redundant_node{available = true,
                                          node = Node} <- RedundantNodes] of
                [] ->
                    enqueue(Metadata_1);
                ActiveNodes ->
                    lists:foreach(fun(N) ->
                                          append(N, Dir, Metadata_1)
                                  end, ActiveNodes),
                    ok
            end;
        {error,_Cause} ->
            enqueue(Metadata)
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
            DirSize  = byte_size(Dir),
            MetaBin  = term_to_binary(Metadata),
            MetaSize = byte_size(MetaBin),
            Data = << DirSize:?DEF_BIN_DIR_SIZE,
                      Dir/binary,
                      MetaSize:?DEF_BIN_META_SIZE,
                      MetaBin/binary,
                      ?DEF_BIN_PADDING/binary >>,
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


%% @doc Create directories
-spec(create_directories(Dir) ->
             ok when Dir::[binary()]).
create_directories([]) ->
    ok;
create_directories([Dir|Rest]) ->
    Metadata = #?METADATA{key = Dir,
                          ksize = byte_size(Dir),
                          clock = leo_date:clock(),
                          timestamp = leo_date:now()},

    case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
        {ok, #redundancies{vnode_id_to = AddrId}} ->
            append(Metadata#?METADATA{addr_id = AddrId}, true);
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
    Ret = slice_and_store(StackedBin),
    {Ret, Ref}.

%% @private
slice_and_store(<<>>) ->
    ok;
slice_and_store(StackedBin) ->
    case slice(StackedBin) of
        {ok, {DirBin, #?METADATA{addr_id = AddrId,
                                 key = Key,
                                 clock = Clock,
                                 del = Del} = Metadata, StackedBin_1}} ->
            DirSize = byte_size(DirBin),
            KeySize = byte_size(Key),
            KeyBin = << AddrId:128, DirSize:16, DirBin/binary, KeySize:16, Key/binary >>,
            ValBin = term_to_binary(Metadata),
            CanPutVal =
                case leo_backend_db_api:get(?DIR_DB_ID, KeyBin) of
                    {ok, ValBin_1} ->
                        case catch binary_to_term(ValBin_1) of
                            #?METADATA{clock = Clock_1} when Clock > Clock_1 ->
                                true;
                            _ ->
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

            case CanPutVal of
                true when Del == ?DEL_FALSE ->
                    case leo_backend_db_api:put(?DIR_DB_ID, KeyBin, ValBin) of
                        ok ->
                            ?debugVal({KeyBin, AddrId, DirBin, Key}),
                            ok;
                        {error, Cause} ->
                            error_logger:info_msg("~p,~p,~p,~p~n",
                                                  [{module, ?MODULE_STRING},
                                                   {function, "slice_and_store/1"},
                                                   {line, ?LINE}, {body, Cause}]),
                            enqueue(Metadata)
                    end;
                true when Del == ?DEL_TRUE ->
                    case leo_backend_db_api:delete(?DIR_DB_ID, KeyBin) of
                        ok ->
                            ok;
                        {error, Cause} ->
                            error_logger:info_msg("~p,~p,~p,~p~n",
                                                  [{module, ?MODULE_STRING},
                                                   {function, "slice_and_store/1"},
                                                   {line, ?LINE}, {body, Cause}]),
                            enqueue(Metadata)
                    end;
                false ->
                    void
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
        ?debugVal(Metadata),
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
recover(#?METADATA{key = Key} = Metadata) ->
    IsDir = (binary:last(Key) == 16#2f),
    append(Metadata, IsDir).


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
    case get_directories_from_stackinf(StackedInfo, sets:new()) of
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
handle_fail(_Unit, [{AddrId, Key}|Rest]) ->
    ok = enqueue(#?METADATA{addr_id = AddrId,
                            key = Key}),
    handle_fail(_Unit, Rest).


%%--------------------------------------------------------------------
%% INNER FUNCTION
%%--------------------------------------------------------------------
%% @doc Retrieve a directory from the key
%% @private
-spec(get_dirirecory_from_key(Key) ->
             binary() when Key::binary()).
get_dirirecory_from_key(Key) ->
    BinSlash = <<"/">>,
    case binary:matches(Key, [BinSlash],[]) of
        [] ->
            <<>>;
        RetL ->
            {Len,_} = lists:last(RetL),
            Bin = binary:part(Key, 0, Len),
            << Bin/binary, BinSlash/binary >>
    end.


%% @doc Retrieve a directories from the metadata
%% @private
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
                            get_directories_1(lists:sublist(Tokens, length(Tokens)-1),
                                              Metadata, false, [], <<>>)
                    end
            end
    end.

%% @private
get_directories_1([], Metadata, true, Acc,_Sofar) ->
    get_directories_2(Acc, Metadata, []);

get_directories_1([], Metadata, false, Acc, Sofar) ->
    get_directories_2([Sofar|Acc], Metadata, []);

get_directories_1([H|T], Metadata, IsDir, Acc, Sofar) ->
    Bin = << Sofar/binary, H/binary, "/" >>,
    get_directories_1(T, Metadata, IsDir, [Bin|Acc], Bin).

%% @private
get_directories_2([], Metadata, Acc) ->
    Len = length(Acc),
    lists:append([ lists:sublist(Acc, Len-1), [Metadata] ]);
get_directories_2([H|T], #?METADATA{clock = Clock,
                                    timestamp = Timestamp} = Metadata, Acc) ->
    get_directories_2(T, Metadata, [#?METADATA{key = H,
                                               ksize = byte_size(H),
                                               clock = Clock,
                                               timestamp = Timestamp}|Acc]).


%% @doc
-spec(get_directories_from_stackinf(StackedInfo, Acc) ->
             [binary()] when StackedInfo::[{integer(), binary(), binary()}],
                             Acc::any()).
get_directories_from_stackinf([], Acc) ->
    sets:to_list(Acc);
get_directories_from_stackinf([{_AddrId, Dir, Dir}|Rest], Acc) ->
    get_directories_from_stackinf(Rest, Acc);
get_directories_from_stackinf([{_AddrId, Dir,_Key}|Rest], Acc) ->
    get_directories_from_stackinf(Rest, sets:add_element(Dir, Acc)).


%% @doc enqueue a message of a miss-replication into the queue
%% @private
enqueue(Metadata) ->
    leo_directory_mq:publish(Metadata).
