%%======================================================================
%%
%% LeoFS Storage
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
%% LeoFS Storage - Object Handler
%% @doc
%% @end
%%======================================================================
-module(leo_storage_handler_object).

-author('Yosuke Hara').

-include("leo_storage.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_ordning_reda/include/leo_ordning_reda.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([get/1, get/2, get/3, get/4, get/5,
         put/1, put/2, put/4,
         delete/1, delete/2,
         delete_objects_under_dir/1,
         delete_objects_under_dir/2,
         delete_objects_under_dir/3,
         head/2, head/3,
         head_with_calc_md5/3,
         replicate/1, replicate/3,
         prefix_search/3, prefix_search_and_remove_objects/1,
         find_uploaded_objects_by_key/1
        ]).

-define(REP_LOCAL,  'local').
-define(REP_REMOTE, 'remote').
-type(replication() :: ?REP_LOCAL | ?REP_REMOTE).

-define(DEF_DELIMITER, <<"/">>).

-ifdef(EUNIT).
-define(output_warn(Fun, _Key, _MSG), ok).
-else.
-define(output_warn(Fun, _Key, _MSG),
        ?warn(Fun, "key:~p, cause:~p", [_Key, _MSG])).
-endif.


%%--------------------------------------------------------------------
%% API - GET
%%--------------------------------------------------------------------
%% @doc get object (from storage-node#1).
%%
-spec(get({reference(), binary()}) ->
             {ok, reference(), binary(), binary(), binary()} |
             {error, reference(), any()}).
get({Ref, Key}) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_GET),
    case leo_redundant_manager_api:get_redundancies_by_key(get, Key) of
        {ok, #redundancies{id = AddrId}} ->
            case get_fun(AddrId, Key) of
                {ok, Metadata, #?OBJECT{data = Bin}} ->
                    {ok, Ref, Metadata, Bin};
                {error, Cause} ->
                    {error, Ref, Cause}
            end;
        _ ->
            {error, Ref, ?ERROR_COULD_NOT_GET_REDUNDANCY}
    end.

%% @doc get object (from storage-node#2).
%%
-spec(get(#read_parameter{}, [#redundant_node{}]) ->
             {ok, #?METADATA{}, binary()} |
             {ok, match} |
             {error, any()}).
get(#read_parameter{addr_id = AddrId} = ReadParameter, []) ->
    case leo_redundant_manager_api:get_redundancies_by_addr_id(get, AddrId) of
        {ok, #redundancies{nodes = Redundancies,
                           r = ReadQuorum}} ->
            get(ReadParameter#read_parameter{quorum = ReadQuorum},
                Redundancies);
        _Error ->
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCY}
    end;
get(ReadParameter, Redundancies) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_GET),
    read_and_repair(ReadParameter, Redundancies).

%% @doc Retrieve an object which is requested from gateway.
%%
-spec(get(integer(), binary(), integer()) ->
             {ok, #?METADATA{}, binary()} |
             {error, any()}).
get(AddrId, Key, ReqId) ->
    get(#read_parameter{ref = make_ref(),
                        addr_id   = AddrId,
                        key       = Key,
                        req_id    = ReqId}, []).

%% @doc Retrieve an object which is requested from gateway w/etag.
%%
-spec(get(integer(), binary(), integer(), integer()) ->
             {ok, #?METADATA{}, binary()} |
             {ok, match} |
             {error, any()}).
get(AddrId, Key, ETag, ReqId) ->
    get(#read_parameter{ref = make_ref(),
                        addr_id   = AddrId,
                        key       = Key,
                        etag      = ETag,
                        req_id    = ReqId}, []).

%% @doc Retrieve a part of an object.
%%
-spec(get(integer(), binary(), integer(), integer(), integer()) ->
             {ok, #?METADATA{}, binary()} |
             {ok, match} |
             {error, any()}).
get(AddrId, Key, StartPos, EndPos, ReqId) ->
    get(#read_parameter{ref = make_ref(),
                        addr_id   = AddrId,
                        key       = Key,
                        start_pos = StartPos,
                        end_pos   = EndPos,
                        req_id    = ReqId}, []).


%% @doc read data (common).
%% @private
-spec(get_fun(integer(), binary()) ->
             {ok, #?METADATA{}, #?OBJECT{}} |
             {error, any()}).
get_fun(AddrId, Key) ->
    get_fun(AddrId, Key, 0, 0).

-spec(get_fun(integer(), binary(), integer(), integer()) ->
             {ok, #?METADATA{}, #?OBJECT{}} |
             {error, any()}).
get_fun(AddrId, Key, StartPos, EndPos) ->
    case leo_object_storage_api:get({AddrId, Key}, StartPos, EndPos) of
        {ok, Metadata, Object} ->
            {ok, Metadata, Object};
        not_found = Cause ->
            {error, Cause};
        {error, Cause} ->
            {error, Cause}
    end.


%%--------------------------------------------------------------------
%% API - PUT
%%--------------------------------------------------------------------
%% @doc Insert an object (request from gateway).
%%
-spec(put({#?OBJECT{}, reference()}) ->
             {ok, reference(), tuple()} |
             {error, reference(), any()}).
put({Object, Ref}) ->
    AddrId = Object#?OBJECT.addr_id,
    Key    = Object#?OBJECT.key,

    case Object#?OBJECT.del of
        ?DEL_TRUE->
            case leo_object_storage_api:head({AddrId, Key}) of
                {ok, MetaBin} ->
                    case binary_to_term(MetaBin) of
                        #?METADATA{cnumber = 0} ->
                            put_fun(Ref, AddrId, Key, Object);
                        #?METADATA{cnumber = CNumber} ->
                            case delete_chunked_objects(CNumber, Key) of
                                ok ->
                                    put_fun(Ref, AddrId, Key, Object);
                                {error, Cause} ->
                                    {error, Ref, Cause}
                            end;
                        _ ->
                            {error, Ref, 'invalid_data'}
                    end;
                {error, Cause} ->
                    {error, Ref, Cause};
                not_found = Cause ->
                    {error, Ref, Cause}
            end;
        %% FOR PUT
        ?DEL_FALSE ->
            put_fun(Ref, AddrId, Key, Object)
    end.


%% @doc Insert an object (request from gateway).
%%
-spec(put(#?OBJECT{}, integer()) ->
             ok | {error, any()}).
put(Object, ReqId) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_PUT),
    replicate_fun(?REP_LOCAL, ?CMD_PUT, Object#?OBJECT.addr_id,
                  Object#?OBJECT{method = ?CMD_PUT,
                                 clock  = leo_date:clock(),
                                 req_id = ReqId}).


%% @doc Insert an  object (request from remote-storage-nodes/replicator).
%%
-spec(put(reference(), pid(), #?OBJECT{}, integer()) ->
             {ok, atom()} | {error, any()}).
put(Ref, From, Object, ReqId) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_PUT),
    case replicate_fun(?REP_REMOTE, ?CMD_PUT, Object) of
        {ok, ETag} ->
            erlang:send(From, {Ref, {ok, ETag}});

        %% not found an object (during rebalance and delete-operation)
        {error, not_found} when ReqId == 0 ->
            erlang:send(From, {Ref, {ok, 0}});
        {error, Cause} ->
            erlang:send(From, {Ref, {error, {node(), Cause}}})
    end.


%% Input an object into the object-storage
%% @private
-spec(put_fun(reference(), integer(), binary(), #?OBJECT{}) ->
             {ok, reference(), tuple()} | {error, reference(), any()}).
put_fun(Ref, AddrId, Key, Object) ->
    case leo_object_storage_api:put({AddrId, Key}, Object) of
        {ok, ETag} ->
            {ok, Ref, {etag, ETag}};
        {error, Cause} ->
            {error, Ref, Cause}
    end.


%% Remove chunked objects from the object-storage
%% @private
-spec(delete_chunked_objects(integer(), binary()) ->
             ok | {error, any()}).
delete_chunked_objects(0,_) ->
    ok;
delete_chunked_objects(CIndex, ParentKey) ->
    IndexBin = list_to_binary(integer_to_list(CIndex)),
    Key    = << ParentKey/binary, "\n", IndexBin/binary >>,
    AddrId = leo_redundant_manager_chash:vnode_id(Key),

    case delete(#?OBJECT{addr_id  = AddrId,
                         key      = Key,
                         cindex   = CIndex,
                         clock    = leo_date:clock()}, 0) of
        ok ->
            delete_chunked_objects(CIndex - 1, ParentKey);
        {error, Cause} ->
            {error, Cause}
    end.


%%--------------------------------------------------------------------
%% API - DELETE
%%--------------------------------------------------------------------
%% @doc Remove an object (request from storage)
%%
-spec(delete({#?OBJECT{}, reference()}) ->
             {ok, reference()} | {error, reference(), any()}).
delete({Object, Ref}) ->
    AddrId = Object#?OBJECT.addr_id,
    Key    = Object#?OBJECT.key,

    case leo_object_storage_api:head({AddrId, Key}) of
        not_found = Cause ->
            {error, Ref, Cause};
        {ok, MetaBin} ->
            case catch binary_to_term(MetaBin) of
                {'EXIT', Cause} ->
                    {error, Cause};
                #?METADATA{del = ?DEL_TRUE} ->
                    {ok, Ref};
                #?METADATA{del = ?DEL_FALSE} ->
                    case leo_object_storage_api:delete({AddrId, Key}, Object) of
                        ok ->
                            {ok, Ref};
                        {error, Why} ->
                            {error, Ref, Why}
                    end
            end;
        {error, _Cause} ->
            {error, Ref, ?ERROR_COULD_NOT_GET_META}
    end.


%% @doc Remova an object (request from gateway)
%%
-spec(delete(#?OBJECT{}, integer()|reference()) ->
             ok | {error, any()}).
delete(Object, ReqId) when is_integer(ReqId) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_DEL),
    case replicate_fun(?REP_LOCAL, ?CMD_DELETE,
                       Object#?OBJECT.addr_id,
                       Object#?OBJECT{method   = ?CMD_DELETE,
                                      data     = <<>>,
                                      dsize    = 0,
                                      clock    = leo_date:clock(),
                                      req_id   = ReqId,
                                      del      = ?DEL_TRUE}) of
        ok ->
            ok = delete_objects_under_dir(Object),
            ok;
        {error, not_found = Cause} ->
            ok = delete_objects_under_dir(Object),
            {error, Cause};
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Remove objects of the under directory
%%
-spec(delete_objects_under_dir(#?OBJECT{}) ->
             ok).
delete_objects_under_dir(Object) ->
    Key = Object#?OBJECT.key,
    KSize = byte_size(Key),

    case catch binary:part(Key, (KSize - 1), 1) of
        {'EXIT',_} ->
            void;
         Bin ->
            Targets = case Bin == <<"/">> of
                          true ->
                              [ Bin,
                                undefined];
                          false when Bin == <<"\n">> ->
                              [ undefined,
                                << Key/binary, "\n" >> ];
                          false ->
                              [ << Key/binary, "/"  >>,
                                << Key/binary, "\n" >> ]
                      end,

            %% for remote storage nodes
            Ref = make_ref(),
            case leo_redundant_manager_api:get_members_by_status(?STATE_RUNNING) of
                {ok, RetL} ->
                    Nodes = [N||#member{node = N} <- RetL],
                    {ok, Ref} = delete_objects_under_dir(Nodes, Ref, Targets),
                    ok;
                _ ->
                    void
            end,
            %% for local object storage
            delete_objects_under_dir(Ref, Targets)
    end,
    ok.


%% @doc Remove objects of the under directory for remote-nodes
%%
-spec(delete_objects_under_dir(Ref, Keys) ->
             {ok, Ref} when Ref::reference(),
                            Keys::[binary()|undefined]).
delete_objects_under_dir(Ref, []) ->
    {ok, Ref};
delete_objects_under_dir(Ref, [undefined|Rest]) ->
    delete_objects_under_dir(Ref, Rest);
delete_objects_under_dir(Ref, [Key|Rest]) ->
    _ = prefix_search_and_remove_objects(Key),
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
            %% enqueu a fail message into the mq
            ok = leo_storage_mq:publish(?QUEUE_TYPE_DEL_DIR, Node, Keys)
    end,
    delete_objects_under_dir(Rest, Ref, Keys).


%%--------------------------------------------------------------------
%% API - HEAD
%%--------------------------------------------------------------------
%% @doc retrieve a meta-data from mata-data-server (file).
%%
-spec(head(integer(), binary()) ->
             {ok, #?METADATA{}} | {error, any}).
head(AddrId, Key) ->
    %% Do retry when being invoked as usual method
    head(AddrId, Key, true).

-spec(head(integer(), binary(), boolean()) ->
             {ok, #?METADATA{}} | {error, any}).
head(AddrId, Key, false) ->
    %% No retry when being invoked from recover/rebalance
    case leo_object_storage_api:head({AddrId, Key}) of
        {ok, MetaBin} ->
            {ok, binary_to_term(MetaBin)};
        Error ->
            Error
    end;
head(AddrId, Key, true) ->
    case leo_redundant_manager_api:get_redundancies_by_addr_id(get, AddrId) of
        {ok, #redundancies{nodes = Redundancies}} ->
            head_1(Redundancies, AddrId, Key);
        _ ->
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCY}
    end.

%% @private
head_1([],_,_) ->
    {error, not_found};
head_1([#redundant_node{node = Node,
                        available = true}|Rest], AddrId, Key) when Node == erlang:node() ->
    case leo_object_storage_api:head({AddrId, Key}) of
        {ok, MetaBin} ->
            {ok, binary_to_term(MetaBin)};
        _Other ->
            head_1(Rest, AddrId, Key)
    end;
head_1([#redundant_node{node = Node,
                        available = true}|Rest], AddrId, Key) ->
    RPCKey = rpc:async_call(Node, leo_object_storage_api, head, [{AddrId, Key}]),
    case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
        {value, {ok, MetaBin}} ->
            {ok, binary_to_term(MetaBin)};
        _ ->
            head_1(Rest, AddrId, Key)
    end;
head_1([_|Rest], AddrId, Key) ->
    head_1(Rest, AddrId, Key).

%%--------------------------------------------------------------------
%% API - HEAD with calculating MD5
%%--------------------------------------------------------------------
%% @doc Retrieve a metada/data from backend_db/object-storage
%%      AND calc MD5 based on the body data
%%
-spec(head_with_calc_md5(integer(), string(), any()) ->
             {ok, #?METADATA{}, any()} | {error, any()}).
head_with_calc_md5(AddrId, Key, MD5Context) ->
    leo_object_storage_api:head_with_calc_md5({AddrId, Key}, MD5Context).

%%--------------------------------------------------------------------
%% API - COPY/STACK-SEND/RECEIVE-STORE
%%--------------------------------------------------------------------
%% @doc Replicate an object, which is requested from remote-cluster
%%
-spec(replicate(#?OBJECT{}) ->
             ok | {ok, reference()} | {error, reference()|any()}).
replicate(Object) ->
    %% Transform an object to a metadata
    Metadata = leo_object_storage_transformer:object_to_metadata(Object),
    Method = Object#?OBJECT.method,
    NumOfReplicas = Object#?OBJECT.num_of_replicas,
    AddrId = Metadata#?METADATA.addr_id,

    %% Retrieve redudancies
    case leo_redundant_manager_api:get_redundancies_by_addr_id(AddrId) of
        {ok, #redundancies{nodes = Redundancies,
                           w = WriteQuorum,
                           d = DeleteQuorum}} ->
            %% Replicate an object into the storage cluster
            Redundancies_1 = lists:sublist(Redundancies, NumOfReplicas),
            Quorum_1 = ?quorum(Method, WriteQuorum, DeleteQuorum),
            Quorum_2 = case (NumOfReplicas < Quorum_1) of
                           true when NumOfReplicas =< 1 -> 1;
                           true  -> NumOfReplicas - 1;
                           false -> Quorum_1
                       end,

            leo_storage_replicator:replicate(
              Method, Quorum_2, Redundancies_1,
              Object, replicate_callback());
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Replicate an object from local to remote
%%
-spec(replicate([atom()], integer(), binary()) ->
             ok | not_found | {error, any()}).
replicate(DestNodes, AddrId, Key) ->
    Ref = make_ref(),

    case leo_object_storage_api:head({AddrId, Key}) of
        {ok, MetaBin} ->
            case binary_to_term(MetaBin) of
                #?METADATA{del = ?DEL_FALSE} = Metadata ->
                    case ?MODULE:get({Ref, Key}) of
                        {ok, Ref, Metadata, Bin} ->
                            Ret = leo_sync_local_cluster:stack(
                                    DestNodes, AddrId, Key, Metadata, Bin),
                            Object_1 = leo_object_storage_transformer:metadata_to_object(Metadata),
                            Object_2 = Object_1#?OBJECT{data = Bin},
                            _ = leo_sync_remote_cluster:defer_stack(Object_2),
                            Ret;
                        {error, Ref, Cause} ->
                            {error, Cause}
                    end;
                #?METADATA{del = ?DEL_TRUE} = Metadata ->
                    leo_sync_local_cluster:stack(
                      DestNodes, AddrId, Key, Metadata, <<>>),
                    Object_1 = leo_object_storage_transformer:metadata_to_object(Metadata),
                    Object_2 = Object_1#?OBJECT{data = <<>>},
                    leo_sync_remote_cluster:defer_stack(Object_2);
                _ ->
                    {error, invalid_data_type}
            end;
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% API - Prefix Search (Fetch)
%%--------------------------------------------------------------------
prefix_search(ParentDir, Marker, MaxKeys) ->
    Fun = fun(Key, V, Acc) when length(Acc) =< MaxKeys ->
                  Meta = binary_to_term(V),
                  InRange = case Marker of
                                [] -> true;
                                Key -> false;
                                _  ->
                                    (Marker == hd(lists:sort([Marker, Key])))
                            end,

                  Token_1  = leo_misc:binary_tokens(ParentDir, ?DEF_DELIMITER),
                  Token_2  = leo_misc:binary_tokens(Key,       ?DEF_DELIMITER),
                  Length_1 = erlang:length(Token_1),
                  Length_2 = Length_1 + 1,
                  Length_3 = erlang:length(Token_2),
                  IsChunkedObj = (nomatch /= binary:match(Key, <<"\n">>)),

                  Pos_1 = case binary:match(Key, [ParentDir]) of
                              nomatch ->
                                  -1;
                              {Pos, _} ->
                                  Pos
                          end,

                  case (InRange == true
                        andalso Pos_1 == 0) of
                      true ->
                          case (Length_3 - 1) of
                              %%
                              %%
                              %%
                              Length_1 when Meta#?METADATA.del == ?DEL_FALSE
                                            andalso IsChunkedObj == false ->
                                  KeyLen = byte_size(Key),

                                  case (binary:part(Key, KeyLen - 1, 1) == ?DEF_DELIMITER
                                        andalso KeyLen > 1) of
                                      true  ->
                                          case lists:keyfind(Key, 2, Acc) of
                                              false ->
                                                  ordsets:add_element(#?METADATA{key   = Key,
                                                                                 dsize = -1}, Acc);
                                              _ ->
                                                  Acc
                                          end;
                                      false ->
                                          case lists:keyfind(Key, 2, Acc) of
                                              false ->
                                                  ordsets:add_element(Meta#?METADATA{offset    = 0,
                                                                                     ring_hash = 0}, Acc);
                                              #?METADATA{clock = Clock} when Meta#?METADATA.clock > Clock ->
                                                  Acc_1 = lists:keydelete(Key, 2, Acc),
                                                  ordsets:add_element(Meta#?METADATA{offset    = 0,
                                                                                     ring_hash = 0}, Acc_1);
                                              _ ->
                                                  Acc
                                          end
                                  end;
                              %%
                              %%
                              %%
                              _Any when Meta#?METADATA.del == ?DEL_FALSE
                                        andalso IsChunkedObj == false ->
                                  {Token2, _} = lists:split(Length_2, Token_2),
                                  Dir = lists:foldl(fun(Bin_1, <<>>) ->
                                                            << Bin_1/binary,
                                                               ?DEF_DELIMITER/binary >>;
                                                       (Bin_1, Bin_2) ->
                                                            << Bin_2/binary,
                                                               Bin_1/binary,
                                                               ?DEF_DELIMITER/binary >>
                                                    end, <<>>, Token2),
                                  case lists:keyfind(Dir, 2, Acc) of
                                      false ->
                                          ordsets:add_element(#?METADATA{key   = Dir,
                                                                         dsize = -1}, Acc);
                                      _ ->
                                          Acc
                                  end;
                              _ ->
                                  Acc
                          end;
                      false ->
                          Acc
                  end;
             (_, _, Acc) ->
                  Acc
          end,
    leo_object_storage_api:fetch_by_key(ParentDir, Fun).


%% @doc Retrieve object of deletion from object-storage by key
%%
-spec(prefix_search_and_remove_objects(ParentDir) ->
             {ok, [_]} |
             not_found when ParentDir::undefined|binary()).
prefix_search_and_remove_objects(undefined) ->
    not_found;
prefix_search_and_remove_objects(ParentDir) ->
    Fun = fun(Key, V, Acc) ->
                  Metadata = binary_to_term(V),
                  AddrId   = Metadata#?METADATA.addr_id,

                  Pos_1 = case binary:match(Key, [ParentDir]) of
                              nomatch ->
                                  -1;
                              {Pos, _} ->
                                  Pos
                          end,

                  case (Pos_1 == 0) of
                      true when Metadata#?METADATA.del == ?DEL_FALSE ->
                          leo_storage_mq:publish(
                            ?QUEUE_TYPE_ASYNC_DELETION, AddrId, Key);
                      _ ->
                          Acc
                  end,
                  Acc
          end,
    leo_object_storage_api:fetch_by_key(ParentDir, Fun).


%% @doc Find already uploaded objects by original-filename
%%
-spec(find_uploaded_objects_by_key(binary()) ->
             {ok, list()} | not_found).
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


%%--------------------------------------------------------------------
%% INNNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc read reapir - compare with remote-node's meta-data.
%%
-spec(read_and_repair(#read_parameter{}, [#redundant_node{}]) ->
             {ok, #?METADATA{}, binary()} |
             {ok, match} |
             {error, any()}).
read_and_repair(_, []) ->
    {error, not_found};

read_and_repair(#read_parameter{addr_id   = AddrId,
                                key       = Key,
                                etag      = 0,
                                start_pos = StartPos,
                                end_pos   = EndPos} = ReadParameter,
                [#redundant_node{node = Node,
                                 available = true}|T]
               ) when Node == erlang:node() ->
    read_and_repair_1(
      get_fun(AddrId, Key, StartPos, EndPos), ReadParameter, T);

%% Retrieve an head of object,
%%     then compare it with requested 'Etag'
read_and_repair(#read_parameter{addr_id   = AddrId,
                                key       = Key,
                                etag      = ETag,
                                start_pos = StartPos,
                                end_pos   = EndPos} = ReadParameter,
                [#redundant_node{node = Node,
                                 available = true}|T]
               ) when Node == erlang:node() ->
    Ret = case leo_object_storage_api:head({AddrId, Key}) of
              {ok, MetaBin} ->
                  Metadata = binary_to_term(MetaBin),
                  case Metadata#?METADATA.checksum of
                      ETag ->
                          {ok, match};
                      _ ->
                          []
                  end;
              _ ->
                  []
          end,

    %% If the result is 'match', then response it,
    %% not the case, retrieve an object by key
    case Ret of
        {ok, match} = Reply ->
            Reply;
        _ ->
            read_and_repair_1(
              get_fun(AddrId, Key, StartPos, EndPos), ReadParameter, T)
    end;

read_and_repair(ReadParameter, [#redundant_node{node = Node,
                                                available = true}|_] = Redundancies) ->
    RPCKey = rpc:async_call(Node, ?MODULE, get, [ReadParameter, Redundancies]),
    Reply  = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
                 {value, {ok, Meta, Bin}} ->
                     {ok, Meta, #?OBJECT{data = Bin}};
                 {value, {ok, match} = Ret} ->
                     Ret;
                 {value, {error, Cause}} ->
                     {error, Cause};
                 {value, {badrpc, Cause}} ->
                     {error, Cause};
                 timeout = Cause ->
                     {error, Cause}
             end,
    read_and_repair_1(Reply, ReadParameter, []);

read_and_repair(ReadParameter, [_|T]) ->
    read_and_repair(ReadParameter, T).


%% @private
read_and_repair_1({ok, Metadata, #?OBJECT{data = Bin}},
                  #read_parameter{}, []) ->
    {ok, Metadata, Bin};
read_and_repair_1({ok, match} = Reply, #read_parameter{}, []) ->
    Reply;
read_and_repair_1({ok, Metadata, #?OBJECT{data = Bin}},
                  #read_parameter{quorum = Quorum} = ReadParameter, Redundancies) ->
    Fun = fun(ok) ->
                  {ok, Metadata, Bin};
             ({error,_Cause}) ->
                  {error, ?ERROR_RECOVER_FAILURE}
          end,
    ReadParameter_1 = ReadParameter#read_parameter{quorum = Quorum - 1},
    leo_storage_read_repairer:repair(ReadParameter_1, Redundancies, Metadata, Fun);

read_and_repair_1({error, not_found = Cause}, #read_parameter{key = _K}, []) ->
    {error, Cause};
read_and_repair_1({error, timeout = Cause}, #read_parameter{key = _K}, _Redundancies) ->
    ?output_warn("read_and_repair_1/3", _K, Cause),
    {error, Cause};
read_and_repair_1({error, Cause}, #read_parameter{key = _K}, []) ->
    ?output_warn("read_and_repair_1/3", _K, Cause),
    {error, Cause};

read_and_repair_1({error, Reason},
                  #read_parameter{key = _K,
                                  quorum = ReadQuorum} = ReadParameter, Redundancies) ->
    NumOfNodes = erlang:length([N || #redundant_node{node = N,
                                                     can_read_repair = true}
                                         <- Redundancies]),
    case (NumOfNodes >= ReadQuorum) of
        true ->
            read_and_repair(ReadParameter, Redundancies);
        false when Reason == not_found ->
            {error, not_found};
        false ->
            ?output_warn("read_and_repair_1/3", _K, Reason),
            {error, ?ERROR_COULD_NOT_GET_DATA}
    end;
read_and_repair_1(_,_,_) ->
    {error, invalid_request}.


%% @doc Replicate an object from local-node to remote node
%% @private
-spec(replicate_fun(replication(), request_verb(), integer(), #?OBJECT{}) ->
             ok | {error, any()}).
replicate_fun(?REP_LOCAL, Method, AddrId, Object) ->
    case leo_redundant_manager_api:get_redundancies_by_addr_id(put, AddrId) of
        {ok, #redundancies{nodes     = Redundancies,
                           w         = WriteQuorum,
                           d         = DeleteQuorum,
                           ring_hash = RingHash}} ->

            Quorum = case Method of
                         ?CMD_PUT    -> WriteQuorum;
                         ?CMD_DELETE -> DeleteQuorum
                     end,

            leo_storage_replicator:replicate(
              Method, Quorum, Redundancies, Object#?OBJECT{ring_hash = RingHash},
              replicate_callback(Object));
        {error,_Cause} ->
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCY}
    end.

%% @doc obj-replication request from remote node.
%%
replicate_fun(?REP_REMOTE, Method, Object) ->
    Ref = make_ref(),
    Ret = case Method of
              ?CMD_PUT    -> ?MODULE:put({Object, Ref});
              ?CMD_DELETE -> ?MODULE:delete({Object, Ref})
          end,
    case Ret of
        %% Put
        {ok, Ref, Checksum} ->
            {ok, Checksum};
        %% Delete
        {ok, Ref} ->
            ok;
        {error, Ref, not_found} ->
            {error, not_found};
        {error, Ref, Cause} ->
            ?warn("replicate_fun/3", "cause:~p", [Cause]),
            {error, Cause}
    end.


%% @doc Being callback, after executed replication of an object
%% @private
-spec(replicate_callback() ->
             function()).
replicate_callback() ->
    replicate_callback(null).

-spec(replicate_callback(#?OBJECT{}|null) ->
             function()).
replicate_callback(Object) ->
    fun({ok, ?CMD_PUT, Checksum}) ->
            ok = leo_sync_remote_cluster:defer_stack(Object),
            {ok, Checksum};
       ({ok,?CMD_DELETE,_Checksum}) ->
            ok = leo_sync_remote_cluster:defer_stack(Object),
            ok;
       ({ok,_,_Checksum}) ->
            ok = leo_sync_remote_cluster:defer_stack(Object),
            ok;
       ({error, Cause}) ->
            case lists:keyfind(not_found, 2, Cause) of
                false ->
                    {error, ?ERROR_REPLICATE_FAILURE};
                _ ->
                    {error, not_found}
            end
    end.
