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
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([get/1, get/3, get/4, get/5,
         put/1, put/2, put/6, delete/1, delete/4,
         head/2, copy/3, prefix_search/3]).

-define(PROC_TYPE_REPLICATE,   'replicate').
-define(PROC_TYPE_READ_REPAIR, 'read_repair').

-record(read_parameter, {
          addr_id       :: integer(),
          key           :: string(),
          start_pos = 0 :: integer(),
          end_pos   = 0 :: integer(),
          quorum        :: integer(),
          req_id        :: integer()
         }).

%%--------------------------------------------------------------------
%% API - GET
%%--------------------------------------------------------------------
%% @doc get object (from storage-node).
%%
-spec(get({reference(), string()}) ->
             {ok, reference(), binary(), binary(), binary()} |
             {error, reference(), any()}).
get({Ref, Key}) ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_GET),

    case leo_redundant_manager_api:get_redundancies_by_key(get, Key) of
        {ok, #redundancies{id = Id}} ->
            case get_fun(Ref, Id, Key) of
                {ok, Ref, Metadata, ObjectPool} ->
                    case leo_object_storage_pool:get(ObjectPool) of
                        not_found = Cause ->
                            {error, Cause};
                        #object{data = Bin} ->
                            ok = leo_object_storage_pool:destroy(ObjectPool),
                            {ok, Metadata, Bin}
                    end;
                {error, Cause} ->
                    {error, Ref, Cause}
            end;
        _ ->
            {error, Ref, ?ERROR_COULD_NOT_GET_REDUNDANCY}
    end.

%% @doc Retrieve an object which is requested from gateway.
%%
-spec(get(integer(), string(), integer()) ->
             {ok, #metadata{}, binary()} |
             {error, any()}).
get(AddrId, Key, ReqId) ->
    get(AddrId, Key, 0, 0, ReqId).

%% @doc Retrieve an object which is requested from gateway w/etag.
%%
-spec(get(integer(), string(), string(), integer()) ->
             {ok, #metadata{}, binary()} |
             {ok, match} |
             {error, any()}).
get(AddrId, Key, ETag, ReqId) ->
    case leo_object_storage_api:head({AddrId, Key}) of
        {ok, MetaBin} ->
            Metadata = binary_to_term(MetaBin),
            case (Metadata#metadata.checksum == ETag) of
                true ->
                    {ok, match};
                false ->
                    get(AddrId, Key, ReqId)
            end;
        not_found = Cause ->
            {error, Cause};
        Error ->
            Error
    end.

%% @doc Retrieve a part of an object.
%%
-spec(get(integer(), string(), integer(), integer(), integer()) ->
             {ok, #metadata{}, binary()} |
             {error, any()}).
get(AddrId, Key, StartPos, EndPos, ReqId) ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_GET),

    Ret = case leo_redundant_manager_api:get_redundancies_by_addr_id(get, AddrId) of
              {ok, #redundancies{nodes = Redundancies, r = ReadQuorum}} ->
                  ReadParameter = #read_parameter{addr_id   = AddrId,
                                                  key       = Key,
                                                  start_pos = StartPos,
                                                  end_pos   = EndPos,
                                                  quorum    = ReadQuorum,
                                                  req_id    = ReqId},
                  read_and_repair(ReadParameter, Redundancies);
              _Error ->
                  {error, ?ERROR_COULD_NOT_GET_REDUNDANCY}
          end,

    case Ret of
        {ok, NewMeta, ObjectPool} ->
            case leo_object_storage_pool:get(ObjectPool) of
                not_found = Cause ->
                    {error, Cause};
                #object{data = Bin} ->
                    ok = leo_object_storage_pool:destroy(ObjectPool),
                    {ok, NewMeta, Bin}
            end;
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% API - PUT
%%--------------------------------------------------------------------
%% @doc put object (from gateway).
%%
-spec(put(integer(), string(), binary(), integer(), integer(), integer()) ->
             ok | {error, any()}).
put(AddrId, Key, Bin, Size, ReqId, Timestamp) ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_PUT),
    ObjectPool = leo_object_storage_pool:new(#object{method    = ?CMD_PUT,
                                                     addr_id   = AddrId,
                                                     key       = Key,
                                                     data      = Bin,
                                                     dsize     = Size,
                                                     req_id    = ReqId,
                                                     timestamp = Timestamp,
                                                     del       = 0}),
    replicate({?CMD_PUT, AddrId, Key, ObjectPool}).

-spec(put(pid(), reference()) ->
             ok | {error, any()}).
put(ObjectPool, Ref) ->
    put_fun(ObjectPool, Ref).

%% @doc put object (from remote-storage-nodes).
%%
-spec(put(#object{}) ->
             {ok, atom()} | {error, any()}).
put(DataObject) when erlang:is_record(DataObject, object) ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_PUT),
    replicate(?CMD_PUT, DataObject).


%%--------------------------------------------------------------------
%% API - DELETE
%%--------------------------------------------------------------------
%% @doc delete object.
%%
-spec(delete(integer(), string(), integer(), integer()) ->
             ok | {error, any()}).
delete(AddrId, Key, ReqId, Timestamp) ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_DEL),
    replicate(?CMD_DELETE, Key, AddrId, ReqId, Timestamp).

%% @doc delete object (from remote-storage-nodes).
%%
-spec(delete(#object{}) ->
             ok | {error, any()}).
delete(DataObject) ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_DEL),
    replicate(?CMD_DELETE, DataObject).


%%--------------------------------------------------------------------
%% API - HEAD
%%--------------------------------------------------------------------
%% @doc retrieve a meta-data from mata-data-server (file).
%%
-spec(head(integer(), string()) ->
             {ok, #metadata{}} |
             {error, any}).
head(AddrId, Key) ->
    case leo_object_storage_api:head({AddrId, Key}) of
        {ok, MetaBin} ->
            {ok, binary_to_term(MetaBin)};
        not_found = Cause ->
            {error, Cause};
        {error, Why} ->
            {error, Why}
    end.


%%--------------------------------------------------------------------
%% API - COPY
%%--------------------------------------------------------------------
%% @doc copy an object.
%%
-spec(copy(list(), integer(), string()) ->
             ok | not_found | {error, any()}).
copy(InconsistentNodes, AddrId, Key) ->
    Ref = make_ref(),

    case ?MODULE:head(AddrId, Key) of
        {ok, #metadata{del = 0} = Metadata} ->
            case ?MODULE:get({Ref, Key}) of
                {ok, Metadata, Bin} ->
                    copy(?CMD_PUT, InconsistentNodes, Metadata, #object{data  = Bin,
                                                                        dsize = byte_size(Bin)});
                {error, Ref, Cause} ->
                    {error, Cause}
            end;
        {ok, #metadata{del = 1} = Metadata} ->
            copy(?CMD_DELETE, InconsistentNodes, Metadata, #object{});
        Error ->
            Error
    end.

copy(Method, InconsistentNodes, #metadata{key       = Key,
                                          addr_id   = AddrId,
                                          clock     = Clock,
                                          timestamp = Timestamp,
                                          del       = DelFlag}, DataObj) ->
    %% @TODO > ordning&reda
    ObjectPool = leo_object_storage_pool:new(DataObj#object{method    = Method,
                                                            addr_id   = AddrId,
                                                            key       = Key,
                                                            clock     = Clock,
                                                            timestamp = Timestamp,
                                                            del       = DelFlag}),
    Ref = make_ref(),
    Nodes = lists:map(fun(Item) ->
                              {Item, true}
                      end, InconsistentNodes),

    ProcId = get_proc_id(?PROC_TYPE_REPLICATE),
    case leo_storage_replicate_server:replicate(
           ProcId, Ref, length(InconsistentNodes), Nodes, ObjectPool) of
        {ok, Ref}->
            ok = leo_object_storage_pool:destroy(ObjectPool),
            ok;
        {error, Ref, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% API - Prefix Search (Fetch)
%%--------------------------------------------------------------------
prefix_search(ParentDir, Marker, MaxKeys) ->
    Delimiter = "/",
    Fun = fun(K, V, Acc) when length(Acc) =< MaxKeys ->
                  {_AddrId, Key} = binary_to_term(K),
                  Metadata       = binary_to_term(V),
                  InRange = case Marker of
                                [] -> true;
                                _  ->
                                    (Marker == hd(lists:sort([Marker, Key])))
                            end,

                  Token0  = string:tokens(ParentDir, Delimiter),
                  Token1  = string:tokens(Key,       Delimiter),

                  Length0 = erlang:length(Token0),
                  Length1 = Length0 + 1,
                  Length2 = erlang:length(Token1),

                  case (InRange == true andalso string:str(Key, ParentDir) == 1) of
                      true ->
                          case (Length2 -1) of
                              Length0 when Metadata#metadata.del == 0 ->
                                  case (string:rstr(Key, Delimiter) == length(Key)) of
                                      true  -> ordsets:add_element(#metadata{key   = Key,
                                                                             dsize = -1}, Acc);
                                      false -> ordsets:add_element(Metadata, Acc)
                                  end;
                              Length1 when Metadata#metadata.del == 0 ->
                                  {Token2, _} = lists:split(Length1, Token1),
                                  Dir = lists:foldl(fun(Str0, []  ) -> Str0 ++ Delimiter;
                                                       (Str0, Str1) -> Str1 ++ Str0 ++ Delimiter
                                                    end, [], Token2),
                                  ordsets:add_element(#metadata{key   = Dir,
                                                                dsize = -1}, Acc);
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


%%--------------------------------------------------------------------
%% INNNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc put object.
%%
-spec(put_fun(pid(), reference()) ->
             ok | {error, any()}).
put_fun(ObjectPool, Ref) ->
    case catch leo_object_storage_pool:head(ObjectPool) of
        {'EXIT', Cause} ->
            {error, Ref, Cause};
        not_found ->
            {error, Ref, timeout};
        #metadata{key = Key, addr_id = AddrId} ->
            case leo_object_storage_api:put({AddrId, Key}, ObjectPool) of
                ok ->
                    {ok, Ref};
                {error, Cause} ->
                    {error, Ref, Cause}
            end
    end.


%% @doc read data (common).
%%
-spec(get_fun(reference(), integer(), string()) ->
             {ok, reference(), #metadata{}, pid()} | {error, reference(), any()}).
get_fun(Ref, AddrId, Key) ->
    get_fun(Ref, AddrId, Key, 0, 0).

-spec(get_fun(reference(), integer(), string(), integer(), integer()) ->
             {ok, reference(), #metadata{}, pid()} | {error, reference(), any()}).
get_fun(Ref, AddrId, Key, StartPos, EndPos) ->
    case leo_object_storage_api:get({AddrId, Key}, StartPos, EndPos) of
        {ok, Metadata, ObjectPool} ->
            {ok, Ref, Metadata, ObjectPool};
        not_found = Cause ->
            {error, Ref, Cause};
        {error, Cause} ->
            {error, Ref, Cause}
    end.


%% @doc delete object.
%%
-spec(delete_fun(pid(), reference()) ->
             ok | {error, any()}).
delete_fun(ObjectPool, Ref) ->
    case catch leo_object_storage_pool:get(ObjectPool) of
        {'EXIT', Cause} ->
            {error, Ref, Cause};
        not_found ->
            {error, Ref, timeout};
        #object{addr_id = AddrId,
                key      = Key} ->
            case leo_object_storage_api:head({AddrId, Key}) of
                not_found = Cause ->
                    {error, Ref, Cause};
                {ok, Metadata} when Metadata#metadata.del == 1 ->
                    {error, Ref, not_found};
                {ok, Metadata} when Metadata#metadata.del == 0 ->
                    case leo_object_storage_api:delete({AddrId, Key}, ObjectPool) of
                        ok ->
                            {ok, Ref};
                        {error, Why} ->
                            {error, Ref, Why}
                    end;
                {error, _Cause} ->
                    {error, Ref, ?ERROR_COULD_NOT_GET_META}
            end
    end.


%% @doc read reapir - compare with remote-node's meta-data.
%%
-spec(read_and_repair(#read_parameter{}, list()) ->
             {ok, #metadata{}, binary()} |
             {error, any()}).
read_and_repair(_, []) ->
    {error, ?ERROR_COULD_NOT_GET_DATA};

read_and_repair(#read_parameter{addr_id   = AddrId,
                                key       = Key,
                                start_pos = StartPos,
                                end_pos   = EndPos,
                                quorum    = ReadQuorum,
                                req_id    = ReqId} = ReadParameter, [_|T]) ->
    Ref   = make_ref(),

    case get_fun(Ref, AddrId, Key, StartPos, EndPos) of
        {ok, Ref, Metadata, ObjectPool} when T =:= [] ->
            {ok, Metadata, ObjectPool};
        {ok, Ref, Metadata, ObjectPool} when T =/= [] ->
            ProcId = get_proc_id(?PROC_TYPE_READ_REPAIR),
            case leo_storage_read_repair_server:repair(ProcId, Ref, ReadQuorum -1, T, Metadata, ReqId) of
                {ok, Ref} ->
                    {ok, Metadata, ObjectPool};
                {error, Ref} ->
                    {error, ?ERROR_RECOVER_FAILURE}
            end;
        {error, Ref, not_found = Cause} ->
            {error, Cause};
        {error, Ref, _Cause} ->
            case (erlang:length(T) >= ReadQuorum) of
                true ->
                    read_and_repair(ReadParameter, T);
                false ->
                    {error, ?ERROR_COULD_NOT_GET_DATA}
            end
    end.

%% @doc replicate object.
%%
-spec(replicate(atom(), string(), integer(), integer(), integer()) ->
             ok | {error, any}).
replicate(?CMD_DELETE = Method, Key, AddrId, ReqId, Timestamp) ->
    ObjectPool = leo_object_storage_pool:new(#object{method    = Method,
                                                     addr_id   = AddrId,
                                                     key       = Key,
                                                     data      = <<>>,
                                                     dsize     = 0,
                                                     req_id    = ReqId,
                                                     timestamp = Timestamp,
                                                     del       = 1}),
    replicate({Method, AddrId, Key, ObjectPool}).

replicate({Method, AddrId, _Key, ObjectPool}) ->
    case leo_redundant_manager_api:get_redundancies_by_addr_id(put, AddrId) of
        {ok, #redundancies{nodes = Redundancies,
                           w = WriteQuorum,
                           d = DeleteQuorum,
                           ring_hash = RingHash}} ->
            leo_object_storage_pool:set_ring_hash(ObjectPool, RingHash),

            Ref = make_ref(),
            ProcId0 = get_proc_id(?PROC_TYPE_REPLICATE),
            Quorum  = case Method of
                          ?CMD_PUT    -> WriteQuorum;
                          ?CMD_DELETE -> DeleteQuorum
                      end,

            case leo_storage_replicate_server:replicate(
                   ProcId0, Ref, Quorum, Redundancies, ObjectPool) of
                {ok, Ref} ->
                    ok = leo_object_storage_pool:destroy(ObjectPool),
                    ok;
                {error, Ref, _Cause} ->
                    {error, ?ERROR_REPLICATE_FAILURE}
            end;

        undefined ->
            {error, ?ERROR_META_NOT_FOUND}
    end.

%% @doc obj-replication request from remote node.
%%
-spec(replicate(atom(), #object{}) ->
             ok | {error, any()}).
replicate(Method, DataObject) when is_record(DataObject, object) == true ->
    #object{key      = Key,
            addr_id  = AddrId,
            clock    = Clock,
            checksum = Checksum} = DataObject,
    case leo_object_storage_api:head({AddrId, Key}) of
        {ok, Metadata} when Metadata#metadata.clock    =:= Clock andalso
                            Metadata#metadata.checksum =:= Checksum ->
            {ok, erlang:node()};

        _Other ->
            ObjectPool = leo_object_storage_pool:new(DataObject),
            Ref  = make_ref(),
            Ret0 = case Method of
                       ?CMD_PUT    -> put_fun(ObjectPool, Ref);
                       ?CMD_DELETE -> delete_fun(ObjectPool, Ref)
                   end,
            Ret1 = case Ret0 of
                       {ok, Ref}           -> ok;
                       {error, Ref, Cause} -> {error, Cause}
                   end,

            ok = leo_object_storage_pool:destroy(ObjectPool),
            Ret1
    end;
replicate(_Method, _DataObject) ->
    {error, badarg}.


%% @doc
%%
-spec(get_proc_id(?PROC_TYPE_REPLICATE | ?PROC_TYPE_READ_REPAIR) ->
             atom()).
get_proc_id(?PROC_TYPE_REPLICATE) ->
    N = (leo_utils:clock() rem ?env_num_of_replicators()),
    list_to_atom(?PFIX_REPLICATOR ++ integer_to_list(N));

get_proc_id(?PROC_TYPE_READ_REPAIR) ->
    N = (leo_utils:clock() rem ?env_num_of_repairers()),
    list_to_atom(?PFIX_REPAIRER   ++ integer_to_list(N)).

