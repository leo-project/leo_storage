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
-include_lib("leo_ordning_reda/include/leo_ordning_reda.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([get/1, get/3, get/4, get/5,
         put/1, put/2, put/3,
         delete/1, delete/2, head/2,
         copy/3,
         prefix_search/3, prefix_search_and_remove_objects/1,
         find_uploaded_objects_by_key/1
        ]).

-define(REP_LOCAL,  'local').
-define(REP_REMOTE, 'remote').
-type(replication() :: ?REP_LOCAL | ?REP_REMOTE).

-define(DEF_DELIMITER, <<"/">>).

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
        {ok, #redundancies{id = AddrId}} ->
            case get_fun(Ref, AddrId, Key) of
                {ok, Ref, Metadata, #object{data = Bin}} ->
                    {ok, Metadata, Bin};
                {error, Ref, Cause} ->
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

    Ret =  case leo_redundant_manager_api:get_redundancies_by_addr_id(get, AddrId) of
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
        {ok, NewMeta, #object{data = Bin}} ->
            {ok, NewMeta, Bin};
        Error ->
            Error
    end.


%% @doc read data (common).
%% @private
-spec(get_fun(reference(), integer(), string()) ->
             {ok, reference(), #metadata{}, pid()} | {error, reference(), any()}).
get_fun(Ref, AddrId, Key) ->
    get_fun(Ref, AddrId, Key, 0, 0).

-spec(get_fun(reference(), integer(), string(), integer(), integer()) ->
             {ok, reference(), #metadata{}, pid()} | {error, reference(), any()}).
get_fun(Ref, AddrId, Key, StartPos, EndPos) ->
    case leo_object_storage_api:get({AddrId, Key}, StartPos, EndPos) of
        {ok, Metadata, Object} ->
            {ok, Ref, Metadata, Object};
        not_found = Cause ->
            {error, Ref, Cause};
        {error, Cause} ->
            {error, Ref, Cause}
    end.


%%--------------------------------------------------------------------
%% API - PUT
%%--------------------------------------------------------------------
%% @doc Insert an  object (request from remote-storage-nodes).
%%
-spec(put(#object{}) ->
             {ok, atom()} | {error, any()}).
put(Object) ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_PUT),
    replicate(?REP_REMOTE, ?CMD_PUT, Object).

%% @doc Insert an object (request from gateway).
%%
-spec(put(#object{}, integer()|reference()) ->
             ok | {error, any()}).
put(Object, ReqId) when is_integer(ReqId)  ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_PUT),

    replicate(?REP_LOCAL, ?CMD_PUT, Object#object.addr_id,
              Object#object{method = ?CMD_PUT,
                            clock  = leo_date:clock(),
                            req_id = ReqId});

put(Object, Ref) when is_reference(Ref) ->
    AddrId = Object#object.addr_id,
    Key    = Object#object.key,

    case Object#object.del of
        ?DEL_TRUE->
            case leo_object_storage_api:head({AddrId, Key}) of
                {ok, MetaBin} ->
                    case binary_to_term(MetaBin) of
                        #metadata{cnumber = 0} ->
                            put_fun(Ref, AddrId, Key, Object);
                        #metadata{cnumber = CNumber} ->
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
    end;
put(_,_) ->
    {error, badarg}.

%% @doc Insert an  object (request from remote-storage-nodes).
%%
-spec(put(pid(), #object{}, integer()) ->
             {ok, atom()} | {error, any()}).
put(From, Object, ReqId) ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_PUT),

    case replicate(?REP_REMOTE, ?CMD_PUT, Object) of
        {ok, ETag} ->
            From ! {ok, ETag};
        {error, Cause} ->
            ?warn("put/3", "req-id:~w, cause:~p", [ReqId, Cause]),
            From ! {error, {node(), Cause}}
    end.


%% Input an object into the object-storage
%% @private
-spec(put_fun(reference(), integer(), binary(), #object{}) ->
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

    case delete(#object{addr_id  = AddrId,
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
%% @doc Remove an object (request from remote-storage-nodes).
%%
-spec(delete(#object{}) ->
             ok | {error, any()}).
delete(Object) ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_DEL),
    replicate(?REP_REMOTE, ?CMD_DELETE, Object).

%% @doc Remova an object (request from gateway)
%%
-spec(delete(#object{}, integer()|reference()) ->
             ok | {error, any()}).
delete(Object, ReqId) when is_integer(ReqId) ->
    _ = leo_statistics_req_counter:increment(?STAT_REQ_DEL),
    replicate(?REP_LOCAL, ?CMD_DELETE,
              Object#object.addr_id, Object#object{method   = ?CMD_DELETE,
                                                   data     = <<>>,
                                                   dsize    = 0,
                                                   clock    = leo_date:clock(),
                                                   req_id   = ReqId,
                                                   del      = ?DEL_TRUE});

delete(Object, Ref) when is_reference(Ref) ->
    AddrId = Object#object.addr_id,
    Key    = Object#object.key,

    case leo_object_storage_api:head({AddrId, Key}) of
        not_found = Cause ->
            {error, Ref, Cause};
        {ok, Metadata} when Metadata#metadata.del == ?DEL_TRUE ->
            {ok, Ref};
        {ok, Metadata} when Metadata#metadata.del == ?DEL_FALSE ->
            case leo_object_storage_api:delete({AddrId, Key}, Object) of
                ok ->
                    {ok, Ref};
                {error, Why} ->
                    {error, Ref, Why}
            end;
        {error, _Cause} ->
            {error, Ref, ?ERROR_COULD_NOT_GET_META}
    end;
delete(_,_) ->
    {error, badarg}.


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
%% API - COPY/STACK-SEND/RECEIVE-STORE
%%--------------------------------------------------------------------
%% @doc copy an object.
%%
-spec(copy(list(), integer(), string()) ->
             ok | not_found | {error, any()}).
copy(DestNodes, AddrId, Key) ->
    Ref = make_ref(),
    case ?MODULE:head(AddrId, Key) of
        {ok, #metadata{del = ?DEL_FALSE} = Metadata} ->
            case ?MODULE:get({Ref, Key}) of
                {ok, Metadata, Bin} ->
                    leo_storage_ordning_reda_client:stack(DestNodes, AddrId, Key, Metadata, Bin);
                {error, Ref, Cause} ->
                    {error, Cause}
            end;
        {ok, #metadata{del = ?DEL_TRUE} = Metadata} ->
            leo_storage_ordning_reda_client:stack(DestNodes, AddrId, Key, Metadata, <<>>);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% API - Prefix Search (Fetch)
%%--------------------------------------------------------------------
prefix_search(ParentDir, Marker, MaxKeys) ->
    Fun = fun(K, V, Acc0) when length(Acc0) =< MaxKeys ->
                  {_AddrId, Key} = binary_to_term(K),
                  Meta0   = binary_to_term(V),
                  InRange = case Marker of
                                [] -> true;
                                _  ->
                                    (Marker == hd(lists:sort([Marker, Key])))
                            end,

                  Token0 = leo_misc:binary_tokens(ParentDir, ?DEF_DELIMITER),
                  Token1 = leo_misc:binary_tokens(Key,       ?DEF_DELIMITER),

                  Length0 = erlang:length(Token0),
                  Length1 = Length0 + 1,
                  Length2 = erlang:length(Token1),

                  IsChunkedObj = (nomatch /= binary:match(Key, <<"\n">>)),

                  Pos1 = case binary:match(Key, [ParentDir]) of
                             nomatch ->
                                 -1;
                             {Pos0, _} ->
                                 Pos0
                         end,

                  case (InRange == true andalso Pos1 == 0) of
                      true ->
                          case (Length2 -1) of
                              Length0 when Meta0#metadata.del == ?DEL_FALSE andalso
                                           IsChunkedObj == false ->
                                  KeyLen = byte_size(Key),

                                  case (binary:part(Key, KeyLen - 1, 1) == ?DEF_DELIMITER andalso KeyLen > 1) of
                                      true  ->
                                          case lists:keyfind(Key, 2, Acc0) of
                                              false ->
                                                  ordsets:add_element(#metadata{key      = Key,
                                                                                dsize    = -1}, Acc0);
                                              _ ->
                                                  Acc0
                                          end;
                                      false ->
                                          case lists:keyfind(Key, 2, Acc0) of
                                              false ->
                                                  ordsets:add_element(Meta0#metadata{offset    = 0,
                                                                                     ring_hash = 0}, Acc0);
                                              #metadata{clock = Clock} when Meta0#metadata.clock > Clock ->
                                                  Acc1 = lists:keydelete(Key, 2, Acc0),
                                                  ordsets:add_element(Meta0#metadata{offset    = 0,
                                                                                     ring_hash = 0}, Acc1);
                                              _ ->
                                                  Acc0
                                          end
                                  end;

                              Length1 when Meta0#metadata.del == ?DEL_FALSE andalso
                                           IsChunkedObj == false ->
                                  {Token2, _} = lists:split(Length1, Token1),
                                  Dir = lists:foldl(fun(Bin0, <<>>) ->
                                                            << Bin0/binary, ?DEF_DELIMITER/binary >>;
                                                       (Bin0, Bin1) ->
                                                            << Bin1/binary, Bin0/binary, ?DEF_DELIMITER/binary >>
                                                    end, <<>>, Token2),
                                  case lists:keyfind(Dir, 2, Acc0) of
                                      false ->
                                          ordsets:add_element(#metadata{key   = Dir,
                                                                        dsize = -1}, Acc0);
                                      _ ->
                                          Acc0
                                  end;
                              _ ->
                                  Acc0
                          end;
                      false ->
                          Acc0
                  end;
             (_, _, Acc0) ->
                  Acc0
          end,
    leo_object_storage_api:fetch_by_key(ParentDir, Fun).


%% @doc Retrieve object of deletion from object-storage by key
%%
-spec(prefix_search_and_remove_objects(binary()) ->
             ok).
prefix_search_and_remove_objects(ParentDir) ->
    Fun = fun(K, V,_Acc) ->
                  {AddrId, Key} = binary_to_term(K),
                  Metadata      = binary_to_term(V),

                  Pos1 = case binary:match(Key, [ParentDir]) of
                             nomatch ->
                                 -1;
                             {Pos0, _} ->
                                 Pos0
                         end,

                  case (Pos1 == 0) of
                      true when Metadata#metadata.del == ?DEL_FALSE ->
                          leo_storage_mq_client:publish(
                            ?QUEUE_TYPE_ASYNC_DELETION, AddrId, Key);
                      _ ->
                          void
                  end,
                  ok
          end,
    leo_object_storage_api:fetch_by_key(ParentDir, Fun).


%% @doc Find already uploaded objects by original-filename
%%
-spec(find_uploaded_objects_by_key(binary()) ->
             ok).
find_uploaded_objects_by_key(OriginalKey) ->
    Fun = fun(K, V, Acc) ->
                  {_AddrId, Key} = binary_to_term(K),
                  Metadata       = binary_to_term(V),

                  case (nomatch /= binary:match(Key, <<"\n">>)) of
                      true ->
                          Pos1 = case binary:match(Key, [OriginalKey]) of
                                     nomatch   -> -1;
                                     {Pos0, _} -> Pos0
                                 end,
                          case (Pos1 == 0) of
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
                                req_id    = ReqId} = ReadParameter, [_|T] = Redundancies) ->
    Ref   = make_ref(),

    case get_fun(Ref, AddrId, Key, StartPos, EndPos) of
        {ok, Ref, Metadata, Object} when T =:= [] ->
            {ok, Metadata, Object};
        {ok, Ref, Metadata, Object} when T =/= [] ->
            F = fun(ok) ->
                        {ok, Metadata, Object};
                   ({error,_Cause}) ->
                        {error, ?ERROR_RECOVER_FAILURE}
                end,
            leo_storage_read_repairer:repair(
              ReadQuorum -1, Redundancies, Metadata, ReqId, F);

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


%% @doc Replicate an object from local-node to remote node
%% @private
-spec(replicate(replication(), put | delete, integer(), #object{}) ->
             ok | {error, any()}).
replicate(?REP_LOCAL, Method, AddrId, Object0) ->
    case leo_redundant_manager_api:get_redundancies_by_addr_id(put, AddrId) of
        {ok, #redundancies{nodes     = Redundancies,
                           w         = WriteQuorum,
                           d         = DeleteQuorum,
                           ring_hash = RingHash}} ->
            Object1 = Object0#object{ring_hash = RingHash},
            Quorum  = case Method of
                          ?CMD_PUT    -> WriteQuorum;
                          ?CMD_DELETE -> DeleteQuorum
                      end,

            F = fun({ok, ETag}) when Method == ?CMD_PUT ->
                        {ok, ETag};
                   ({ok,_ETag}) when Method == ?CMD_DELETE ->
                        ok;
                   ({error,_Cause}) ->
                        {error, ?ERROR_REPLICATE_FAILURE}
                end,
            leo_storage_replicator:replicate(Quorum, Redundancies, Object1, F);
        _Error ->
            {error, ?ERROR_META_NOT_FOUND}
    end;
replicate(_,_,_,_) ->
    {error, badarg}.


%% @doc obj-replication request from remote node.
%%
replicate(?REP_REMOTE, Method, Object) ->
    Ref  = make_ref(),
    Ret0 = case Method of
               ?CMD_PUT    -> ?MODULE:put(Object, Ref);
               ?CMD_DELETE -> ?MODULE:delete(Object, Ref)
           end,

    case Ret0 of
        %% Put
        {ok, Ref, ETag} ->
            {ok, ETag};
        %% Delete
        {ok, Ref} ->
            ok;
        {error, Ref, Cause} ->
            {error, Cause}
    end;
replicate(_,_,_) ->
    {error, badarg}.

