%%======================================================================
%%
%% LeoFS Storage
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
%% @doc Handling an object, which is included in put, get, delete and head operation
%% @end
%%======================================================================
-module(leo_storage_handler_object).

-include("leo_storage.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_ordning_reda/include/leo_ordning_reda.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-undef(MAX_RETRY_TIMES).
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([get/1, get/2, get/3, get/4, get/5,
         put/1, put/2, put/4,
         delete/1, delete/2, delete/3,
         head/2, head/3,
         head_with_calc_md5/3,
         replicate/1, replicate/3
        ]).

-define(REP_LOCAL,  'local').
-define(REP_REMOTE, 'remote').
-type(replication() :: ?REP_LOCAL | ?REP_REMOTE).

-define(DEF_DELIMITER, <<"/">>).

-ifdef(EUNIT).
-define(output_warn(Fun, _Key, _MSG), ok).
-else.
-define(output_warn(Fun, _Key, _MSG),
        ?warn(Fun, [{key, _Key}, {cause, _MSG}])).
-endif.


%%--------------------------------------------------------------------
%% API - GET
%%--------------------------------------------------------------------
%% @doc get object (from storage-node#1).
%%
-spec(get(RefAndKey) ->
             {ok, reference(), binary(), binary(), binary()} |
             {error, reference(), any()} when RefAndKey::{reference(), binary()}).
get({Ref, Key}) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_GET),
    case leo_redundant_manager_api:get_redundancies_by_key(get, Key) of
        {ok, #redundancies{id = AddrId}} ->
            IsForcedCheck = true,
            case get_fun(AddrId, Key, IsForcedCheck) of
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
-spec(get(ReadParams, Redundancies) ->
             {ok, #?METADATA{}, binary()} |
             {ok, match} |
             {error, any()} when ReadParams::#?READ_PARAMETER{},
                                 Redundancies::[#redundant_node{}]).
get(#?READ_PARAMETER{num_of_replicas = NumOfReplicas} = ReadParameter, Redundancies)
  when NumOfReplicas > 0 andalso Redundancies /= [] ->
    ok = leo_metrics_req:notify(?STAT_COUNT_GET),
    read_and_repair(ReadParameter, Redundancies);

get(#?READ_PARAMETER{addr_id = AddrId} = ReadParameter,_Redundancies) ->
    case leo_redundant_manager_api:get_redundancies_by_addr_id(get, AddrId) of
        {ok, #redundancies{nodes = Redundancies,
                           n = NumOfReplicas,
                           r = ReadQuorum}} ->
            get(ReadParameter#?READ_PARAMETER{num_of_replicas = NumOfReplicas,
                                              quorum = ReadQuorum},
                Redundancies);
        _Error ->
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCY}
    end;

get(ReadParameter, Redundancies) ->
    {ok, ReadParameter_1} = leo_storage_transformer:transform_read_parameter(ReadParameter),
    get(ReadParameter_1, Redundancies).


%% @doc Retrieve an object which is requested from gateway.
%%
-spec(get(AddrId, Key, ReqId) ->
             {ok, #?METADATA{}, binary()} |
             {error, any()} when AddrId::integer(),
                                 Key::binary(),
                                 ReqId::integer()).
get(AddrId, Key, ReqId) ->
    get(#?READ_PARAMETER{ref = make_ref(),
                         addr_id   = AddrId,
                         key       = Key,
                         req_id    = ReqId}, []).

%% @doc Retrieve an object which is requested from gateway w/etag.
%%
-spec(get(AddrId, Key, ETag, ReqId) ->
             {ok, #?METADATA{}, binary()} |
             {ok, match} |
             {error, any()} when AddrId::integer(),
                                 Key::binary(),
                                 ETag::integer(),
                                 ReqId::integer()).
get(AddrId, Key, ETag, ReqId) ->
    get(#?READ_PARAMETER{ref = make_ref(),
                         addr_id   = AddrId,
                         key       = Key,
                         etag      = ETag,
                         req_id    = ReqId}, []).

%% @doc Retrieve a part of an object.
%%
-spec(get(AddrId, Key, StartPos, EndPos, ReqId) ->
             {ok, #?METADATA{}, binary()} |
             {ok, match} |
             {error, any()} when AddrId::integer(),
                                 Key::binary(),
                                 StartPos::integer(),
                                 EndPos::integer(),
                                 ReqId::integer()).
get(AddrId, Key, StartPos, EndPos, ReqId) ->
    get(#?READ_PARAMETER{ref = make_ref(),
                         addr_id   = AddrId,
                         key       = Key,
                         start_pos = StartPos,
                         end_pos   = EndPos,
                         req_id    = ReqId}, []).


%% @doc read data (common).
%% @private
%% @private
-spec(get_fun(AddrId, Key, IsForcedCheck) ->
             {ok, #?METADATA{}, #?OBJECT{}} |
             {error, any()} when AddrId::integer(),
                                 Key::binary(),
                                 IsForcedCheck::boolean()).
get_fun(AddrId, Key, IsForcedCheck) ->
    get_fun(AddrId, Key, ?DEF_POS_START, ?DEF_POS_END, IsForcedCheck).

%% @private
-spec(get_fun(AddrId, Key, StartPos, EndPos) ->
             {ok, #?METADATA{}, #?OBJECT{}} |
             {error, any()} when AddrId::integer(),
                                 Key::binary(),
                                 StartPos::integer(),
                                 EndPos::integer()).
get_fun(AddrId, Key, StartPos, EndPos) ->
    get_fun(AddrId, Key, StartPos, EndPos, false).

%% @private
-spec(get_fun(AddrId, Key, StartPos, EndPos, IsForcedCheck) ->
             {ok, #?METADATA{}, #?OBJECT{}} |
             {error, any()} when AddrId::integer(),
                                 Key::binary(),
                                 StartPos::integer(),
                                 EndPos::integer(),
                                 IsForcedCheck::boolean()).
get_fun(AddrId, Key, StartPos, EndPos, IsForcedCheck) ->
    %% Check state of the node
    case leo_watchdog_state:find_not_safe_items(?WD_EXCLUDE_ITEMS) of
        not_found ->
            %% Retrieve the object
            case leo_object_storage_api:get(
                   {AddrId, Key}, StartPos, EndPos, IsForcedCheck) of
                {ok, Metadata, Object} ->
                    {ok, Metadata, Object};
                not_found = Cause ->
                    {error, Cause};
                {error, ?ERROR_LOCKED_CONTAINER} ->
                    {error, unavailable};
                {error, Cause} ->
                    {error, Cause}
            end;
        {ok, ErrorItems} ->
            ?debug("get_fun/4", "error-items:~p", [ErrorItems]),
            {error, unavailable}
    end.


%%--------------------------------------------------------------------
%% API - PUT
%%--------------------------------------------------------------------
%% @doc Insert an object (request from gateway).
%%
-spec(put(ObjAndRef) ->
             {ok, reference(), tuple()} |
             {error, reference(), any()} when ObjAndRef::{#?OBJECT{}, reference()}).
put({Object, Ref}) ->
    Object_1 = leo_object_storage_transformer:transform_object(Object),
    AddrId = Object_1#?OBJECT.addr_id,
    Key = Object_1#?OBJECT.key,

    case Object_1#?OBJECT.del of
        ?DEL_TRUE->
            case leo_object_storage_api:head({AddrId, Key}) of
                {ok, MetaBin} ->
                    case binary_to_term(MetaBin) of
                        #?METADATA{cnumber = 0} ->
                            put_fun(Ref, AddrId, Key, Object_1);
                        #?METADATA{cnumber = CNumber} ->
                            case delete_chunked_objects(CNumber, Key) of
                                ok ->
                                    put_fun(Ref, AddrId, Key, Object_1);
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
            put_fun(Ref, AddrId, Key, Object_1)
    end.


%% @doc Insert an object (request from gateway).
%%
-spec(put(Object, ReqId) ->
             {ok, ETag} | {error, any()} when Object::#?OBJECT{},
                                              ReqId::integer(),
                                              ETag::{etag, integer()}).
put(Object, ReqId) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_PUT),
    Object_1 = leo_object_storage_transformer:transform_object(Object),
    replicate_fun(?REP_LOCAL, ?CMD_PUT, Object_1#?OBJECT.addr_id,
                  Object_1#?OBJECT{method = ?CMD_PUT,
                                   clock  = leo_date:clock(),
                                   req_id = ReqId}).


%% @doc Insert an  object (request from remote-storage-nodes/replicator).
%%
-spec(put(Ref, From, Object, ReqId) ->
             {ok, atom()} |
             {error, any()} when Ref::reference(),
                                 From::pid(),
                                 Object::#?OBJECT{},
                                 ReqId::integer()).
put(Ref, From, Object, ReqId) ->
    Object_1 = leo_object_storage_transformer:transform_object(Object),
    Method = case Object_1#?OBJECT.del of
                 ?DEL_TRUE ->
                     ok = leo_metrics_req:notify(?STAT_COUNT_DEL),
                     ?CMD_DELETE;
                 ?DEL_FALSE ->
                     ok = leo_metrics_req:notify(?STAT_COUNT_PUT),
                     ?CMD_PUT
             end,

    case replicate_fun(?REP_REMOTE, Method, Object_1) of
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
-spec(put_fun(Ref, AddrId, Key, Object) ->
             {ok, reference(), tuple()} |
             {error, reference(), any()} when Ref::reference(),
                                              AddrId::integer(),
                                              Key::binary(),
                                              Object::#?OBJECT{}).
put_fun(Ref, AddrId, Key, #?OBJECT{del = ?DEL_TRUE} = Object) ->
    %% Check state of the node
    case leo_watchdog_state:find_not_safe_items(?WD_EXCLUDE_ITEMS) of
        not_found ->
            %% Set deletion-flag to the object
            case leo_object_storage_api:delete({AddrId, Key}, Object) of
                ok ->
                    {ok, Ref, {etag, 0}};
                {error, ?ERROR_LOCKED_CONTAINER} ->
                    {error, Ref, unavailable};
                {error, Cause} ->
                    {error, Ref, Cause}
            end;
        {ok, ErrorItems} ->
            ?debug("put_fun/4", "error-items:~p", [ErrorItems]),
            {error, Ref, unavailable}
    end;
put_fun(Ref, AddrId, Key, Object) ->
    %% Check state of the node
    case leo_watchdog_state:find_not_safe_items(?WD_EXCLUDE_ITEMS) of
        not_found ->
            %% Put the object to the local object-storage
            case leo_object_storage_api:put({AddrId, Key}, Object) of
                {ok, ETag} ->
                    {ok, Ref, {etag, ETag}};
                {error, ?ERROR_LOCKED_CONTAINER} ->
                    {error, Ref, unavailable};
                {error, Cause} ->
                    {error, Ref, Cause}
            end;
        {ok, ErrorItems} ->
            ?debug("put_fun/4", "error-items:~p", [ErrorItems]),
            {error, Ref, unavailable}
    end.



%% Remove chunked objects from the object-storage
%% @private
-spec(delete_chunked_objects(CIndex, ParentKey) ->
             ok | {error, any()} when CIndex::integer(),
                                      ParentKey::binary()).
delete_chunked_objects(0,_) ->
    ok;
delete_chunked_objects(CIndex, ParentKey) ->
    IndexBin = list_to_binary(integer_to_list(CIndex)),
    Key    = << ParentKey/binary, "\n", IndexBin/binary >>,
    AddrId = leo_redundant_manager_chash:vnode_id(Key),

    case delete(#?OBJECT{addr_id   = AddrId,
                         key       = Key,
                         cindex    = CIndex,
                         clock     = leo_date:clock(),
                         timestamp = leo_date:now(),
                         del       = ?DEL_TRUE
                        }, 0) of
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
-spec(delete(ObjAndRef) ->
             {ok, reference()} |
             {error, reference(), any()} when ObjAndRef::{#?OBJECT{}, reference()}).
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
                    case leo_object_storage_api:delete(
                           {AddrId, Key}, Object#?OBJECT{data  = <<>>,
                                                         dsize = 0,
                                                         del   = ?DEL_TRUE}) of
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
-spec(delete(Object, ReqId) ->
             ok | {error, any()} when Object::#?OBJECT{},
                                      ReqId::integer()|reference()).
delete(Object, ReqId) ->
    delete(Object, ReqId, true).

-spec(delete(Object, ReqId, CheckUnderDir) ->
             ok | {error, any()} when Object::#?OBJECT{},
                                      ReqId::integer()|reference(),
                                      CheckUnderDir::boolean()).
delete(Object, ReqId, CheckUnderDir) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_DEL),
    case replicate_fun(?REP_LOCAL, ?CMD_DELETE,
                       Object#?OBJECT.addr_id,
                       Object#?OBJECT{method = ?CMD_DELETE,
                                      data   = <<>>,
                                      dsize  = 0,
                                      clock  = leo_date:clock(),
                                      req_id = ReqId,
                                      del    = ?DEL_TRUE}) of
        {ok,_} ->
            delete_1(ok, Object, CheckUnderDir);
        {error, not_found = Cause} ->
            delete_1({error, Cause}, Object, CheckUnderDir);
        {error, Cause} ->
            {error, Cause}
    end.

%% @private
delete_1(Ret,_Object, false) ->
    Ret;
delete_1(Ret, Object, true) ->
    ok = leo_storage_handler_directory:delete_objects_under_dir(Object),
    Ret.


%%--------------------------------------------------------------------
%% API - HEAD
%%--------------------------------------------------------------------
%% @doc retrieve a meta-data from mata-data-server (file).
%%
-spec(head(AddrId, Key) ->
             {ok, #?METADATA{}} | not_found | {error, any} when AddrId::integer(),
                                                                Key::binary()).
head(AddrId, Key) ->
    %% Do retry when being invoked as usual method
    head(AddrId, Key, true).

-spec(head(AddrId, Key, CanRetry) ->
             {ok, #?METADATA{}} | {error, any} when AddrId::integer(),
                                                    Key::binary(),
                                                    CanRetry::boolean()).
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
-spec(head_with_calc_md5(AddrId, Key, MD5Context) ->
             {ok, #?METADATA{}, any()} |
             {error, any()} when AddrId::integer(),
                                 Key::binary(),
                                 MD5Context::any()).
head_with_calc_md5(AddrId, Key, MD5Context) ->
    leo_object_storage_api:head_with_calc_md5({AddrId, Key}, MD5Context).


%%--------------------------------------------------------------------
%% API - COPY/STACK-SEND/RECEIVE-STORE
%%--------------------------------------------------------------------
%% @doc Replicate an object, which is requested from remote-cluster
%%
-spec(replicate(Object) ->
             ok |
             {ok, reference()} |
             {error, reference()|any()} when Object::#?OBJECT{}).
replicate(Object) ->
    %% Transform an object to a metadata
    Metadata = leo_object_storage_transformer:object_to_metadata(Object),
    Method = case Object#?OBJECT.del of
                 ?DEL_TRUE  -> ?CMD_DELETE;
                 ?DEL_FALSE -> ?CMD_PUT
             end,
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
            case get_active_redundancies(Quorum_2, Redundancies_1) of
                {ok, Redundancies_2} ->
                    leo_storage_replicator:replicate(Method, Quorum_2, Redundancies_2,
                                                     Object, replicate_callback());
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Replicate an object from local to remote
%%
-spec(replicate(DestNodes, AddrId, Key) ->
             ok |
             not_found |
             {error, any()} when DestNodes::[atom()],
                                 AddrId::integer(),
                                 Key::binary()).
replicate(DestNodes, AddrId, Key) ->
    case leo_object_storage_api:head({AddrId, Key}) of
        {ok, MetaBin} ->
            Ref = make_ref(),
            case binary_to_term(MetaBin) of
                #?METADATA{del = ?DEL_FALSE} = Metadata ->
                    case ?MODULE:get({Ref, Key}) of
                        {ok, Ref, Metadata, Bin} ->
                            leo_storage_event_notifier:replicate(DestNodes, Metadata, Bin);
                        {error, Ref, Cause} ->
                            {error, Cause};
                        _Other ->
                            {error, invalid_response}
                    end;
                #?METADATA{del = ?DEL_TRUE} = Metadata ->
                    leo_storage_event_notifier:replicate(DestNodes, Metadata, <<>>);
                _ ->
                    {error, invalid_data_type}
            end;
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% INNNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Retrieve active redundancies
%% @private
-spec(get_active_redundancies(Quorum, Redundancies) ->
             {ok, [#redundant_node{}]} |
             {error, any()} when Quorum::non_neg_integer(),
                                 Redundancies::[#redundant_node{}]).
get_active_redundancies(_, []) ->
    {error, ?ERROR_NOT_SATISFY_QUORUM};
get_active_redundancies(Quorum, Redundancies) ->
    AvailableNodes = [RedundantNode ||
                         #redundant_node{available = true} = RedundantNode <- Redundancies],
    case (Quorum =< erlang:length(AvailableNodes)) of
        true ->
            {ok, AvailableNodes};
        false ->
            {error, ?ERROR_NOT_SATISFY_QUORUM}
    end.


%% @doc read reapir - compare with remote-node's meta-data.
%% @private
-spec(read_and_repair(ReadParams, Redundancies) ->
             {ok, #?METADATA{}, binary()} |
             {ok, match} |
             {error, any()} when ReadParams::#?READ_PARAMETER{},
                                 Redundancies::[#redundant_node{}]).
read_and_repair(#?READ_PARAMETER{quorum = Q} = ReadParams, Redundancies) ->
    case get_active_redundancies(Q, Redundancies) of
        {ok, AvailableNodes} ->
            read_and_repair_1(ReadParams, AvailableNodes, AvailableNodes, []);
        Error ->
            Error
    end.

%% @private
read_and_repair_1(_,[],_,[Error|_]) ->
    {error, Error};
read_and_repair_1(ReadParams, [Node|Rest], AvailableNodes, Errors) ->
    case read_and_repair_2(ReadParams, Node, AvailableNodes) of
        {error, Cause} ->
            read_and_repair_1(ReadParams, Rest, AvailableNodes, [Cause|Errors]);
        Ret ->
            Ret
    end.

%% @private
-spec(read_and_repair_2(ReadParams, Redundancies, Redundancies) ->
             {ok, #?METADATA{}, binary()} |
             {ok, match} |
             {error, any()} when ReadParams::#?READ_PARAMETER{},
                                 Redundancies::[atom()]).
read_and_repair_2(_, [], _) ->
    {error, not_found};
read_and_repair_2(#?READ_PARAMETER{addr_id   = AddrId,
                                   key       = Key,
                                   etag      = 0,
                                   start_pos = StartPos,
                                   end_pos   = EndPos} = ReadParameter,
                  #redundant_node{node = Node}, Redundancies) when Node == erlang:node() ->
    read_and_repair_3(get_fun(AddrId, Key, StartPos, EndPos), ReadParameter, Redundancies);

read_and_repair_2(#?READ_PARAMETER{addr_id   = AddrId,
                                   key       = Key,
                                   etag      = ETag,
                                   start_pos = StartPos,
                                   end_pos   = EndPos} = ReadParameter,
                  #redundant_node{node = Node}, Redundancies) when Node == erlang:node() ->
    %% Retrieve an head of object,
    %%     then compare it with requested 'Etag'
    HeadRet =
        case leo_object_storage_api:head({AddrId, Key}) of
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
    case HeadRet of
        {ok, match} = Reply ->
            Reply;
        _ ->
            read_and_repair_3(
              get_fun(AddrId, Key, StartPos, EndPos), ReadParameter, Redundancies)
    end;

read_and_repair_2(ReadParameter, #redundant_node{node = Node}, Redundancies) ->
    Ref = make_ref(),
    Key = ReadParameter#?READ_PARAMETER.key,

    RPCKey = rpc:async_call(Node, ?MODULE, get, [{Ref, Key}]),
    RetRPC = case catch rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
                 {'EXIT', Cause} ->
                     {error, Cause};
                 {value, {ok, Ref, Meta, Bin}} ->
                     {ok, Meta, #?OBJECT{data = Bin}};
                 {value, {error, Ref, Cause}} ->
                     {error, Cause};
                 {value, {badrpc, Cause}} ->
                     {error, Cause};
                 {value, _} ->
                     {error, invalid_access};
                 timeout = Cause ->
                     {error, Cause}
             end,
    read_and_repair_3(RetRPC, ReadParameter, Redundancies).

%% @private
read_and_repair_3({ok, Metadata, #?OBJECT{data = Bin}}, #?READ_PARAMETER{}, []) ->
    {ok, Metadata, Bin};
read_and_repair_3({ok, Metadata, #?OBJECT{data = Bin}}, #?READ_PARAMETER{num_of_replicas = 1},_Redundancies) ->
    {ok, Metadata, Bin};
read_and_repair_3({ok, match} = Reply, #?READ_PARAMETER{},_Redundancies) ->
    Reply;
read_and_repair_3({ok, Metadata, #?OBJECT{data = Bin}},
                  #?READ_PARAMETER{quorum = Quorum} = ReadParameter, Redundancies) ->
    Fun = fun(ok) ->
                  {ok, Metadata, Bin};
             ({error,_Cause}) ->
                  {error, ?ERROR_RECOVER_FAILURE}
          end,
    ReadParameter_1 = ReadParameter#?READ_PARAMETER{quorum = Quorum},
    leo_storage_read_repairer:repair(ReadParameter_1, Redundancies, Metadata, Fun);

read_and_repair_3({error, not_found = Cause}, #?READ_PARAMETER{key = _K}, _Redundancies) ->
    {error, Cause};
read_and_repair_3({error, timeout = Cause}, #?READ_PARAMETER{key = _K}, _Redundancies) ->
    ?output_warn("read_and_repair_3/3", _K, Cause),
    {error, Cause};
read_and_repair_3({error, Cause}, #?READ_PARAMETER{key = _K}, _Redundancies) ->
    ?output_warn("read_and_repair_3/3", _K, Cause),
    {error, Cause};
read_and_repair_3(_,_,_) ->
    {error, invalid_request}.


%% @doc Replicate an object from local-node to remote node
%% @private
-spec(replicate_fun(replication(), request_verb(), integer(), #?OBJECT{}) ->
             {ok, ETag} | {error, any()} when ETag::{etag, integer()}).
replicate_fun(?REP_LOCAL, Method, AddrId, Object) ->
    %% Check state of the node
    case leo_watchdog_state:find_not_safe_items(?WD_EXCLUDE_ITEMS) of
        not_found ->
            case leo_redundant_manager_api:get_redundancies_by_addr_id(put, AddrId) of
                {ok, #redundancies{nodes     = Redundancies,
                                   w         = WriteQuorum,
                                   d         = DeleteQuorum,
                                   ring_hash = RingHash}} ->
                    Quorum = ?quorum(Method, WriteQuorum, DeleteQuorum),
                    case get_active_redundancies(Quorum, Redundancies) of
                        {ok, Redundancies_1} ->
                            leo_storage_replicator:replicate(
                              Method, Quorum, Redundancies_1, Object#?OBJECT{ring_hash = RingHash},
                              replicate_callback(Object));
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error,_Cause} ->
                    {error, ?ERROR_COULD_NOT_GET_REDUNDANCY}
            end;
        {ok, ErrorItems}->
            ?debug("replicate_fun/4", "error-items:~p", [ErrorItems]),
            {error, unavailable}
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
        %% for put-operation
        {ok, Ref, Checksum} ->
            ok = leo_storage_event_notifier:operate(Method, Object),
            {ok, Checksum};
        %% for delete-operation
        {ok, Ref} ->
            ok = leo_storage_event_notifier:operate(Method, Object),
            {ok, 0};
        {error, Ref, not_found = Cause} ->
            {error, Cause};
        {error, Ref, unavailable = Cause} ->
            {error, Cause};
        {error, Ref, Cause} ->
            ?warn("replicate_fun/3", [{cause, Cause}]),
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
    fun({ok, ?CMD_PUT = Method, Checksum}) ->
            leo_storage_event_notifier:operate(Method, Object),
            {ok, Checksum};
       ({ok,?CMD_DELETE = Method,_Checksum}) ->
            leo_storage_event_notifier:operate(Method, Object),
            {ok, 0};
       ({error, Errors}) ->
            case catch lists:keyfind(not_found, 2, Errors) of
                {'EXIT',_} ->
                    {error, ?ERROR_REPLICATE_FAILURE};
                false ->
                    {error, ?ERROR_REPLICATE_FAILURE};
                _ ->
                    {error, not_found}
            end
    end.
