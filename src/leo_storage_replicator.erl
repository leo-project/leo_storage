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
%% LeoFS - Replicator.
%% @doc
%% @end
%%======================================================================
-module(leo_storage_replicator).

-author('Yosuke Hara').

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([replicate/4]).

-type(error_msg_type() :: ?ERR_TYPE_REPLICATE_DATA  |
                          ?ERR_TYPE_DELETE_DATA).

-record(req_params, {pid     :: pid(),
                     addr_id :: integer(),
                     key     :: string(),
                     object  :: #object{},
                     req_id  :: integer()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Replicate an object to local-node and remote-nodes.
%%
-spec(replicate(pos_integer(), list(), #object{}, function()) ->
             {ok, reference()} | {error, {reference(), any()}}).
replicate(Quorum, Nodes, Object, Callback) ->
    AddrId = Object#object.addr_id,
    Key    = Object#object.key,
    ReqId  = Object#object.req_id,
    From = self(),

    lists:foreach(
      fun({Node, true}) when Node == erlang:node() ->
              spawn(fun() ->
                            replicate_fun(local, #req_params{pid     = From,
                                                             addr_id = AddrId,
                                                             key     = Key,
                                                             object  = Object,
                                                             req_id  = ReqId})
                    end);
         ({Node, true}) ->
              true = rpc:cast(Node, leo_storage_handler_object, put, [From, Object, ReqId]);
         ({Node, false}) ->
              ok = enqueue(?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
              erlang:send(From, {error, {Node, nodedown}})
      end, Nodes),

    loop(Quorum, From, AddrId, Key, {length(Nodes), [], []}, Callback).

%% @doc Waiting for messages (replication)
%%
-spec(loop(integer(), function(), integer(), binary(),
           {integer(), string(), integer(), list()}, function()) ->
             ok).
loop(0,_From,_AddrId,_Key, {_NumOfNodes,_ResL,_Errors}, Callback) ->
    Callback({ok, hd(_ResL)});
loop(W,_From,_AddrId,_Key, { NumOfNodes,_ResL, Errors}, Callback) when (NumOfNodes - W) < length(Errors) ->
    Callback({error, Errors});

loop(W, From, AddrId, Key, { NumOfNodes, ResL, Errors}, Callback) ->
    receive
        {ok, Checksum} ->
            loop(W-1, From, AddrId, Key, {NumOfNodes, [Checksum|ResL], Errors}, Callback);
        {error, {Node, Cause}} ->
            ok = enqueue(?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
            loop(W,   From, AddrId, Key, {NumOfNodes, ResL, [{Node, Cause}|Errors]}, Callback)
    after
        ?DEF_REQ_TIMEOUT ->
            case (W >= 0) of
                true ->
                    Callback({error, timeout});
                false ->
                    void
            end
    end.


%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Request a replication-message for local-node.
%%
-spec(replicate_fun(local | atom(), #req_params{}) ->
             {ok, atom()} | {error, atom(), any()}).
replicate_fun(local, #req_params{pid     = Pid,
                                 addr_id = AddrId,
                                 key     = Key,
                                 object  = Object,
                                 req_id  = ReqId}) ->
    Ref  = make_ref(),
    Ret  = case leo_storage_handler_object:put(Object, Ref) of
               {ok, Ref, Checksum} ->
                   {ok, Checksum};
               {error, Ref, Cause} ->
                   ?warn("replicate_fun/2", "key:~s, node:~w, reqid:~w, cause:~p",
                         [Key, local, ReqId, Cause]),
                   ok = enqueue(?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
                   {error, {node(), Cause}}
           end,
    erlang:send(Pid, Ret).


%% @doc Input a message into the queue.
%%
-spec(enqueue(error_msg_type(), integer(), string()) ->
             ok | void).
enqueue(?ERR_TYPE_REPLICATE_DATA = Type,  AddrId, Key) ->
    leo_storage_mq_client:publish(?QUEUE_TYPE_REPLICATION_MISS, AddrId, Key, Type);
enqueue(?ERR_TYPE_DELETE_DATA = Type,     AddrId, Key) ->
    leo_storage_mq_client:publish(?QUEUE_TYPE_REPLICATION_MISS, AddrId, Key, Type);
enqueue(_,_,_) ->
    void.

