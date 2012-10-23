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

-export([replicate/4, loop/3]).

-type(error_msg_type() :: ?ERR_TYPE_REPLICATE_DATA  |
                          ?ERR_TYPE_DELETE_DATA).

-record(req_params, {pid      :: pid(),
                     rpc_key  :: rpc:key(),
                     addr_id  :: integer(),
                     key      :: string(),
                     obj_pool :: pid(),
                     req_id   :: integer()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Replicate an object to local-node and remote-nodes.
%%
-spec(replicate(pos_integer(), list(), pid(), function()) ->
             {ok, reference()} | {error, {reference(), any()}}).
replicate(Quorum, Nodes, ObjPoolPid, Callback) ->
    case catch leo_object_storage_pool:get(ObjPoolPid) of
        {'EXIT', _Cause} ->
            Callback({error, timeout});
        undefined ->
            Callback({error, timeout});

        #object{addr_id = AddrId, key = Key, req_id = ReqId} = Object ->
            From = self(),
            Pid  = spawn(?MODULE, loop, [Quorum, From, {length(Nodes), [], []}]),
            ReqParams = #req_params{pid     = Pid,
                                    addr_id = AddrId,
                                    key     = Key,
                                    req_id  = ReqId},
            lists:foreach(
              fun({Node, true}) when Node == erlang:node() ->
                      spawn(fun() ->
                                    replicate_fun(local, ReqParams#req_params{obj_pool = ObjPoolPid})
                            end);
                 ({Node, true}) ->
                      spawn(fun() ->
                                    RPCKey = rpc:async_call(Node, leo_storage_handler_object, put, [Object]),
                                    replicate_fun(Node, ReqParams#req_params{rpc_key = RPCKey})
                            end);
                 ({Node, false}) ->
                      enqueue(?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
                      Pid ! {error, Node, nodedown}
              end, Nodes),

            loop(Callback)
    end.


%% @doc Waiting for messages (replication)
%%
-spec(loop(integer(), function(), {integer(), string(), integer(), list()}) ->
             ok).
loop(0, From, {_NumOfNodes,_ResL,_Errors}) ->
    From ! {ok, hd(_ResL)};

loop(W, From, { NumOfNodes,_ResL, Errors}) when (NumOfNodes - W) < length(Errors) ->
    From ! {error, Errors};

loop(W, From, { NumOfNodes, ResL, Errors} = Args) ->
    receive
        ok ->
            loop(W-1, From, Args);
        {ok, Checksum} ->
            loop(W-1, From, {NumOfNodes, [Checksum|ResL], Errors});
        {error, Node, Cause} ->
            loop(W,   From, {NumOfNodes, ResL, [{Node, Cause}|Errors]})
    after
        ?DEF_REQ_TIMEOUT ->
            case (W >= 0) of
                true ->
                    From ! {error, timeout};
                false ->
                    void
            end
    end.

%% @doc Waiting for messages (result)
%%
-spec(loop(function()) ->
             any()).
loop(Callback) ->
    receive
        Res ->
            Callback(Res)
    after ?DEF_REQ_TIMEOUT ->
            Callback({error, timeout})
    end.


%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Request a replication-message for local-node.
%%
-spec(replicate_fun(local | atom(), #req_params{}) ->
             {ok, atom()} | {error, atom(), any()}).
replicate_fun(local, #req_params{pid      = Pid,
                                 addr_id  = AddrId,
                                 key      = Key,
                                 obj_pool = ObjPool,
                                 req_id   = ReqId}) ->
    Ref  = make_ref(),
    Node = erlang:node(),
    Ret  = case leo_storage_handler_object:put(local, ObjPool, Ref) of
               {ok, Ref, Checksum} ->
                   {ok, Checksum};
               {error, Ref, Cause} ->
                   ?warn("replicate_fun/2", "key:~s, node:~w, reqid:~w, cause:~p",
                         [Key, local, ReqId, Cause]),
                   enqueue(?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
                   {error, Node, Cause}
           end,

    catch leo_object_storage_pool:destroy(ObjPool),
    Pid ! Ret;

%% @doc Request a replication-message for remote-node.
%%
replicate_fun(_Node, #req_params{pid     = Pid,
                                 rpc_key = RPCKey,
                                 addr_id = AddrId,
                                 key     = Key}) ->
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
              {value, {ok, Checksum}} ->
                  {ok, Checksum};
              {value, {error, Cause}} ->
                  {error, Cause};
              {value, {badrpc, Cause}} ->
                  {error, Cause};
              timeout = Cause ->
                  {error, Cause}
          end,

    case Ret of
        {error, _Reason} ->
            enqueue(?ERR_TYPE_REPLICATE_DATA, AddrId, Key);
        _ ->
            void
    end,
    Pid ! Ret.


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

