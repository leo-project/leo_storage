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
%% LeoFS Storage - Read Repair Server.
%% @doc
%% @end
%%======================================================================
-module(leo_storage_read_repairer).

-author('Yosuke Hara').

-include("leo_storage.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").

%% API
-export([repair/5]).

-record(req_params, {addr_id = 0       :: integer(),
                     key               :: string(),
                     read_quorum = 0   :: integer(),
                     redundancies = [] :: list(),
                     metadata          :: #metadata{},
                     rpc_key           :: rpc:key(),
                     req_id = 0        :: integer()
                    }).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Repair an object.
%% @end
-spec(repair(pos_integer(), list(), #metadata{}, integer(), function()) ->
             {ok, reference()} | {error, reference(),  any()}).
repair(ReadQuorum, Nodes, Metadata, ReqId, Callback) ->
    From   = self(),
    AddrId = Metadata#metadata.addr_id,
    Key    = Metadata#metadata.key,
    Params = #req_params{read_quorum  = ReadQuorum,
                         redundancies = Nodes,
                         metadata     = Metadata,
                         req_id       = ReqId},

    lists:foreach(
      fun({Node, true}) ->
              spawn(fun() ->
                            RPCKey = rpc:async_call(
                                       Node, leo_storage_handler_object, head, [AddrId, Key]),
                            compare(From, RPCKey, Node, Params)
                    end);
         ({_, false}) ->
              void
      end, Nodes),

    loop(ReadQuorum, From, erlang:length(Nodes), {ReqId, Key, []}, Callback).


%% @doc Waiting for messages (compare a metadata)
%%
-spec(loop(integer(), pid(), list(), tuple(), function()) ->
             ok).
loop(0,_From,_NumOfNodes, {_,_,_Errors}, Callback) ->
    Callback(ok);
loop(R,_From, NumOfNodes, {_,_, Errors}, Callback) when (NumOfNodes - R) < length(Errors) ->
    Callback({error, Errors});

loop(R, From, NumOfNodes, {ReqId, Key, Errors} = Args, Callback) ->
    receive
        ok ->
            loop(R-1, From, NumOfNodes, Args, Callback);
        {error, {Node, Cause}} ->
            loop(R,   From, NumOfNodes, {ReqId, Key, [{Node, Cause}|Errors]}, Callback)
    after
        ?DEF_REQ_TIMEOUT ->
            case (R >= 0) of
                true ->
                    Callback( {error, timeout});
                false ->
                    void
            end
    end.


%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Compare local-metadata with remote-metadata
%% @private
-spec(compare(pid(), pid(), atom(), #req_params{}) ->
             ok).
compare(Pid, RPCKey, Node, #req_params{metadata = #metadata{addr_id = AddrId,
                                                            key     = Key,
                                                            clock   = Clock}}) ->
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
              {value, {ok, #metadata{clock = RemoteClock}}} when Clock == RemoteClock ->
                  ok;
              {value, {ok, #metadata{clock = RemoteClock}}} when Clock  > RemoteClock ->
                  {error, {Node, secondary_inconsistency}};
              {value, {ok, #metadata{clock = RemoteClock}}} when Clock  < RemoteClock ->
                  {error, {Node, primary_inconsistency}};
              {value, {error, Cause}} ->
                  {error, {Node, Cause}};
              {value, {badrpc, Cause}} ->
                  {error, {Node, Cause}};
              timeout = Cause ->
                  {error, {Node, Cause}}
          end,

    case Ret of
        ok ->
            ok;
        {error, {Node, Reason}} ->
            ?warn("compare/4", "node:~w, vnode-id:~w, key:~s, clock:~w, cause:~p",
                  [Node, AddrId, Key, Clock, Reason]),
            enqueue(AddrId, Key)
    end,
    erlang:send(Pid, Ret).


%% @doc Insert a message into the queue
%% @private
-spec(enqueue(integer(), string()) ->
             ok | {error, any()}).
enqueue(AddrId, Key) ->
    leo_storage_mq_client:publish(
      ?QUEUE_TYPE_INCONSISTENT_DATA, AddrId, Key, ?ERR_TYPE_RECOVER_DATA).

