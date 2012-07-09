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
-module(leo_storage_replicate_server).

-author('Yosuke Hara').
-vsn('0.9.0').

-behaviour(gen_server).

-include("leo_storage.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").

%% API
-export([start_link/1, stop/1]).
-export([replicate/5, replicate1/2, replicate1/5, loop/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-define(SLASH, "/").
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
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link(Id) ->
    gen_server:start_link({local, Id}, ?MODULE, [], []).

stop(Id) ->
    gen_server:call(Id, stop).

%% @doc Replicate an object to local-node and remote-nodes.
replicate(Id, Ref, Quorum, Nodes, ObjPool) ->
    gen_server:call(Id, {replicate, Ref, Quorum, Nodes, ObjPool}).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([]) ->
    {ok, null}.


%% Function: handle_call(Msg, State) -> {noreply, State}        |
%%                                      {reply, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
handle_call(stop,_From,State) ->
    {stop, normal, ok, State};


handle_call({replicate, Ref, Quorum, Nodes, ObjPool}, From, State) ->
    spawn(?MODULE, replicate1, [From, Ref, Quorum, Nodes, ObjPool]),
    {noreply, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
handle_cast(_Msg, State) ->
    {noreply, State}.

%% Function: handle_info(Info, State) -> {noreply, State}          |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
handle_info(_Info, State) ->
    {noreply, State}.

%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _State) ->
    ok.

%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% OBJECT-OPERATION.
%%--------------------------------------------------------------------
%%
%% for regular-case.
%%
-spec(replicate1(pid(), reference(), integer(), list(), pid()) ->
             ok).
replicate1(From, Ref, Quorum, Nodes, ObjPoolPid) ->
    case catch leo_object_storage_pool:get(ObjPoolPid) of
        {'EXIT', _Cause} ->
            gen_server:reply(From, {error, Ref, timeout});
        undefined ->
            gen_server:reply(From, {error, Ref, timeout});

        #object{addr_id = AddrId, key = Key, req_id = ReqId} = Object ->
            Pid = spawn(?MODULE, loop, [From, Quorum, {Ref, length(Nodes), []}]),
            ReqParams = #req_params{pid     = Pid,
                                    addr_id = AddrId,
                                    key     = Key,
                                    req_id  = ReqId},

            lists:foreach(
              fun({Node, true}) when Node == erlang:node() ->
                      spawn(fun() ->
                                    replicate1(local, ReqParams#req_params{obj_pool = ObjPoolPid})
                            end);
                 ({Node, true}) ->
                      spawn(fun() ->
                                    RPCKey = rpc:async_call(Node, leo_storage_handler_object, put, [Object]),
                                    replicate1(Node, ReqParams#req_params{rpc_key = RPCKey})
                            end);
                 ({Node, false}) ->
                      enqueue(?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
                      Pid ! {error, Node, nodedown}
              end, Nodes)
    end.


%% @doc Request a replication-message for local-node.
%%
-spec(replicate1(local | atom(), #req_params{}) ->
             {ok, atom()} | {error, atom(), any()}).
replicate1(local, #req_params{pid      = Pid,
                              addr_id  = AddrId,
                              key      = Key,
                              obj_pool = ObjPool,
                              req_id   = ReqId}) ->
    Ref  = make_ref(),
    Node = erlang:node(),
    Ret  = case leo_storage_handler_object:put(ObjPool, Ref) of
               {ok, Ref} ->
                   {ok, Node};
               {error, Ref, Cause} ->
                   ?warn("replicate1/2", "key:~s, node:~w, reqid:~w, cause:~p",
                         [Key, local, ReqId, Cause]),
                   enqueue(?ERR_TYPE_REPLICATE_DATA, AddrId, Key),
                   {error, Node, Cause}
           end,

    catch leo_object_storage_pool:destroy(ObjPool),
    Pid ! Ret;

%% @doc Request a replication-message for remote-node.
%%
replicate1(Node, #req_params{pid     = Pid,
                             rpc_key = RPCKey,
                             addr_id = AddrId,
                             key     = Key}) ->
    Ret = case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
        {value, ok} ->
            {ok, Node};
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


%% @doc Waiting for messages.
%%
-spec(loop(pid(), integer(), {reference(), integer(), string(), integer(), list()}) ->
             ok).
loop(From, 0, {Ref,_NumOfNodes,_Errors}) ->
    gen_server:reply(From, {ok, Ref}),
    ok;

loop(From, W, {Ref, NumOfNodes, Errors}) when (NumOfNodes - W) < length(Errors) ->
    gen_server:reply(From, {error, Ref, Errors});

loop(From, W, {Ref, NumOfNodes, Errors} = Args) ->
    receive
        {ok, _Node} ->
            loop(From, W-1, Args);
        {error, Node, Cause} ->
            loop(From, W, {Ref, NumOfNodes, [{Node, Cause}|Errors]})
    after
        ?DEF_TIMEOUT ->
            case (W >= 0) of
                true ->
                    gen_server:reply(From, {error, Ref, timeout});
                false ->
                    void
            end
    end.


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

