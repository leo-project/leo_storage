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
-module(leo_storage_read_repair_server).

-author('Yosuke Hara').
-vsn('0.9.0').

-behaviour(gen_server).

-include("leo_storage.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").

%% API
-export([start_link/1, stop/1, repair/6]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([compare/3, loop/4]).

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
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link(Id) ->
    gen_server:start_link({local, Id}, ?MODULE, [], []).

stop(Id) ->
    gen_server:call(Id, stop).


%% @doc Repair an object.
%% @end
-spec(repair(atom(), reference(), integer(), list(), #metadata{}, integer()) ->
             {ok, reference()} | {error, reference(),  any()}).
repair(Id, Ref, ReadQuorum, Nodes, Metadata, ReqId) ->
    gen_server:call(Id, {repair, Ref, ReadQuorum, Nodes, Metadata, ReqId}).


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

handle_call(stop,_From,State) ->
    {stop, normal, ok, State};

handle_call({repair, Ref, ReadQuorum, Nodes,  Metadata, ReqId}, From, State) ->
    spawn(?MODULE, compare, [From, Ref, #req_params{read_quorum  = ReadQuorum,
                                                    redundancies = Nodes,
                                                    metadata     = Metadata,
                                                    req_id       = ReqId}]),
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
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc
%%
-spec(compare(pid(), reference(), #req_params{}) ->
             ok).
compare(From, Ref, #req_params{read_quorum  = ReadQuorum,
                               redundancies = Nodes,
                               metadata     = #metadata{addr_id = AddrId,
                                                        key      = Key},
                               req_id       = ReqId} = ReqParams) ->
    Pid = spawn(?MODULE, loop, [From, ReadQuorum, erlang:length(Nodes),
                                {Ref, ReqId, Key, []}]),
    lists:foreach(
      fun({Node, true}) ->
              spawn(fun() ->
                            RPCKey = rpc:async_call(
                                       Node, leo_storage_handler_object, head, [AddrId, Key]),
                            compare(Pid, RPCKey, Node, ReqParams)
                    end);
         ({_, false}) ->
              void
      end, Nodes).

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
    Pid ! Ret.


loop(From, 0,_NumOfNodes, {Ref,_,_,_Errors}) ->
    gen_server:reply(From, {ok, Ref});

loop(From, R, NumOfNodes, {Ref,_,_, Errors})
  when (NumOfNodes - R) < length(Errors) ->
    gen_server:reply(From, {error, Ref, Errors});

loop(From, R, NumOfNodes, {Ref, ReqId, Key, Errors} = Args) ->
    receive
        ok ->
            loop(From, R-1, NumOfNodes, Args);
        {error, Node, Cause} ->
            loop(From, R,   NumOfNodes, {Ref, ReqId, Key, [{Node, Cause}|Errors]})
    after
        ?DEF_TIMEOUT ->
            case (R >= 0) of
                true  -> gen_server:reply(From, {error, Ref, timeout});
                false -> void
            end
    end.

enqueue(AddrId, Key) ->
    leo_storage_mq_client:publish(
      ?QUEUE_TYPE_INCONSISTENT_DATA, AddrId, Key, ?ERR_TYPE_RECOVER_DATA).

