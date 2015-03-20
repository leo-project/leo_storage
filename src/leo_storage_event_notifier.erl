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
%% Leo Storage - Event Notifier
%%
%% @doc LeoStorage's Evet Notifier
%% @reference https://github.com/leo-project/leo_storage/blob/master/src/leo_storage_event_notifier.erl
%% @end
%%======================================================================
-module(leo_storage_event_notifier).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_storage.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0, stop/0]).
-export([operate/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-record(state, {
          event_pid :: pid()
         }).


%%====================================================================
%% API
%%====================================================================
%% @doc Starts the server
%%
-spec(start_link() ->
             {ok, pid()} | {error, any()}).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% @doc Stop this server
%%
-spec(stop() ->
             ok).
stop() ->
    gen_server:call(?MODULE, stop, ?TIMEOUT).


%% @doc Stop this server
%%
-spec(operate(Method, Data) ->
             ok | {error, any()} when Method::request_verb(),
                                      Data::#?OBJECT{}|#?METADATA{}).
operate(Method, Data) ->
    gen_server:call(?MODULE, {operate, Method, Data}, ?TIMEOUT).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
init([]) ->
    {ok, Pid} = gen_event:start_link(),
    ok = gen_event:add_handler(Pid, leo_storage_event_metadata, []),
    {ok, #state{event_pid = Pid}}.


%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State};

handle_call({operate, Method, #?METADATA{} = Metadata}, _From,
            #state{event_pid = Pid} = State) when Method == ?CMD_PUT;
                                                  Method == ?CMD_DELETE ->
    ok = gen_event:notify(Pid, {Method, Metadata}),
    {reply, ok, State};

handle_call({operate, Method, #?OBJECT{} = Object}, _From,
            #state{event_pid = Pid} = State) when Method == ?CMD_PUT;
                                                  Method == ?CMD_DELETE ->
    Metadata = leo_object_storage_transformer:object_to_metadata(Object),
    ok = leo_sync_remote_cluster:defer_stack(Object),
    ok = gen_event:notify(Pid, {Method, Metadata}),
    {reply, ok, State};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.


%% @doc Handling cast message
%% <p>
%% gen_server callback - Module:handle_cast(Request, State) -> Result.
%% </p>
handle_cast(_Msg, State) ->
    {noreply, State}.


%% @doc Handling all non call/cast messages
%% <p>
%% gen_server callback - Module:handle_info(Info, State) -> Result.
%% </p>
handle_info(_Info, State) ->
    {noreply, State}.


%% @doc This function is called by a gen_server when it is about to
%%      terminate. It should be the opposite of Module:init/1 and do any necessary
%%      cleaning up. When it returns, the gen_server terminates with Reason.
terminate(_Reason,_State) ->
    ok.


%% @doc Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
