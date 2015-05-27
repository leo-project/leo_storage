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
%% @doc Directory operation's Event
%% @reference https://github.com/leo-project/leo_storage/blob/master/src/leo_directory_event.erl
%% @end
%%======================================================================
-module(leo_directory_event).

-author('Yosuke Hara').

-behaviour(gen_event).

-include("leo_storage.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% gen_server callbacks
-export([init/1,
         handle_event/2,
         handle_call/2,
         handle_info/2,
         code_change/3,
         terminate/2]).


%%====================================================================
%% GEN_EVENT CALLBACKS
%%====================================================================
init([]) ->
    {ok, []}.

handle_event({Method, Metadata}, State) when Method == put;
                                             Method == delete ->
    _ = leo_directory_sync:append(async, Metadata),
    {ok, State};
handle_event(_, State) ->
    {ok, State}.

handle_call(_, State) ->
    {ok, ok, State}.

handle_info(_, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.


%%====================================================================
%% Inner Functions
%%====================================================================
