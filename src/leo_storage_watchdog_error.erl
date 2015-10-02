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
%%======================================================================
-module(leo_storage_watchdog_error).

-include("leo_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([push/1]).


%% @doc Push an error message to the watchdog-error
-spec(push(ErrorMsg) ->
             ok | {error, any()} when ErrorMsg::term()).
push(ErrorMsg) ->
    ?debugVal(ErrorMsg),
    FormattedMsg = ErrorMsg,
    push_1(FormattedMsg).

%% @private
push_1(FormattedMsg) ->
    ?debugVal(FormattedMsg),
    leo_watchdog_collector:push(FormattedMsg).
