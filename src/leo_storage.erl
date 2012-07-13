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
%% LeoFS Storage
%% @doc
%% @end
%%======================================================================
-module(leo_storage).

-author('Yosuke Hara').
-vsn('0.9.1').

%% Application and Supervisor callbacks
-export([start/0, stop/0]).

%%----------------------------------------------------------------------
%% Application behaviour callbacks
%%----------------------------------------------------------------------
start() ->
    ok = application:start(crypto),
    ok = application:start(leo_storage).


stop() ->
    application:stop(mnesia),
    application:stop(crypto),
    application:stop(leo_storage),
    init:stop().

%% ---------------------------------------------------------------------
%% Internal-Functions
%% ---------------------------------------------------------------------
