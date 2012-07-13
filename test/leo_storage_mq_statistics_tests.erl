%%====================================================================
%%
%% LeoFS Storage
%%
%% Copyright (c) 2012
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
%% -------------------------------------------------------------------
%% LeoFS Storage - EUnit
%% @doc
%% @end
%%====================================================================
-module(leo_storage_mq_statistics_tests).
-author('yosuke hara').
-vsn('0.9.1').

-include("leo_storage.hrl").
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

mq_statistics_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun sync_/1
                          ]]}.

setup() ->
    %% gen mock.
    meck:new(leo_logger_api),
    meck:expect(leo_logger_api, new,          fun(_,_,_) -> ok end),
    meck:expect(leo_logger_api, new,          fun(_,_,_,_,_) -> ok end),
    meck:expect(leo_logger_api, new,          fun(_,_,_,_,_,_) -> ok end),
    meck:expect(leo_logger_api, add_appender, fun(_,_) -> ok end),
    meck:expect(leo_logger_api, append,       fun(_,_) -> ok end),
    meck:expect(leo_logger_api, append,       fun(_,_,_) -> ok end),

    meck:new(leo_mq_api),
    meck:expect(leo_mq_api, status, fun(_Id) ->
                                            {ok, {8,8}}
                                    end),
    ok.

teardown(_) ->
    meck:unload(),
    ok.


%% sync vnode-id queue.
sync_(_) ->
    ok = leo_storage_mq_statistics:init(),
    ok = leo_storage_mq_statistics:sync(?STAT_INTERVAL_1M),
    ok = leo_storage_mq_statistics:sync(?STAT_INTERVAL_5M),

    Res = meck:history(leo_mq_api),
    ?assertEqual(4, length(Res)),
    ok.

-endif.
