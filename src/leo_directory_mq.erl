%%====================================================================
%%
%% LeoFS Directory
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
%% -------------------------------------------------------------------
%% @doc
%% @end
%%====================================================================
-module(leo_directory_mq).

-author('Yosuke Hara').

-behaviour(leo_mq_behaviour).

-include("leo_storage.hrl").
-include_lib("leo_mq/include/leo_mq.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start/1, start/2,
         publish/1]).
-export([init/0, handle_call/1, handle_call/3]).

-define(SLASH, "/").


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc create queues and launch mq-servers.
%%
-spec(start(RootPath) ->
             ok | {error, any()} when RootPath::binary()).
start(RootPath) ->
    start(leo_storage_sup, RootPath).

-spec(start(RefSup, RootPath) ->
             ok | {error, any()} when RefSup::pid(),
                                      RootPath::binary()).
start(RefSup, RootPath) ->
    %% Launch mq-sup under the sup when undedined mq-sup
    RefMQSup =
        case whereis(leo_mq_sup) of
            undefined ->
                ChildSpec = {leo_mq_sup,
                             {leo_mq_sup, start_link, []},
                             permanent, 2000, supervisor, [leo_mq_sup]},
                {ok, Pid} = supervisor:start_child(RefSup, ChildSpec),
                Pid;
            Pid ->
                Pid
        end,

    RootPath_1 =
        case (string:len(RootPath) == string:rstr(RootPath, ?SLASH)) of
            true  -> RootPath;
            false -> RootPath ++ ?SLASH
        end,
    RootPath_2 = lists:append([RootPath_1, "directory/"]),

    %% Launch the mq
    Id = ?MODULE,
    leo_mq_api:new(RefMQSup, Id, [{?MQ_PROP_MOD, ?MODULE},
                                  {?MQ_PROP_FUN, ?MQ_SUBSCRIBE_FUN},
                                  {?MQ_PROP_ROOT_PATH, RootPath_2},
                                  {?MQ_PROP_DB_NAME,   ?env_mq_backend_db()},
                                  {?MQ_PROP_DB_PROCS,  ?env_num_of_mq_procs()},
                                  {?MQ_PROP_BATCH_MSGS_MAX,  ?env_mq_num_of_batch_process_max()},
                                  {?MQ_PROP_BATCH_MSGS_REG,  ?env_mq_num_of_batch_process_reg()},
                                  {?MQ_PROP_INTERVAL_MAX,  ?env_mq_interval_between_batch_procs_max()},
                                  {?MQ_PROP_INTERVAL_REG,  ?env_mq_interval_between_batch_procs_reg()}
                                 ]).


%% @doc Input a message into the queue.
%%
-spec(publish(#?METADATA{}) ->
             ok | {error, any()}).
publish(#?METADATA{addr_id = AddrId,
                   key = Key} = Metadata) ->
    KeyBin = term_to_binary({AddrId, Key}),
    ValBin = term_to_binary(Metadata),
    leo_mq_api:publish(?MODULE, KeyBin, ValBin).


%% -------------------------------------------------------------------
%% Callbacks
%% -------------------------------------------------------------------
%% @doc Initializer
%%
-spec(init() -> ok | {error, any()}).
init() ->
    ok.


%% @doc Subscribe a message from the queue.
%%
-spec(handle_call({consume, atom(), binary()}) ->
             ok | {error, any()}).
handle_call({consume, ?MODULE, MessageBin}) ->
    case catch binary_to_term(MessageBin) of
        #?METADATA{addr_id = AddrId,
                   key = Key} ->
            %% Retrieve latest metadata of the object
            case leo_storage_handler_object:head(AddrId, Key, true) of
                {ok, Metadata_1} ->
                    leo_directory_sync:append(Metadata_1);
                {error, not_found} ->
                    ok;
                _ ->
                    ok
            end;
        _Error ->
            {error, invalid_format}
    end;
handle_call(_) ->
    ok.
handle_call(_,_,_) ->
    ok.
