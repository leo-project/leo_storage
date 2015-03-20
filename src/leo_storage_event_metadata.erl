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
%% @doc Metadata Operation's Event
%% @reference https://github.com/leo-project/leo_storage/blob/master/src/leo_storage_event_metadata.erl
%% @end
%%======================================================================
-module(leo_storage_event_metadata).

-author('Yosuke Hara').

-behaviour(gen_event).

-include("leo_storage.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
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

handle_event({Verb, #?METADATA{key = Key} = Metadata}, State) when Verb == ?CMD_PUT;
                                                                   Verb == ?CMD_DELETE ->
    Dir = get_dir(Key),
    case leo_redundant_manager_api:get_redundancies_by_key(Dir) of
        {ok, #redundancies{nodes = RedundantNodes}} ->
            %% Store/Remove the metadata into the metadata-cluster
            case [Node || #redundant_node{available = true,
                                          node = Node} <- RedundantNodes] of
                [] ->
                    enqueue(Dir, Metadata);
                ActiveNodes ->
                    append_metadata(ActiveNodes, Dir, Metadata)
            end;
        {error,_Cause} ->
            enqueue(Dir, Metadata)
    end,
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
%% @doc Retrieve a directory from the key
%% @private
get_dir(Key) ->
    BinSlash = <<"/">>,
    case binary:matches(Key, [BinSlash],[]) of
        [] ->
            Key;
        RetL ->
            {Len,_} = lists:last(RetL),
            binary:part(Key, 0, Len)
    end.


%% @doc Append a metadata to the container
%% @private
append_metadata([],_,_) ->
    ok;
append_metadata([Node|Rest], Dir, Metadata) ->
    case leo_storage_handler_directory:append(Node, Dir, Metadata) of
        ok ->
            ok;
        _ ->
            enqueue(Dir, Metadata)
    end,
    append_metadata(Rest, Dir, Metadata).


%% @doc enqueue a message of a miss-replication into the queue
%% @private
enqueue(Dir, Metadata) ->
    %% @TODO
    ?debugVal({Dir, Metadata}),
    ok.
