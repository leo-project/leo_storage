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
%% Leo Storage - Directory Cache
%%
%% @doc LeoStorage's Event Notifier
%% @reference https://github.com/leo-project/leo_storage/blob/master/src/leo_directory_cache.erl
%% @end
%%======================================================================
-module(leo_directory_cache).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0, stop/0]).
-export([append/2,
         get/1,
         delete/1,
         merge/0, merge/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-record(state, {
          directories = [] :: dict()
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
-spec(stop() ->
             ok).
stop() ->
    gen_server:call(?MODULE, stop, ?TIMEOUT).


%% @doc Append a metadata under a dir
-spec(append(Dir, Metadata) ->
             ok | {error, any()} when Dir::binary(),
                                      Metadata::#?METADATA{}).
append(Dir, Metadata) ->
    gen_server:call(?MODULE, {append, Dir, Metadata}, ?TIMEOUT).


%% @doc Retrieve a metadata under a dir
-spec(get(Dir) ->
             {ok, [Metadata]} | not_found | {error, any()} when Dir::binary(),
                                                                Metadata::#?METADATA{}).
get(Dir) ->
    gen_server:call(?MODULE, {get, Dir}, ?TIMEOUT).


%% @doc Remove a metadata under a dir
-spec(delete(Dir) ->
             ok | {error, any()} when Dir::binary()).
delete(Dir) ->
    gen_server:call(?MODULE, {delete, Dir}, ?TIMEOUT).


%% @doc Remove a metadata under a dir
-spec(merge() ->
             ok | {error, any()}).
merge() ->
    gen_server:call(?MODULE, merge, ?TIMEOUT).

-spec(merge(Dir, [Metadata]) ->
             ok | {error, any()} when Dir::binary(),
                                      Metadata::#?METADATA{}).
merge(Dir, MetadataL) ->
    gen_server:call(?MODULE, {merge, Dir, MetadataL}, ?TIMEOUT).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
init([]) ->
    {ok, #state{directories = dict:new()}}.


%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State};


handle_call({append, Dir, #?METADATA{} = Metadata}, _From, #state{directories = DirL} = State) ->
    DirL_1 = dict:append(Dir, Metadata, DirL),
    {reply, ok, State#state{directories = DirL_1}};

handle_call({get, Dir}, _From, #state{directories = DirL} = State) ->
    Reply =
        case (dict:size(DirL) == 0) of
            true ->
                not_found;
            false ->
                case catch dict:find(Dir, DirL) of
                    {ok, Value} ->
                        {ok, Value};
                    error ->
                        not_found;
                    {_, Cause} ->
                        ?error("handle_call/3 - get",
                               [{dir, Dir}, {cause, Cause}]),
                        {error, Cause}
                end
        end,
    {reply, Reply, State};

handle_call({delete, Dir}, _From, #state{directories = DirL} = State) ->
    NewState =
        case (dict:size(DirL) == 0) of
            true ->
                State;
            false ->
                case catch dict:erase(Dir, DirL) of
                    {'EXIT', Cause}->
                        ?error("handle_call/3 - delete",
                               [{dir, Dir}, {cause, Cause}]),
                        State;
                    DirL_1 ->
                        State#state{directories = DirL_1}
                end
        end,
    Reply = leo_cache_api:delete(Dir),
    {reply, Reply, NewState};

handle_call(merge, _From, #state{directories = DirL} = State) ->
    Reply = case (dict:size(DirL) == 0) of
                true ->
                    ok;
                false ->
                    DirL_1 = dict:to_list(DirL),
                    [merge_fun(Dir, MetadataL) || {Dir, MetadataL}  <- DirL_1],
                    ok
            end,
    erlang:garbage_collect(self()),
    {reply, Reply, State#state{directories = dict:new()}};

handle_call({merge, Dir, MetadataL}, _From, #state{directories = DirL} = State) ->
    Reply = case (dict:size(DirL) == 0) of
                true ->
                    merge_fun(Dir, MetadataL);
                false ->
                    case catch dict:find(Dir, DirL) of
                        {ok, Value} ->
                            merge_fun(Dir, lists:append([MetadataL, Value]));
                        error ->
                            merge_fun(Dir, MetadataL);
                        {_, Cause} ->
                            ?error("handle_call/3 - get",
                                   [{dir, Dir}, {cause, Cause}]),
                            {error, Cause}
                    end
            end,

    erlang:garbage_collect(self()),
    DirL_1 = dict:erase(Dir, DirL),
    {reply, Reply, State#state{directories = DirL_1}};

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


%%====================================================================
%% Inner Functions
%%====================================================================
%% @doc Merge metadatas into the cache
%% @private
-spec(merge_fun(Dir, MetadataL) ->
             ok when Dir::binary(),
                     MetadataL::[#?METADATA{}]).
merge_fun(<<>>,_) ->
    ok;
merge_fun(Dir, MetadataL) ->
    case leo_cache_api:get(Dir) of
        {ok, CacheBin} ->
            case binary_to_term(CacheBin) of
                #metadata_cache{metadatas = CachedMetadataL} ->
                    merge_fun(Dir, MetadataL, CachedMetadataL);
                _ ->
                    ok
            end;
        not_found ->
            merge_fun(Dir, MetadataL, []);
        {error, Cause} ->
            {error, Cause}
    end.

%% @private
-spec(merge_fun(Dir, MetadataL, CachedMetadataL) ->
             ok when Dir::binary(),
                     MetadataL::[#?METADATA{}],
                     CachedMetadataL::[#?METADATA{}]).
merge_fun(Dir, [], CachedMetadataL) ->
    %% Put the latest metadata-list into the cache
    CachedMetadataL_1 =
        case (length(CachedMetadataL) > ?DEF_MAX_CACHE_DIR_METADATA) of
            true ->
                lists:sublist(
                  ?DEF_MAX_CACHE_DIR_METADATA, CachedMetadataL);
            false ->
                CachedMetadataL
        end,
    leo_cache_api:put(Dir, term_to_binary(CachedMetadataL_1));

merge_fun(Dir, [#?METADATA{key = Key,
                           clock = Clock,
                           del = Del} = Metadata|Rest], CachedMetadataL) ->
    {CanUpdate, CachedMetadataL_2} =
        case lists:keyfind(Key, 2, CachedMetadataL) of
            #?METADATA{clock = Clock_1} = TargetMetadata
              when Clock > Clock_1 orelse
                   Del == ?DEL_TRUE ->
                CachedMetadataL_1 = lists:delete(TargetMetadata, CachedMetadataL),
                {(Del == ?DEL_FALSE), CachedMetadataL_1};
            false when Del == ?DEL_FALSE ->
                {true, CachedMetadataL};
            _ ->
                {false, CachedMetadataL}
        end,

    NewCachedMetadataL =
        case CanUpdate of
            true ->
                OrdSets = ordsets:from_list(CachedMetadataL_2),
                ordsets:to_list(ordsets:add_element(Metadata, OrdSets));
            false ->
                CachedMetadataL_2
        end,
    merge_fun(Dir, Rest, NewCachedMetadataL).
