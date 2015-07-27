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
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0, stop/0]).
-export([append/2,
         get/1,
         delete/1,
         merge/0, merge/1
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

-spec(merge(Dir) ->
             ok | {error, any()} when Dir::binary()).
merge(Dir) ->
    gen_server:call(?MODULE, {merge, Dir}, ?TIMEOUT).


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
                        {error, Cause}
                end
        end,
    {reply, Reply, State};

handle_call({delete, Dir}, _From, #state{directories = DirL} = State) ->
    {Reply, NewState} =
        case (dict:size(DirL) == 0) of
            true ->
                {ok, State};
            false ->
                case catch dict:erase(Dir, DirL) of
                    {'EXIT', Cause}->
                        {{error, Cause}, State};
                    DirL_1 ->
                        {ok, State#state{directories = DirL_1}}
                end
        end,
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
    {reply, Reply, State#state{directories = dict:new()}};

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
                           del = Del} = Metadata|Rest], CachedMetadataL) ->
    CachedMetadataL_1 =
        case lists:keyfind(Key, 2, CachedMetadataL) of
            #?METADATA{} = TargetMetadata ->
                lists:delete(TargetMetadata, CachedMetadataL);
            _ ->
                CachedMetadataL
        end,

    CachedMetadataL_2 =
        case Del of
            ?DEL_TRUE ->
                CachedMetadataL_1;
            ?DEL_FALSE ->
                OrdSets = ordsets:from_list(CachedMetadataL_1),
                ordsets:to_list(ordsets:add_element(Metadata, OrdSets))
        end,
    merge_fun(Dir, Rest, CachedMetadataL_2).
