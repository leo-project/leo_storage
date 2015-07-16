%%======================================================================
%%
%% LeoFS Storage
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
%% @doc Handling an object, which is included in put, get, delete and head operation
%% @end
%%======================================================================
-module(leo_storage_transformer).

-include("leo_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([transform_read_parameter/1]).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec(transform_read_parameter(ReadParameter) ->
             {ok, #read_parameter_1{}} |
             {error, invalid_record} when ReadParameter::#read_parameter{}|
                                                         #read_parameter_1{}).
transform_read_parameter(#read_parameter{ref = Ref,
                                         addr_id = AddrId,
                                         key = Key,
                                         etag = Etag,
                                         start_pos = StartPos,
                                         end_pos = EndPos,
                                         quorum = Q,
                                         req_id = ReqId}) ->
    {ok, #read_parameter_1{ref = Ref,
                           addr_id = AddrId,
                           key = Key,
                           etag = Etag,
                           start_pos = StartPos,
                           end_pos = EndPos,
                           quorum = Q,
                           req_id = ReqId}};
transform_read_parameter(#read_parameter_1{} = ReadParameter) ->
    {ok, ReadParameter};
transform_read_parameter(_) ->
    {error, invalid_record}.
