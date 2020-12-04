%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% Global Channel Registry
-module(emqx_cm_registry).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Registry]").

-export([start_link/0]).

-export([is_enabled/0]).

-export([ register_channel/1
        , unregister_channel/1
        ]).

-export([lookup_channels/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(REGISTRY, ?MODULE).
-define(TAB, emqx_channel_registry).
-define(LOCK, {?MODULE, cleanup_down}).

-record(channel, {chid, pid}).

%% @doc Start the global channel registry.
-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?REGISTRY}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Is the global registry enabled?
-spec(is_enabled() -> boolean()).
is_enabled() ->
    emqx:get_env(enable_session_registry, true).

%% @doc Register a global channel.
-spec(register_channel(emqx_types:clientid()
                    | {emqx_types:clientid(), pid()}) -> ok).
register_channel(ClientId) when is_binary(ClientId) ->
    register_channel({ClientId, self()});

register_channel({ClientId, ChanPid}) when is_binary(ClientId), is_pid(ChanPid) ->
    case is_enabled() of
        true -> mnesia:dirty_write(?TAB, record(ClientId, ChanPid));
        false -> ok
    end.

%% @doc Unregister a global channel.
-spec(unregister_channel(emqx_types:clientid()
                      | {emqx_types:clientid(), pid()}) -> ok).
unregister_channel(ClientId) when is_binary(ClientId) ->
    unregister_channel({ClientId, self()});

unregister_channel({ClientId, ChanPid}) when is_binary(ClientId), is_pid(ChanPid) ->
    case is_enabled() of
        true -> mnesia:dirty_delete_object(?TAB, record(ClientId, ChanPid));
        false -> ok
    end.

%% @doc Lookup the global channels.
-spec(lookup_channels(emqx_types:clientid()) -> list(pid())).
lookup_channels(ClientId) ->
    [ChanPid || #channel{pid = ChanPid} <- mnesia:dirty_read(?TAB, ClientId)].

record(ClientId, ChanPid) ->
    #channel{chid = ClientId, pid = ChanPid}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, channel},
                {attributes, record_info(fields, channel)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]),
    ok = ekka_mnesia:copy_table(?TAB),
    ok = ekka:monitor(membership),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State) ->
    global:trans({?LOCK, self()},
                 fun() ->
                     mnesia:transaction(fun cleanup_channels/1, [Node])
                 end),
    {noreply, State};

handle_info({membership, _Event}, State) ->
    {noreply, State};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

cleanup_channels(Node) ->
    Pat = [{#channel{pid = '$1', _ = '_'}, [{'==', {node, '$1'}, Node}], ['$_']}],
    lists:foreach(fun delete_channel/1, mnesia:select(?TAB, Pat, write)).

delete_channel(Chan) ->
    mnesia:delete_object(?TAB, Chan, write).

