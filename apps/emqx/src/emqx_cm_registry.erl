%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start_link/0]).

-export([is_enabled/0]).

-export([
    register_channel/1,
    unregister_channel/1
]).

-export([lookup_channels/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(REGISTRY, ?MODULE).
-define(TAB, emqx_channel_registry).
-define(LOCK, {?MODULE, cleanup_down}).

-record(channel, {chid, pid}).

%% @doc Start the global channel registry.
-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?REGISTRY}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Is the global registry enabled?
-spec is_enabled() -> boolean().
is_enabled() ->
    emqx:get_config([broker, enable_session_registry]).

%% @doc Register a global channel.
-spec register_channel(
    emqx_types:clientid()
    | {emqx_types:clientid(), pid()}
) -> ok.
register_channel(ClientId) when is_binary(ClientId) ->
    register_channel({ClientId, self()});
register_channel({ClientId, ChanPid}) when is_binary(ClientId), is_pid(ChanPid) ->
    case is_enabled() of
        true -> mria:dirty_write(?TAB, record(ClientId, ChanPid));
        false -> ok
    end.

%% @doc Unregister a global channel.
-spec unregister_channel(
    emqx_types:clientid()
    | {emqx_types:clientid(), pid()}
) -> ok.
unregister_channel(ClientId) when is_binary(ClientId) ->
    unregister_channel({ClientId, self()});
unregister_channel({ClientId, ChanPid}) when is_binary(ClientId), is_pid(ChanPid) ->
    case is_enabled() of
        true -> mria:dirty_delete_object(?TAB, record(ClientId, ChanPid));
        false -> ok
    end.

%% @doc Lookup the global channels.
-spec lookup_channels(emqx_types:clientid()) -> list(pid()).
lookup_channels(ClientId) ->
    [ChanPid || #channel{pid = ChanPid} <- mnesia:dirty_read(?TAB, ClientId)].

record(ClientId, ChanPid) ->
    #channel{chid = ClientId, pid = ChanPid}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    mria_config:set_dirty_shard(?CM_SHARD, true),
    ok = mria:create_table(?TAB, [
        {type, bag},
        {rlog_shard, ?CM_SHARD},
        {storage, ram_copies},
        {record_name, channel},
        {attributes, record_info(fields, channel)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    ok = mria_rlog:wait_for_shards([?CM_SHARD], infinity),
    ok = ekka:monitor(membership),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State) ->
    cleanup_channels(Node),
    {noreply, State};
handle_info({membership, {node, down, Node}}, State) ->
    cleanup_channels(Node),
    {noreply, State};
handle_info({membership, _Event}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

cleanup_channels(Node) ->
    global:trans(
        {?LOCK, self()},
        fun() ->
            mria:transaction(?CM_SHARD, fun do_cleanup_channels/1, [Node])
        end
    ).

do_cleanup_channels(Node) ->
    Pat = [{#channel{pid = '$1', _ = '_'}, [{'==', {node, '$1'}, Node}], ['$_']}],
    lists:foreach(fun delete_channel/1, mnesia:select(?TAB, Pat, write)).

delete_channel(Chan) ->
    mnesia:delete_object(?TAB, Chan, write).
