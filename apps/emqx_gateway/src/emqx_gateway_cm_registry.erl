%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The gateway connection registry
-module(emqx_gateway_cm_registry).

-behaviour(gen_server).

-logger_header("[PGW-CM-Registy]").

-export([start_link/1]).

%% XXX: needless
%-export([is_enabled/0]).

-export([ register_channel/2
        , unregister_channel/2
        ]).

-export([lookup_channels/2]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(LOCK, {?MODULE, cleanup_down}).

-record(channel, {chid, pid}).

%% @doc Start the global channel registry.
-spec(start_link(atom()) -> gen_server:startlink_ret()).
start_link(Type) ->
    gen_server:start_link(?MODULE, [Type], []).

-spec tabname(atom()) -> atom().
tabname(Type) ->
    list_to_atom(lists:concat([emqx_gateway_, Type, '_channel_registry'])).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Register a global channel.
-spec register_channel(atom(), binary() | {binary(), pid()}) -> ok.
register_channel(Type, ClientId) when is_binary(ClientId) ->
    register_channel(Type, {ClientId, self()});

register_channel(Type, {ClientId, ChanPid}) when is_binary(ClientId), is_pid(ChanPid) ->
    ekka_mnesia:dirty_write(tabname(Type), record(ClientId, ChanPid)).

%% @doc Unregister a global channel.
-spec unregister_channel(atom(), binary() | {binary(), pid()}) -> ok.
unregister_channel(Type, ClientId) when is_binary(ClientId) ->
    unregister_channel(Type, {ClientId, self()});

unregister_channel(Type, {ClientId, ChanPid}) when is_binary(ClientId), is_pid(ChanPid) ->
    ekka_mnesia:dirty_delete_object(tabname(Type), record(ClientId, ChanPid)).

%% @doc Lookup the global channels.
-spec lookup_channels(atom(), binary()) -> list(pid()).
lookup_channels(Type, ClientId) ->
    [ChanPid || #channel{pid = ChanPid} <- mnesia:dirty_read(tabname(Type), ClientId)].

record(ClientId, ChanPid) ->
    #channel{chid = ClientId, pid = ChanPid}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Type]) ->
    Tab = tabname(Type),
    ok = ekka_mnesia:create_table(Tab, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, channel},
                {attributes, record_info(fields, channel)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]),
    ok = ekka_mnesia:copy_table(Tab, ram_copies),
    %%ok = ekka_rlog:wait_for_shards([?CM_SHARD], infinity),
    ok = ekka:monitor(membership),
    {ok, #{type => Type}}.

handle_call(Req, _From, State) ->
    logger:error("Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    logger:error("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State = #{type := Type}) ->
    Tab = tabname(Type),
    global:trans({?LOCK, self()},
                 fun() ->
                     %% FIXME: The shard name should be fixed later
                     ekka_mnesia:transaction(?MODULE, fun cleanup_channels/2, [Node, Tab])
                 end),
    {noreply, State};

handle_info({membership, _Event}, State) ->
    {noreply, State};

handle_info(Info, State) ->
    logger:error("Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

cleanup_channels(Node, Tab) ->
    Pat = [{#channel{pid = '$1', _ = '_'}, [{'==', {node, '$1'}, Node}], ['$_']}],
    lists:foreach(fun(Chan) ->
        mnesia:delete_object(Tab, Chan, write)
    end, mnesia:select(Tab, Pat, write)).
