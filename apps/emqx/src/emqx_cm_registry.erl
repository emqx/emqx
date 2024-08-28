%%--------------------------------------------------------------------
%% Copyright (c) 2019-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start_link/0]).

-export([is_enabled/0, is_hist_enabled/0]).

-export([
    register_channel/1,
    register_channel2/1,
    unregister_channel/1,
    unregister_channel2/1
]).

-export([lookup_channels/1, lookup_channels/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Internal exports (RPC)
-export([
    do_cleanup_channels/1
]).

-include("emqx.hrl").
-include("emqx_cm.hrl").
-include("logger.hrl").
-include("types.hrl").

-define(REGISTRY, ?MODULE).
-define(NODE_DOWN_CLEANUP_LOCK, {?MODULE, cleanup_down}).

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

%% @doc Is the global session registration history enabled?
-spec is_hist_enabled() -> boolean().
is_hist_enabled() ->
    retain_duration() > 0.

%% @doc Register a global channel.
-spec register_channel(
    emqx_types:cid() | emqx_types:clientid() | {emqx_types:cid(), pid()}
) -> ok.
register_channel(ClientId) when ?IS_CLIENTID(ClientId) ->
    register_channel({{?NO_MTNS, ClientId}, self()});
register_channel(CId) when ?IS_CID(CId) ->
    register_channel({CId, self()});
register_channel({CId, ChanPid}) when ?IS_CID(CId), is_pid(ChanPid) ->
    IsHistEnabled = is_hist_enabled(),
    case is_enabled() of
        true when IsHistEnabled ->
            mria:async_dirty(?CM_SHARD, fun ?MODULE:register_channel2/1, [record(CId, ChanPid)]);
        true ->
            mria:dirty_write(?CHAN_REG_TAB, record(CId, ChanPid));
        false ->
            ok
    end.

%% @private
register_channel2(#channel{chid = CId} = Record) ->
    _ = delete_hist_d(CId),
    mria:dirty_write(?CHAN_REG_TAB, Record).

%% @doc Unregister a global channel.
-spec unregister_channel(
    emqx_types:cid() | emqx_types:clientid() | {emqx_types:cid(), pid()}
) -> ok.
unregister_channel(ClientId) when ?IS_CLIENTID(ClientId) ->
    unregister_channel({{?NO_MTNS, ClientId}, self()});
unregister_channel(CId) when ?IS_CID(CId) ->
    unregister_channel({CId, self()});
unregister_channel({CId, ChanPid}) when ?IS_CID(CId), is_pid(ChanPid) ->
    IsHistEnabled = is_hist_enabled(),
    case is_enabled() of
        true when IsHistEnabled ->
            mria:async_dirty(?CM_SHARD, fun ?MODULE:unregister_channel2/1, [
                record(CId, ChanPid)
            ]);
        true ->
            mria:dirty_delete_object(?CHAN_REG_TAB, record(CId, ChanPid));
        false ->
            ok
    end.

%% @private
unregister_channel2(#channel{chid = CId} = Record) ->
    mria:dirty_delete_object(?CHAN_REG_TAB, Record),
    ok = insert_hist_d(CId).

%% @doc Lookup the global channels.
-spec lookup_channels(emqx_types:cid() | emqx_types:clientid()) -> list(pid()).
lookup_channels(ClientId) when ?IS_CLIENTID(ClientId) ->
    lookup_channels(emqx_mtns:cid(?NO_MTNS, ClientId));
lookup_channels(_CId = {Mtns, ClientId}) ->
    lookup_channels(Mtns, ClientId).

-spec lookup_channels(emqx_types:mtns(), emqx_types:clientid()) -> list(pid()).
lookup_channels(Mtns, ClientId) ->
    CId = emqx_mtns:cid(Mtns, ClientId),
    lists:filtermap(
        fun
            (#channel{pid = ChanPid}) when is_pid(ChanPid) ->
                case is_pid_down(ChanPid) of
                    true ->
                        false;
                    _ ->
                        {true, ChanPid}
                end;
            (_) ->
                false
        end,
        mnesia:dirty_read(?CHAN_REG_TAB, CId)
    ).

%% Return 'true' or 'false' if it's a local pid.
%% Otherwise return 'unknown'.
is_pid_down(Pid) when node(Pid) =:= node() ->
    not erlang:is_process_alive(Pid);
is_pid_down(_) ->
    unknown.

record(CId, ChanPid) ->
    #channel{chid = CId, pid = ChanPid}.

hist(CId) ->
    #channel{chid = CId, pid = now_ts()}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    mria_config:set_dirty_shard(?CM_SHARD, true),
    ok = mria:create_table(?CHAN_REG_TAB, [
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
        {?NODE_DOWN_CLEANUP_LOCK, self()},
        fun() ->
            mria:transaction(?CM_SHARD, fun ?MODULE:do_cleanup_channels/1, [Node])
        end
    ).

do_cleanup_channels(Node) ->
    Pat = [
        {
            #channel{pid = '$1', _ = '_'},
            _Match = [{'andalso', {is_pid, '$1'}, {'==', {node, '$1'}, Node}}],
            _Return = ['$_']
        }
    ],
    IsHistEnabled = is_hist_enabled(),
    lists:foreach(
        fun(Chan) -> delete_channel(IsHistEnabled, Chan) end,
        mnesia:select(?CHAN_REG_TAB, Pat, write)
    ).

delete_channel(IsHistEnabled, Chan) ->
    mnesia:delete_object(?CHAN_REG_TAB, Chan, write),
    case IsHistEnabled of
        true ->
            insert_hist_t(Chan#channel.chid);
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% History entry operations

%% Insert unregistration history in a transaction when unregistering the last channel for a clientid.
insert_hist_t(CId) ->
    case delete_hist_t(CId) of
        true ->
            ok;
        false ->
            mnesia:write(?CHAN_REG_TAB, hist(CId), write)
    end.

%% Dirty insert unregistration history.
%% Since dirty opts are used, async pool workers may race deletes and inserts,
%% so there could be more than one history records for a clientid,
%% but it should be eventually consistent after the client re-registers or the periodic cleanup.
insert_hist_d(CId) ->
    %% delete old hist records first
    case delete_hist_d(CId) of
        true ->
            ok;
        false ->
            mria:dirty_write(?CHAN_REG_TAB, hist(CId))
    end.

%% Current timestamp in seconds.
now_ts() ->
    erlang:system_time(seconds).

%% Delete all history records for a clientid, return true if there is a Pid found.
delete_hist_t(CId) ->
    fold_hist(
        fun(Hist) -> mnesia:delete_object(?CHAN_REG_TAB, Hist, write) end,
        mnesia:read(?CHAN_REG_TAB, CId, write)
    ).

%% Delete all history records for a clientid, return true if there is a Pid found.
delete_hist_d(CId) ->
    fold_hist(
        fun(Hist) -> mria:dirty_delete_object(?CHAN_REG_TAB, Hist) end,
        mnesia:dirty_read(?CHAN_REG_TAB, CId)
    ).

%% Fold over the history records, return true if there is a Pid found.
fold_hist(F, List) ->
    lists:foldl(
        fun(#channel{pid = Ts} = Record, HasPid) ->
            case is_integer(Ts) of
                true ->
                    ok = F(Record),
                    HasPid;
                false ->
                    true
            end
        end,
        false,
        List
    ).

%% Return the session registration history retain duration.
-spec retain_duration() -> non_neg_integer().
retain_duration() ->
    emqx:get_config([broker, session_history_retain]).
