%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Global Channel Registry
-module(emqx_cm_registry).

-behaviour(gen_server).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/0]).

-export([is_enabled/0, is_hist_enabled/0, table_size/0]).

-export([
    register_channel/1,
    register_channel2/1,
    unregister_channel/1,
    unregister_channel2/1
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

%% Internal exports (RPC)
-export([
    do_cleanup_channels/1
]).

-export([count_dirty/0]).

-ifdef(TEST).
%% For testing only
-export([
    force_delete/1,
    purge/0
]).
-endif.

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
-spec count_dirty() -> non_neg_integer().
count_dirty() ->
    mnesia:table_info(?CHAN_REG_TAB, size).

%% @doc Is the global registry enabled?
-spec is_enabled() -> boolean().
is_enabled() ->
    emqx:get_config([broker, enable_session_registry]).

%% @doc Is the global session registration history enabled?
-spec is_hist_enabled() -> boolean().
is_hist_enabled() ->
    retain_duration() > 0.

%% @doc Get the size of the global channel registry table.
-spec table_size() -> non_neg_integer().
table_size() ->
    ets:info(?CHAN_REG_TAB, size).

%% @doc Register a global channel.
-spec register_channel(
    emqx_types:clientid()
    | {emqx_types:clientid(), pid()}
) -> ok.
register_channel(ClientId) when is_binary(ClientId) ->
    register_channel({ClientId, self()});
register_channel({ClientId, ChanPid}) when is_binary(ClientId), is_pid(ChanPid) ->
    IsHistEnabled = is_hist_enabled(),
    case is_enabled() of
        true when IsHistEnabled ->
            mria:async_dirty(?CM_SHARD, fun ?MODULE:register_channel2/1, [record(ClientId, ChanPid)]);
        true ->
            mria:dirty_write(?CHAN_REG_TAB, record(ClientId, ChanPid));
        false ->
            ok
    end.

%% @private
register_channel2(#channel{chid = ClientId} = Record) ->
    _ = delete_hist_d(ClientId),
    mria:dirty_write(?CHAN_REG_TAB, Record).

%% @doc Unregister a global channel.
-spec unregister_channel(
    emqx_types:clientid()
    | {emqx_types:clientid(), pid()}
) -> ok.
unregister_channel(ClientId) when is_binary(ClientId) ->
    unregister_channel({ClientId, self()});
unregister_channel({ClientId, ChanPid}) when is_binary(ClientId), is_pid(ChanPid) ->
    IsHistEnabled = is_hist_enabled(),
    case is_enabled() of
        true when IsHistEnabled ->
            mria:async_dirty(?CM_SHARD, fun ?MODULE:unregister_channel2/1, [
                record(ClientId, ChanPid)
            ]);
        true ->
            mria:dirty_delete_object(?CHAN_REG_TAB, record(ClientId, ChanPid));
        false ->
            ok
    end.

-ifdef(TEST).
%% @hidden Force delete a global channel.
%% For testing only.
-spec force_delete(emqx_types:clientid()) -> ok.
force_delete(ClientId) ->
    mria:dirty_delete(?CHAN_REG_TAB, ClientId).

%% @hidden Purge the global channel registry.
%% For testing only.
-spec purge() -> ok.
purge() ->
    ets:delete_all_objects(?CHAN_REG_TAB).
-endif.

%% @private
unregister_channel2(#channel{chid = ClientId} = Record) ->
    mria:dirty_delete_object(?CHAN_REG_TAB, Record),
    ok = insert_hist_d(ClientId).

%% @doc Lookup the global channels.
-spec lookup_channels(emqx_types:clientid()) -> list(pid()).
lookup_channels(ClientId) ->
    Chans = mnesia:dirty_read(?CHAN_REG_TAB, ClientId),
    %% @NOTE: remote node PID is always "alive".
    [ChanPid || #channel{pid = ChanPid} <- Chans, is_pid_alive(ChanPid) =/= false].

%% Return 'true' or 'false' if it's a local pid.
%% Otherwise return 'unknown'.
is_pid_alive(Pid) when is_integer(Pid) ->
    %% broker.session_history_retain > 0
    false;
is_pid_alive(Pid) when is_pid(Pid) andalso node(Pid) =:= node() ->
    erlang:is_process_alive(Pid);
is_pid_alive(_) ->
    unknown.

record(ClientId, ChanPid) ->
    #channel{chid = ClientId, pid = ChanPid}.

hist(ClientId) ->
    #channel{chid = ClientId, pid = now_ts()}.

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
    ?tp(warning, cm_registry_mnesia_down, #{node => Node}),
    cleanup_channels(Node),
    {noreply, State};
handle_info({membership, {node, down, Node}}, State) ->
    ?tp(warning, cm_registry_node_down, #{node => Node}),
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
insert_hist_t(ClientId) ->
    case delete_hist_t(ClientId) of
        true ->
            ok;
        false ->
            mnesia:write(?CHAN_REG_TAB, hist(ClientId), write)
    end.

%% Dirty insert unregistration history.
%% Since dirty opts are used, async pool workers may race deletes and inserts,
%% so there could be more than one history records for a clientid,
%% but it should be eventually consistent after the client re-registers or the periodic cleanup.
insert_hist_d(ClientId) ->
    %% delete old hist records first
    case delete_hist_d(ClientId) of
        true ->
            ok;
        false ->
            mria:dirty_write(?CHAN_REG_TAB, hist(ClientId))
    end.

%% Current timestamp in seconds.
now_ts() ->
    erlang:system_time(seconds).

%% Delete all history records for a clientid, return true if there is a Pid found.
delete_hist_t(ClientId) ->
    fold_hist(
        fun(Hist) -> mnesia:delete_object(?CHAN_REG_TAB, Hist, write) end,
        mnesia:read(?CHAN_REG_TAB, ClientId, write)
    ).

%% Delete all history records for a clientid, return true if there is a Pid found.
delete_hist_d(ClientId) ->
    fold_hist(
        fun(Hist) -> mria:dirty_delete_object(?CHAN_REG_TAB, Hist) end,
        mnesia:dirty_read(?CHAN_REG_TAB, ClientId)
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
