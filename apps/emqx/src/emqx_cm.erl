%%-------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Channel Manager
-module(emqx_cm).

-behaviour(gen_server).

-include("logger.hrl").
-include("types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/0]).

-export([
    register_channel/3,
    unregister_channel/1,
    insert_channel_info/3
]).

-export([connection_closed/1]).

-export([
    get_chan_info/1,
    get_chan_info/2,
    set_chan_info/2
]).

-export([
    get_chan_stats/1,
    get_chan_stats/2,
    set_chan_stats/2
]).

-export([get_chann_conn_mod/2]).

-export([
    open_session/3,
    discard_session/1,
    discard_session/2,
    takeover_session/1,
    takeover_session/2,
    kick_session/1,
    kick_session/2
]).

-export([
    lookup_channels/1,
    lookup_channels/2,

    lookup_client/1
]).

%% Test/debug interface
-export([
    all_channels/0,
    all_client_ids/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Internal export
-export([
    stats_fun/0,
    clean_down/1,
    mark_channel_connected/1,
    mark_channel_disconnected/1,
    get_connected_client_count/0,

    do_kick_session/3,
    do_get_chan_stats/2,
    do_get_chan_info/2,
    do_get_chann_conn_mod/2
]).

-export_type([
    channel_info/0,
    chan_pid/0
]).

-type chan_pid() :: pid().

-type channel_info() :: {
    _Chan :: {emqx_types:clientid(), pid()},
    _Info :: emqx_types:infos(),
    _Stats :: emqx_types:stats()
}.

-include("emqx_cm.hrl").

%% Tables for channel management.
-define(CHAN_TAB, emqx_channel).
-define(CHAN_CONN_TAB, emqx_channel_conn).
-define(CHAN_INFO_TAB, emqx_channel_info).
-define(CHAN_LIVE_TAB, emqx_channel_live).

-define(CHAN_STATS, [
    {?CHAN_TAB, 'channels.count', 'channels.max'},
    {?CHAN_TAB, 'sessions.count', 'sessions.max'},
    {?CHAN_CONN_TAB, 'connections.count', 'connections.max'},
    {?CHAN_LIVE_TAB, 'live_connections.count', 'live_connections.max'}
]).

%% Batch drain
-define(BATCH_SIZE, 100000).

%% Server name
-define(CM, ?MODULE).

%% linting overrides
-elvis([
    {elvis_style, invalid_dynamic_call, #{ignore => [emqx_cm]}},
    {elvis_style, god_modules, #{ignore => [emqx_cm]}}
]).

%% @doc Start the channel manager.
-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?CM}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Insert/Update the channel info and stats to emqx_channel table
-spec insert_channel_info(
    emqx_types:clientid(),
    emqx_types:infos(),
    emqx_types:stats()
) -> ok.
insert_channel_info(ClientId, Info, Stats) ->
    Chan = {ClientId, self()},
    true = ets:insert(?CHAN_INFO_TAB, {Chan, Info, Stats}),
    ?tp(debug, insert_channel_info, #{client_id => ClientId}),
    ok.

%% @private
%% @doc Register a channel with pid and conn_mod.
%%
%% There is a Race-Condition on one node or cluster when many connections
%% login to Broker with the same clientid. We should register it and save
%% the conn_mod first for taking up the clientid access right.
%%
%% Note that: It should be called on a lock transaction
register_channel(ClientId, ChanPid, #{conn_mod := ConnMod}) when is_pid(ChanPid) ->
    Chan = {ClientId, ChanPid},
    true = ets:insert(?CHAN_TAB, Chan),
    true = ets:insert(?CHAN_CONN_TAB, {Chan, ConnMod}),
    ok = emqx_cm_registry:register_channel(Chan),
    mark_channel_connected(ChanPid),
    cast({registered, Chan}).

%% @doc Unregister a channel.
-spec unregister_channel(emqx_types:clientid()) -> ok.
unregister_channel(ClientId) when is_binary(ClientId) ->
    true = do_unregister_channel({ClientId, self()}),
    ok.

%% @private
do_unregister_channel(Chan) ->
    ok = emqx_cm_registry:unregister_channel(Chan),
    true = ets:delete(?CHAN_CONN_TAB, Chan),
    true = ets:delete(?CHAN_INFO_TAB, Chan),
    ets:delete_object(?CHAN_TAB, Chan).

-spec connection_closed(emqx_types:clientid()) -> true.
connection_closed(ClientId) ->
    connection_closed(ClientId, self()).

-spec connection_closed(emqx_types:clientid(), chan_pid()) -> true.
connection_closed(ClientId, ChanPid) ->
    ets:delete_object(?CHAN_CONN_TAB, {ClientId, ChanPid}).

%% @doc Get info of a channel.
-spec get_chan_info(emqx_types:clientid()) -> maybe(emqx_types:infos()).
get_chan_info(ClientId) ->
    with_channel(ClientId, fun(ChanPid) -> get_chan_info(ClientId, ChanPid) end).

-spec do_get_chan_info(emqx_types:clientid(), chan_pid()) ->
    maybe(emqx_types:infos()).
do_get_chan_info(ClientId, ChanPid) ->
    Chan = {ClientId, ChanPid},
    try
        ets:lookup_element(?CHAN_INFO_TAB, Chan, 2)
    catch
        error:badarg -> undefined
    end.

-spec get_chan_info(emqx_types:clientid(), chan_pid()) ->
    maybe(emqx_types:infos()).
get_chan_info(ClientId, ChanPid) ->
    wrap_rpc(emqx_cm_proto_v1:get_chan_info(ClientId, ChanPid)).

%% @doc Update infos of the channel.
-spec set_chan_info(emqx_types:clientid(), emqx_types:attrs()) -> boolean().
set_chan_info(ClientId, Info) when is_binary(ClientId) ->
    Chan = {ClientId, self()},
    try
        ets:update_element(?CHAN_INFO_TAB, Chan, {2, Info})
    catch
        error:badarg -> false
    end.

%% @doc Get channel's stats.
-spec get_chan_stats(emqx_types:clientid()) -> maybe(emqx_types:stats()).
get_chan_stats(ClientId) ->
    with_channel(ClientId, fun(ChanPid) -> get_chan_stats(ClientId, ChanPid) end).

-spec do_get_chan_stats(emqx_types:clientid(), chan_pid()) ->
    maybe(emqx_types:stats()).
do_get_chan_stats(ClientId, ChanPid) ->
    Chan = {ClientId, ChanPid},
    try
        ets:lookup_element(?CHAN_INFO_TAB, Chan, 3)
    catch
        error:badarg -> undefined
    end.

-spec get_chan_stats(emqx_types:clientid(), chan_pid()) ->
    maybe(emqx_types:stats()).
get_chan_stats(ClientId, ChanPid) ->
    wrap_rpc(emqx_cm_proto_v1:get_chan_stats(ClientId, ChanPid)).

%% @doc Set channel's stats.
-spec set_chan_stats(emqx_types:clientid(), emqx_types:stats()) -> boolean().
set_chan_stats(ClientId, Stats) when is_binary(ClientId) ->
    set_chan_stats(ClientId, self(), Stats).

-spec set_chan_stats(emqx_types:clientid(), chan_pid(), emqx_types:stats()) ->
    boolean().
set_chan_stats(ClientId, ChanPid, Stats) ->
    Chan = {ClientId, ChanPid},
    try
        ets:update_element(?CHAN_INFO_TAB, Chan, {3, Stats})
    catch
        error:badarg -> false
    end.

%% @doc Open a session.
-spec open_session(boolean(), emqx_types:clientinfo(), emqx_types:conninfo()) ->
    {ok, #{
        session := emqx_session:session(),
        present := boolean(),
        pendings => list()
    }}
    | {error, Reason :: term()}.
open_session(true, ClientInfo = #{clientid := ClientId}, ConnInfo) ->
    Self = self(),
    CleanStart = fun(_) ->
        ok = discard_session(ClientId),
        ok = emqx_persistent_session:discard_if_present(ClientId),
        Session = create_session(ClientInfo, ConnInfo),
        Session1 = emqx_persistent_session:persist(ClientInfo, ConnInfo, Session),
        register_channel(ClientId, Self, ConnInfo),
        {ok, #{session => Session1, present => false}}
    end,
    emqx_cm_locker:trans(ClientId, CleanStart);
open_session(false, ClientInfo = #{clientid := ClientId}, ConnInfo) ->
    Self = self(),
    ResumeStart = fun(_) ->
        CreateSess =
            fun() ->
                Session = create_session(ClientInfo, ConnInfo),
                Session1 = emqx_persistent_session:persist(
                    ClientInfo, ConnInfo, Session
                ),
                register_channel(ClientId, Self, ConnInfo),
                {ok, #{session => Session1, present => false}}
            end,
        case takeover_session(ClientId) of
            {persistent, Session} ->
                %% This is a persistent session without a managing process.
                {Session1, Pendings} =
                    emqx_persistent_session:resume(ClientInfo, ConnInfo, Session),
                register_channel(ClientId, Self, ConnInfo),

                {ok, #{
                    session => Session1,
                    present => true,
                    pendings => Pendings
                }};
            {living, ConnMod, ChanPid, Session} ->
                ok = emqx_session:resume(ClientInfo, Session),
                case
                    request_stepdown(
                        {takeover, 'end'},
                        ConnMod,
                        ChanPid
                    )
                of
                    {ok, Pendings} ->
                        Session1 = emqx_persistent_session:persist(
                            ClientInfo, ConnInfo, Session
                        ),
                        register_channel(ClientId, Self, ConnInfo),
                        {ok, #{
                            session => Session1,
                            present => true,
                            pendings => Pendings
                        }};
                    {error, _} ->
                        CreateSess()
                end;
            {expired, OldSession} ->
                _ = emqx_persistent_session:discard(ClientId, OldSession),
                Session = create_session(ClientInfo, ConnInfo),
                Session1 = emqx_persistent_session:persist(
                    ClientInfo,
                    ConnInfo,
                    Session
                ),
                register_channel(ClientId, Self, ConnInfo),
                {ok, #{session => Session1, present => false}};
            none ->
                CreateSess()
        end
    end,
    emqx_cm_locker:trans(ClientId, ResumeStart).

create_session(ClientInfo, ConnInfo) ->
    Options = get_session_confs(ClientInfo, ConnInfo),
    Session = emqx_session:init(Options),
    ok = emqx_metrics:inc('session.created'),
    ok = emqx_hooks:run('session.created', [ClientInfo, emqx_session:info(Session)]),
    Session.

get_session_confs(#{zone := Zone, clientid := ClientId}, #{
    receive_maximum := MaxInflight, expiry_interval := EI
}) ->
    #{
        clientid => ClientId,
        max_subscriptions => get_mqtt_conf(Zone, max_subscriptions),
        upgrade_qos => get_mqtt_conf(Zone, upgrade_qos),
        max_inflight => MaxInflight,
        retry_interval => get_mqtt_conf(Zone, retry_interval),
        await_rel_timeout => get_mqtt_conf(Zone, await_rel_timeout),
        mqueue => mqueue_confs(Zone),
        %% TODO: Add conf for allowing/disallowing persistent sessions.
        %% Note that the connection info is already enriched to have
        %% default config values for session expiry.
        is_persistent => EI > 0
    }.

mqueue_confs(Zone) ->
    #{
        max_len => get_mqtt_conf(Zone, max_mqueue_len),
        store_qos0 => get_mqtt_conf(Zone, mqueue_store_qos0),
        priorities => get_mqtt_conf(Zone, mqueue_priorities),
        default_priority => get_mqtt_conf(Zone, mqueue_default_priority)
    }.

get_mqtt_conf(Zone, Key) ->
    emqx_config:get_zone_conf(Zone, [mqtt, Key]).

%% @doc Try to takeover a session.
-spec takeover_session(emqx_types:clientid()) ->
    none
    | {living, atom(), pid(), emqx_session:session()}
    | {persistent, emqx_session:session()}
    | {expired, emqx_session:session()}.
takeover_session(ClientId) ->
    case lookup_channels(ClientId) of
        [] ->
            emqx_persistent_session:lookup(ClientId);
        [ChanPid] ->
            takeover_session(ClientId, ChanPid);
        ChanPids ->
            [ChanPid | StalePids] = lists:reverse(ChanPids),
            ?SLOG(warning, #{msg => "more_than_one_channel_found", chan_pids => ChanPids}),
            lists:foreach(
                fun(StalePid) ->
                    catch discard_session(ClientId, StalePid)
                end,
                StalePids
            ),
            takeover_session(ClientId, ChanPid)
    end.

takeover_session(ClientId, Pid) ->
    try
        do_takeover_session(ClientId, Pid)
    catch
        _:R when
            R == noproc;
            R == timeout;
            %% request_stepdown/3
            R == unexpected_exception
        ->
            emqx_persistent_session:lookup(ClientId);
        % rpc_call/3
        _:{'EXIT', {noproc, _}} ->
            emqx_persistent_session:lookup(ClientId)
    end.

do_takeover_session(ClientId, ChanPid) when node(ChanPid) == node() ->
    case get_chann_conn_mod(ClientId, ChanPid) of
        undefined ->
            emqx_persistent_session:lookup(ClientId);
        ConnMod when is_atom(ConnMod) ->
            case request_stepdown({takeover, 'begin'}, ConnMod, ChanPid) of
                {ok, Session} ->
                    {living, ConnMod, ChanPid, Session};
                {error, Reason} ->
                    error(Reason)
            end
    end;
do_takeover_session(ClientId, ChanPid) ->
    wrap_rpc(emqx_cm_proto_v1:takeover_session(ClientId, ChanPid)).

%% @doc Discard all the sessions identified by the ClientId.
-spec discard_session(emqx_types:clientid()) -> ok.
discard_session(ClientId) when is_binary(ClientId) ->
    case lookup_channels(ClientId) of
        [] -> ok;
        ChanPids -> lists:foreach(fun(Pid) -> discard_session(ClientId, Pid) end, ChanPids)
    end.

%% @private Kick a local stale session to force it step down.
%% If failed to kick (e.g. timeout) force a kill.
%% Keeping the stale pid around, or returning error or raise an exception
%% benefits nobody.
-spec request_stepdown(Action, module(), pid()) ->
    ok
    | {ok, emqx_session:session() | list(emqx_type:deliver())}
    | {error, term()}
when
    Action :: kick | discard | {takeover, 'begin'} | {takeover, 'end'}.
request_stepdown(Action, ConnMod, Pid) ->
    Timeout =
        case Action == kick orelse Action == discard of
            true -> ?T_KICK;
            _ -> ?T_TAKEOVER
        end,
    Return =
        %% this is essentially a gen_server:call implemented in emqx_connection
        %% and emqx_ws_connection.
        %% the handle_call is implemented in emqx_channel
        try apply(ConnMod, call, [Pid, Action, Timeout]) of
            ok -> ok;
            Reply -> {ok, Reply}
        catch
            % emqx_ws_connection: call
            _:noproc ->
                ok = ?tp(debug, "session_already_gone", #{pid => Pid, action => Action}),
                {error, noproc};
            % emqx_connection: gen_server:call
            _:{noproc, _} ->
                ok = ?tp(debug, "session_already_gone", #{pid => Pid, action => Action}),
                {error, noproc};
            _:{shutdown, _} ->
                ok = ?tp(debug, "session_already_shutdown", #{pid => Pid, action => Action}),
                {error, noproc};
            _:{{shutdown, _}, _} ->
                ok = ?tp(debug, "session_already_shutdown", #{pid => Pid, action => Action}),
                {error, noproc};
            _:{timeout, {gen_server, call, _}} ->
                ?tp(
                    warning,
                    "session_stepdown_request_timeout",
                    #{pid => Pid, action => Action, stale_channel => stale_channel_info(Pid)}
                ),
                ok = force_kill(Pid),
                {error, timeout};
            _:Error:St ->
                ?tp(
                    error,
                    "session_stepdown_request_exception",
                    #{
                        pid => Pid,
                        action => Action,
                        reason => Error,
                        stacktrace => St,
                        stale_channel => stale_channel_info(Pid)
                    }
                ),
                ok = force_kill(Pid),
                {error, unexpected_exception}
        end,
    case Action == kick orelse Action == discard of
        true -> ok;
        _ -> Return
    end.

force_kill(Pid) ->
    exit(Pid, kill),
    ok.

stale_channel_info(Pid) ->
    process_info(Pid, [status, message_queue_len, current_stacktrace]).

discard_session(ClientId, ChanPid) ->
    kick_session(discard, ClientId, ChanPid).

kick_session(ClientId, ChanPid) ->
    kick_session(kick, ClientId, ChanPid).

-spec do_kick_session(kick | discard, emqx_types:clientid(), chan_pid()) -> ok.
do_kick_session(Action, ClientId, ChanPid) ->
    case get_chann_conn_mod(ClientId, ChanPid) of
        undefined ->
            %% already deregistered
            ok;
        ConnMod when is_atom(ConnMod) ->
            ok = request_stepdown(Action, ConnMod, ChanPid)
    end.

%% @private This function is shared for session 'kick' and 'discard' (as the first arg Action).
kick_session(Action, ClientId, ChanPid) ->
    try
        wrap_rpc(emqx_cm_proto_v1:kick_session(Action, ClientId, ChanPid))
    catch
        Error:Reason ->
            %% This should mostly be RPC failures.
            %% However, if the node is still running the old version
            %% code (prior to emqx app 4.3.10) some of the RPC handler
            %% exceptions may get propagated to a new version node
            ?SLOG(
                error,
                #{
                    msg => "failed_to_kick_session_on_remote_node",
                    node => node(ChanPid),
                    action => Action,
                    error => Error,
                    reason => Reason
                },
                #{clientid => ClientId}
            )
    end.

kick_session(ClientId) ->
    case lookup_channels(ClientId) of
        [] ->
            ?SLOG(
                warning,
                #{msg => "kicked_an_unknown_session"},
                #{clientid => ClientId}
            ),
            ok;
        ChanPids ->
            case length(ChanPids) > 1 of
                true ->
                    ?SLOG(
                        warning,
                        #{
                            msg => "more_than_one_channel_found",
                            chan_pids => ChanPids
                        },
                        #{clientid => ClientId}
                    );
                false ->
                    ok
            end,
            lists:foreach(fun(Pid) -> kick_session(ClientId, Pid) end, ChanPids)
    end.

%% @doc Is clean start?
% is_clean_start(#{clean_start := false}) -> false;
% is_clean_start(_Attrs) -> true.

with_channel(ClientId, Fun) ->
    case lookup_channels(ClientId) of
        [] -> undefined;
        [Pid] -> Fun(Pid);
        Pids -> Fun(lists:last(Pids))
    end.

%% @doc Get all registered channel pids. Debug/test interface
all_channels() ->
    Pat = [{{'_', '$1'}, [], ['$1']}],
    ets:select(?CHAN_TAB, Pat).

%% @doc Get all registered clientIDs. Debug/test interface
all_client_ids() ->
    Pat = [{{'$1', '_'}, [], ['$1']}],
    ets:select(?CHAN_TAB, Pat).

%% @doc Lookup channels.
-spec lookup_channels(emqx_types:clientid()) -> list(chan_pid()).
lookup_channels(ClientId) ->
    lookup_channels(global, ClientId).

%% @doc Lookup local or global channels.
-spec lookup_channels(local | global, emqx_types:clientid()) -> list(chan_pid()).
lookup_channels(global, ClientId) ->
    case emqx_cm_registry:is_enabled() of
        true ->
            emqx_cm_registry:lookup_channels(ClientId);
        false ->
            lookup_channels(local, ClientId)
    end;
lookup_channels(local, ClientId) ->
    [ChanPid || {_, ChanPid} <- ets:lookup(?CHAN_TAB, ClientId)].

-spec lookup_client({clientid, emqx_types:clientid()} | {username, emqx_types:username()}) ->
    [channel_info()].
lookup_client({username, Username}) ->
    MatchSpec = [
        {{'_', #{clientinfo => #{username => '$1'}}, '_'}, [{'=:=', '$1', Username}], ['$_']}
    ],
    ets:select(emqx_channel_info, MatchSpec);
lookup_client({clientid, ClientId}) ->
    [
        Rec
     || Key <- ets:lookup(emqx_channel, ClientId),
        Rec <- ets:lookup(emqx_channel_info, Key)
    ].

%% @private
wrap_rpc(Result) ->
    case Result of
        {badrpc, Reason} ->
            %% since emqx app 4.3.10, the 'kick' and 'discard' calls handler
            %% should catch all exceptions and always return 'ok'.
            %% This leaves 'badrpc' only possible when there is problem
            %% calling the remote node.
            error({badrpc, Reason});
        Res ->
            Res
    end.

%% @private
cast(Msg) -> gen_server:cast(?CM, Msg).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    TabOpts = [public, {write_concurrency, true}],
    ok = emqx_tables:new(?CHAN_TAB, [bag, {read_concurrency, true} | TabOpts]),
    ok = emqx_tables:new(?CHAN_CONN_TAB, [bag | TabOpts]),
    ok = emqx_tables:new(?CHAN_INFO_TAB, [set, compressed | TabOpts]),
    ok = emqx_tables:new(?CHAN_LIVE_TAB, [set, {write_concurrency, true} | TabOpts]),
    ok = emqx_stats:update_interval(chan_stats, fun ?MODULE:stats_fun/0),
    State = #{chan_pmon => emqx_pmon:new()},
    {ok, State}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast({registered, {ClientId, ChanPid}}, State = #{chan_pmon := PMon}) ->
    PMon1 = emqx_pmon:monitor(ChanPid, ClientId, PMon),
    {noreply, State#{chan_pmon := PMon1}};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State = #{chan_pmon := PMon}) ->
    ?tp(emqx_cm_process_down, #{pid => Pid, reason => _Reason}),
    ChanPids = [Pid | emqx_misc:drain_down(?BATCH_SIZE)],
    {Items, PMon1} = emqx_pmon:erase_all(ChanPids, PMon),
    lists:foreach(fun mark_channel_disconnected/1, ChanPids),
    ok = emqx_pool:async_submit(fun lists:foreach/2, [fun ?MODULE:clean_down/1, Items]),
    {noreply, State#{chan_pmon := PMon1}};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),

    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_stats:cancel_update(chan_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

clean_down({ChanPid, ClientId}) ->
    do_unregister_channel({ClientId, ChanPid}).

stats_fun() ->
    lists:foreach(fun update_stats/1, ?CHAN_STATS).

update_stats({Tab, Stat, MaxStat}) ->
    case ets:info(Tab, size) of
        undefined -> ok;
        Size -> emqx_stats:setstat(Stat, MaxStat, Size)
    end.

-spec do_get_chann_conn_mod(emqx_types:clientid(), chan_pid()) ->
    module() | undefined.
do_get_chann_conn_mod(ClientId, ChanPid) ->
    Chan = {ClientId, ChanPid},
    try
        [ConnMod] = ets:lookup_element(?CHAN_CONN_TAB, Chan, 2),
        ConnMod
    catch
        error:badarg -> undefined
    end.

get_chann_conn_mod(ClientId, ChanPid) ->
    wrap_rpc(emqx_cm_proto_v1:get_chann_conn_mod(ClientId, ChanPid)).

mark_channel_connected(ChanPid) ->
    ?tp(emqx_cm_connected_client_count_inc, #{}),
    ets:insert_new(?CHAN_LIVE_TAB, {ChanPid, true}),
    ok.

mark_channel_disconnected(ChanPid) ->
    ?tp(emqx_cm_connected_client_count_dec, #{}),
    ets:delete(?CHAN_LIVE_TAB, ChanPid),
    ok.

get_connected_client_count() ->
    case ets:info(?CHAN_LIVE_TAB, size) of
        undefined -> 0;
        Size -> Size
    end.
