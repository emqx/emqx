%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The Gateway Channel Manager
%%
%% For a certain type of protocol, this is a single instance of the manager.
%% It means that no matter how many instances of the stomp gateway are created,
%% they all share a single this Connection-Manager
-module(emqx_gateway_cm).

-behaviour(gen_server).

-include("emqx_gateway.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% APIs
-export([start_link/1]).

-export([
    open_session/5,
    open_session/6,
    discard_session/2,
    kick_session/2,
    kick_session/3,
    takeover_session/2,
    register_channel/4,
    unregister_channel/2,
    insert_channel_info/4,
    lookup_by_clientid/2,
    set_chan_info/3,
    set_chan_info/4,
    get_chan_info/2,
    get_chan_info/3,
    set_chan_stats/3,
    set_chan_stats/4,
    get_chan_stats/2,
    get_chan_stats/3,
    connection_closed/2
]).

-export([
    call/3,
    call/4,
    cast/3
]).

-export([
    with_channel/3,
    lookup_channels/2
]).

%% Internal funcs for getting tabname by GatewayId
-export([cmtabs/1, tabname/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% RPC targets
-export([
    do_lookup_by_clientid/2,
    do_get_chan_info/3,
    do_set_chan_info/4,
    do_get_chan_stats/3,
    do_set_chan_stats/4,
    do_kick_session/4,
    do_takeover_session/3,
    do_get_chann_conn_mod/3,
    do_call/4,
    do_call/5,
    do_cast/4
]).

-export_type([gateway_name/0]).

-record(state, {
    %% Gateway Name
    gwname :: gateway_name(),
    %% ClientId Registry server
    registry :: pid(),
    chan_pmon :: emqx_pmon:pmon()
}).

-type option() :: {gwname, gateway_name()}.
-type options() :: list(option()).

-define(T_KICK, 5000).
-define(T_GET_INFO, 5000).
-define(T_TAKEOVER, 15000).
-define(DEFAULT_BATCH_SIZE, 10000).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link(options()) -> {ok, pid()} | ignore | {error, any()}.
start_link(Options) ->
    GwName = proplists:get_value(gwname, Options),
    gen_server:start_link({local, procname(GwName)}, ?MODULE, Options, []).

procname(GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_cm'])).

-spec cmtabs(GwName :: gateway_name()) ->
    {ChanTab :: atom(), ConnTab :: atom(), ChannInfoTab :: atom()}.
cmtabs(GwName) ->
    %% Record: {ClientId, Pid}
    {
        tabname(chan, GwName),
        %% Record: {{ClientId, Pid}, ConnMod}
        tabname(conn, GwName),
        %% Record: {{ClientId, Pid}, Info, Stats}
        tabname(info, GwName)
    }.

tabname(chan, GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_channel']));
tabname(conn, GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_channel_conn']));
tabname(info, GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_channel_info'])).

lockername(GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_locker'])).

-spec register_channel(
    gateway_name(),
    emqx_types:clientid(),
    pid(),
    emqx_types:conninfo()
) -> ok.
register_channel(GwName, ClientId, ChanPid, #{conn_mod := ConnMod}) when is_pid(ChanPid) ->
    Chan = {ClientId, ChanPid},
    true = ets:insert(tabname(chan, GwName), Chan),
    true = ets:insert(tabname(conn, GwName), {Chan, ConnMod}),
    ok = emqx_gateway_cm_registry:register_channel(GwName, Chan),
    cast(procname(GwName), {registered, Chan}).

%% @doc Unregister a channel.
-spec unregister_channel(gateway_name(), emqx_types:clientid()) -> ok.
unregister_channel(GwName, ClientId) when is_binary(ClientId) ->
    true = do_unregister_channel(GwName, {ClientId, self()}, cmtabs(GwName)),
    ok.

%% @doc Insert/Update the channel info and stats
-spec insert_channel_info(
    gateway_name(),
    emqx_types:clientid(),
    emqx_types:infos(),
    emqx_types:stats()
) -> ok.
insert_channel_info(GwName, ClientId, Info, Stats) ->
    Chan = {ClientId, self()},
    true = ets:insert(tabname(info, GwName), {Chan, Info, Stats}),
    ok.

%% @doc Get info of a channel.
-spec get_chan_info(gateway_name(), emqx_types:clientid()) ->
    emqx_types:infos() | undefined.
get_chan_info(GwName, ClientId) ->
    with_channel(
        GwName,
        ClientId,
        fun(ChanPid) ->
            get_chan_info(GwName, ClientId, ChanPid)
        end
    ).

-spec do_lookup_by_clientid(gateway_name(), emqx_types:clientid()) -> [pid()].
do_lookup_by_clientid(GwName, ClientId) ->
    ChanTab = emqx_gateway_cm:tabname(chan, GwName),
    [Pid || {_, Pid} <- ets:lookup(ChanTab, ClientId)].

-spec do_get_chan_info(gateway_name(), emqx_types:clientid(), pid()) ->
    emqx_types:infos() | undefined.
do_get_chan_info(GwName, ClientId, ChanPid) ->
    Chan = {ClientId, ChanPid},
    try
        Info = ets:lookup_element(tabname(info, GwName), Chan, 2),
        Info#{node => node()}
    catch
        error:badarg -> undefined
    end.

-spec get_chan_info(gateway_name(), emqx_types:clientid(), pid()) ->
    emqx_types:infos() | undefined.
get_chan_info(GwName, ClientId, ChanPid) ->
    wrap_rpc(
        emqx_gateway_cm_proto_v1:get_chan_info(GwName, ClientId, ChanPid)
    ).

-spec lookup_by_clientid(gateway_name(), emqx_types:clientid()) -> [pid()].
lookup_by_clientid(GwName, ClientId) ->
    Nodes = mria:running_nodes(),
    case
        emqx_gateway_cm_proto_v1:lookup_by_clientid(
            Nodes, GwName, ClientId
        )
    of
        {Pids, []} ->
            lists:append(Pids);
        {_, _BadNodes} ->
            error(badrpc)
    end.

%% @doc Update infos of the channel.
-spec set_chan_info(
    gateway_name(),
    emqx_types:clientid(),
    emqx_types:infos()
) -> boolean().
set_chan_info(GwName, ClientId, Infos) ->
    set_chan_info(GwName, ClientId, self(), Infos).

-spec do_set_chan_info(
    gateway_name(),
    emqx_types:clientid(),
    pid(),
    emqx_types:infos()
) -> boolean().
do_set_chan_info(GwName, ClientId, ChanPid, Infos) ->
    Chan = {ClientId, ChanPid},
    try
        ets:update_element(tabname(info, GwName), Chan, {2, Infos})
    catch
        error:badarg -> false
    end.

-spec set_chan_info(
    gateway_name(),
    emqx_types:clientid(),
    pid(),
    emqx_types:infos()
) -> boolean().
set_chan_info(GwName, ClientId, ChanPid, Infos) ->
    wrap_rpc(emqx_gateway_cm_proto_v1:set_chan_info(GwName, ClientId, ChanPid, Infos)).

%% @doc Get channel's stats.
-spec get_chan_stats(gateway_name(), emqx_types:clientid()) ->
    emqx_types:stats() | undefined.
get_chan_stats(GwName, ClientId) ->
    with_channel(
        GwName,
        ClientId,
        fun(ChanPid) ->
            get_chan_stats(GwName, ClientId, ChanPid)
        end
    ).

-spec do_get_chan_stats(gateway_name(), emqx_types:clientid(), pid()) ->
    emqx_types:stats() | undefined.
do_get_chan_stats(GwName, ClientId, ChanPid) ->
    Chan = {ClientId, ChanPid},
    try
        ets:lookup_element(tabname(info, GwName), Chan, 3)
    catch
        error:badarg -> undefined
    end.

-spec get_chan_stats(gateway_name(), emqx_types:clientid(), pid()) ->
    emqx_types:stats() | undefined.
get_chan_stats(GwName, ClientId, ChanPid) ->
    wrap_rpc(emqx_gateway_cm_proto_v1:get_chan_stats(GwName, ClientId, ChanPid)).

-spec set_chan_stats(
    gateway_name(),
    emqx_types:clientid(),
    emqx_types:stats()
) -> boolean().
set_chan_stats(GwName, ClientId, Stats) ->
    set_chan_stats(GwName, ClientId, self(), Stats).

-spec do_set_chan_stats(
    gateway_name(),
    emqx_types:clientid(),
    pid(),
    emqx_types:stats()
) -> boolean().
do_set_chan_stats(GwName, ClientId, ChanPid, Stats) ->
    Chan = {ClientId, ChanPid},
    try
        ets:update_element(tabname(info, GwName), Chan, {3, Stats})
    catch
        error:badarg -> false
    end.

-spec set_chan_stats(
    gateway_name(),
    emqx_types:clientid(),
    pid(),
    emqx_types:stats()
) -> boolean().
set_chan_stats(GwName, ClientId, ChanPid, Stats) ->
    wrap_rpc(emqx_gateway_cm_proto_v1:set_chan_stats(GwName, ClientId, ChanPid, Stats)).

-spec connection_closed(gateway_name(), emqx_types:clientid()) -> true.
connection_closed(GwName, ClientId) ->
    %% XXX: Why we need to delete conn_mod tab ???
    Chan = {ClientId, self()},
    ets:delete_object(tabname(conn, GwName), Chan).

-spec open_session(
    GwName :: gateway_name(),
    CleanStart :: boolean(),
    ClientInfo :: emqx_types:clientinfo(),
    ConnInfo :: emqx_types:conninfo(),
    CreateSessionFun :: fun(
        (
            emqx_types:clientinfo(),
            emqx_types:conninfo()
        ) -> Session
    )
) ->
    {ok, #{
        session := Session,
        present := boolean(),
        pendings => list()
    }}
    | {error, any()}.

open_session(GwName, CleanStart, ClientInfo, ConnInfo, CreateSessionFun) ->
    open_session(GwName, CleanStart, ClientInfo, ConnInfo, CreateSessionFun, emqx_session).

open_session(GwName, true = _CleanStart, ClientInfo, ConnInfo, CreateSessionFun, SessionMod) ->
    Self = self(),
    ClientId = maps:get(clientid, ClientInfo),
    Fun = fun(_) ->
        _ = discard_session(GwName, ClientId),
        Session = create_session(
            GwName,
            ClientInfo,
            ConnInfo,
            CreateSessionFun,
            SessionMod
        ),
        register_channel(GwName, ClientId, Self, ConnInfo),
        {ok, #{session => Session, present => false}}
    end,
    locker_trans(GwName, ClientId, Fun);
open_session(
    GwName,
    false = _CleanStart,
    ClientInfo = #{clientid := ClientId},
    ConnInfo,
    CreateSessionFun,
    SessionMod
) ->
    Self = self(),

    ResumeStart =
        fun(_) ->
            CreateSess =
                fun() ->
                    Session = create_session(
                        GwName,
                        ClientInfo,
                        ConnInfo,
                        CreateSessionFun,
                        SessionMod
                    ),
                    register_channel(
                        GwName, ClientId, Self, ConnInfo
                    ),
                    {ok, #{session => Session, present => false}}
                end,
            case takeover_session(GwName, ClientId) of
                {ok, ConnMod, ChanPid, SessionIn} ->
                    Session = SessionMod:resume(ClientInfo, SessionIn),
                    case request_stepdown({takeover, 'end'}, ConnMod, ChanPid) of
                        {ok, Pendings} ->
                            register_channel(
                                GwName, ClientId, Self, ConnInfo
                            ),
                            {ok, #{
                                session => Session,
                                present => true,
                                pendings => Pendings
                            }};
                        {error, _} ->
                            CreateSess()
                    end;
                {error, _Reason} ->
                    CreateSess()
            end
        end,
    locker_trans(GwName, ClientId, ResumeStart).

%% @private
create_session(GwName, ClientInfo, ConnInfo, CreateSessionFun, SessionMod) ->
    try
        Session = emqx_gateway_utils:apply(
            CreateSessionFun,
            [ClientInfo, ConnInfo]
        ),
        ok = emqx_gateway_metrics:inc(GwName, 'session.created'),
        SessionInfo =
            case
                is_tuple(Session) andalso
                    element(1, Session) == session
            of
                true ->
                    SessionMod:info(Session);
                _ ->
                    case is_map(Session) of
                        false ->
                            throw(session_structure_should_be_map);
                        _ ->
                            Session
                    end
            end,
        ok = emqx_hooks:run('session.created', [ClientInfo, SessionInfo]),
        Session
    catch
        Class:Reason:Stk ->
            ?SLOG(error, #{
                msg => "failed_create_session",
                clientid => maps:get(clientid, ClientInfo, undefined),
                username => maps:get(username, ClientInfo, undefined),
                reason => {Class, Reason},
                stacktrace => Stk
            }),
            throw(Reason)
    end.

%% @doc Try to takeover a session.
-spec takeover_session(gateway_name(), emqx_types:clientid()) ->
    {error, term()}
    | {ok, atom(), pid(), emqx_session:session()}.
takeover_session(GwName, ClientId) ->
    case lookup_channels(GwName, ClientId) of
        [] ->
            {error, not_found};
        [ChanPid] ->
            do_takeover_session(GwName, ClientId, ChanPid);
        ChanPids ->
            [ChanPid | StalePids] = lists:reverse(ChanPids),
            ?SLOG(warning, #{
                msg => "more_than_one_channel_found",
                chan_pids => ChanPids
            }),
            lists:foreach(
                fun(StalePid) ->
                    catch discard_session(GwName, ClientId, StalePid)
                end,
                StalePids
            ),
            do_takeover_session(GwName, ClientId, ChanPid)
    end.

do_takeover_session(GwName, ClientId, ChanPid) when node(ChanPid) == node() ->
    case get_chann_conn_mod(GwName, ClientId, ChanPid) of
        undefined ->
            {error, not_found};
        ConnMod when is_atom(ConnMod) ->
            case request_stepdown({takeover, 'begin'}, ConnMod, ChanPid) of
                {ok, Session} ->
                    {ok, ConnMod, ChanPid, Session};
                {error, Reason} ->
                    {error, Reason}
            end
    end;
do_takeover_session(GwName, ClientId, ChanPid) ->
    wrap_rpc(emqx_gateway_cm_proto_v1:takeover_session(GwName, ClientId, ChanPid)).

%% @doc Discard all the sessions identified by the ClientId.
-spec discard_session(GwName :: gateway_name(), binary()) -> ok | {error, not_found}.
discard_session(GwName, ClientId) when is_binary(ClientId) ->
    case lookup_channels(GwName, ClientId) of
        [] -> {error, not_found};
        ChanPids -> lists:foreach(fun(Pid) -> discard_session(GwName, ClientId, Pid) end, ChanPids)
    end.

discard_session(GwName, ClientId, ChanPid) ->
    kick_session(GwName, discard, ClientId, ChanPid).

-spec kick_session(gateway_name(), emqx_types:clientid()) -> ok | {error, not_found}.
kick_session(GwName, ClientId) ->
    case lookup_channels(GwName, ClientId) of
        [] ->
            {error, not_found};
        ChanPids ->
            length(ChanPids) > 1 andalso
                begin
                    ?SLOG(
                        warning,
                        #{
                            msg => "more_than_one_channel_found",
                            chan_pids => ChanPids
                        },
                        #{clientid => ClientId}
                    )
                end,
            lists:foreach(
                fun(Pid) ->
                    _ = kick_session(GwName, ClientId, Pid)
                end,
                ChanPids
            )
    end.

kick_session(GwName, ClientId, ChanPid) ->
    kick_session(GwName, kick, ClientId, ChanPid).

%% @private This function is shared for session 'kick' and 'discard' (as the first arg Action).
kick_session(GwName, Action, ClientId, ChanPid) ->
    try
        wrap_rpc(emqx_gateway_cm_proto_v1:kick_session(GwName, Action, ClientId, ChanPid))
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

-spec do_kick_session(
    gateway_name(),
    kick | discard,
    emqx_types:clientid(),
    pid()
) -> ok.
do_kick_session(GwName, Action, ClientId, ChanPid) ->
    case get_chann_conn_mod(GwName, ClientId, ChanPid) of
        undefined ->
            ok;
        ConnMod when is_atom(ConnMod) ->
            ok = request_stepdown(Action, ConnMod, ChanPid)
    end.

%% @private call a local stale session to execute an Action.
%% If failed to response (e.g. timeout) force a kill.
%% Keeping the stale pid around, or returning error or raise an exception
%% benefits nobody.
-spec request_stepdown(Action, module(), pid()) ->
    ok
    | {ok, emqx_session:session() | list(emqx_types:deliver())}
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
        %% this is essentailly a gen_server:call implemented in emqx_connection
        %% and emqx_ws_connection.
        %% the handle_call is implemented in emqx_channel
        try apply(ConnMod, call, [Pid, Action, Timeout]) of
            ok -> ok;
            Reply -> {ok, Reply}
        catch
            % emqx_ws_connection: call
            _:noproc ->
                ok = ?tp(debug, "session_already_gone", #{stale_pid => Pid, action => Action}),
                {error, noproc};
            % emqx_connection: gen_server:call
            _:{noproc, _} ->
                ok = ?tp(debug, "session_already_gone", #{stale_pid => Pid, action => Action}),
                {error, noproc};
            _:Reason = {shutdown, _} ->
                ok = ?tp(debug, "session_already_shutdown", #{stale_pid => Pid, action => Action}),
                {error, Reason};
            _:Reason = {{shutdown, _}, _} ->
                ok = ?tp(debug, "session_already_shutdown", #{stale_pid => Pid, action => Action}),
                {error, Reason};
            _:{timeout, {gen_server, call, _}} ->
                ?tp(
                    warning,
                    "session_stepdown_request_timeout",
                    #{
                        stale_pid => Pid,
                        action => Action,
                        stale_channel => stale_channel_info(Pid)
                    }
                ),
                ok = force_kill(Pid),
                {error, timeout};
            _:Error:St ->
                ?tp(
                    error,
                    "session_stepdown_request_exception",
                    #{
                        stale_pid => Pid,
                        action => Action,
                        reason => Error,
                        stacktrace => St,
                        stale_channel => stale_channel_info(Pid)
                    }
                ),
                ok = force_kill(Pid),
                {error, Error}
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

with_channel(GwName, ClientId, Fun) ->
    case lookup_channels(GwName, ClientId) of
        [] -> undefined;
        [Pid] -> Fun(Pid);
        Pids -> Fun(lists:last(Pids))
    end.

%% @doc Lookup channels.
-spec lookup_channels(gateway_name(), emqx_types:clientid()) -> list(pid()).
lookup_channels(GwName, ClientId) ->
    emqx_gateway_cm_registry:lookup_channels(GwName, ClientId).

-spec do_get_chann_conn_mod(gateway_name(), emqx_types:clientid(), pid()) -> atom().
do_get_chann_conn_mod(GwName, ClientId, ChanPid) ->
    Chan = {ClientId, ChanPid},
    try
        [ConnMod] = ets:lookup_element(tabname(conn, GwName), Chan, 2),
        ConnMod
    catch
        error:badarg -> undefined
    end.

-spec get_chann_conn_mod(gateway_name(), emqx_types:clientid(), pid()) -> atom().
get_chann_conn_mod(GwName, ClientId, ChanPid) ->
    wrap_rpc(emqx_gateway_cm_proto_v1:get_chann_conn_mod(GwName, ClientId, ChanPid)).

-spec call(gateway_name(), emqx_types:clientid(), term()) ->
    undefined | term().
call(GwName, ClientId, Req) ->
    with_channel(
        GwName,
        ClientId,
        fun(ChanPid) ->
            wrap_rpc(
                emqx_gateway_cm_proto_v1:call(GwName, ClientId, ChanPid, Req)
            )
        end
    ).

-spec call(gateway_name(), emqx_types:clientid(), term(), timeout()) ->
    undefined | term().
call(GwName, ClientId, Req, Timeout) ->
    with_channel(
        GwName,
        ClientId,
        fun(ChanPid) ->
            wrap_rpc(
                emqx_gateway_cm_proto_v1:call(
                    GwName, ClientId, ChanPid, Req, Timeout
                )
            )
        end
    ).

do_call(GwName, ClientId, ChanPid, Req) ->
    case do_get_chann_conn_mod(GwName, ClientId, ChanPid) of
        undefined -> undefined;
        ConnMod -> ConnMod:call(ChanPid, Req)
    end.

do_call(GwName, ClientId, ChanPid, Req, Timeout) ->
    case do_get_chann_conn_mod(GwName, ClientId, ChanPid) of
        undefined -> undefined;
        ConnMod -> ConnMod:call(ChanPid, Req, Timeout)
    end.

-spec cast(gateway_name(), emqx_types:clientid(), term()) -> undefined | ok.
cast(GwName, ClientId, Req) ->
    with_channel(
        GwName,
        ClientId,
        fun(ChanPid) ->
            wrap_rpc(
                emqx_gateway_cm_proto_v1:cast(GwName, ClientId, ChanPid, Req)
            )
        end
    ),
    ok.

do_cast(GwName, ClientId, ChanPid, Req) ->
    case do_get_chann_conn_mod(GwName, ClientId, ChanPid) of
        undefined -> undefined;
        ConnMod -> ConnMod:cast(ChanPid, Req)
    end.

%% Locker

locker_trans(_GwName, undefined, Fun) ->
    Fun([]);
locker_trans(GwName, ClientId, Fun) ->
    Locker = lockername(GwName),
    case locker_lock(Locker, ClientId) of
        {true, Nodes} ->
            try
                Fun(Nodes)
            after
                locker_unlock(Locker, ClientId)
            end;
        {false, _Nodes} ->
            {error, client_id_unavailable}
    end.

locker_lock(Locker, ClientId) ->
    ekka_locker:acquire(Locker, ClientId, quorum).

locker_unlock(Locker, ClientId) ->
    ekka_locker:release(Locker, ClientId, quorum).

%% @private
wrap_rpc(Ret) ->
    case Ret of
        {badrpc, Reason} -> throw({badrpc, Reason});
        Res -> Res
    end.

cast(Name, Msg) ->
    gen_server:cast(Name, Msg).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(Options) ->
    GwName = proplists:get_value(gwname, Options),

    TabOpts = [public, {write_concurrency, true}],

    {ChanTab, ConnTab, InfoTab} = cmtabs(GwName),
    ok = emqx_utils_ets:new(ChanTab, [bag, {read_concurrency, true} | TabOpts]),
    ok = emqx_utils_ets:new(ConnTab, [bag | TabOpts]),
    ok = emqx_utils_ets:new(InfoTab, [ordered_set, compressed | TabOpts]),

    %% Start link cm-registry process
    %% XXX: Should I hang it under a higher level supervisor?
    {ok, Registry} = emqx_gateway_cm_registry:start_link(GwName),

    %% Start locker process
    {ok, _LockerPid} = ekka_locker:start_link(lockername(GwName)),

    %% Interval update stats
    %% TODO: v0.2
    %ok = emqx_stats:update_interval(chan_stats, fun ?MODULE:stats_fun/0),

    {ok, #state{
        gwname = GwName,
        registry = Registry,
        chan_pmon = emqx_pmon:new()
    }}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({registered, {ClientId, ChanPid}}, State = #state{chan_pmon = PMon}) ->
    PMon1 = emqx_pmon:monitor(ChanPid, ClientId, PMon),
    {noreply, State#state{chan_pmon = PMon1}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(
    {'DOWN', _MRef, process, Pid, _Reason},
    State = #state{gwname = GwName, chan_pmon = PMon}
) ->
    ChanPids = [Pid | emqx_utils:drain_down(?DEFAULT_BATCH_SIZE)],
    {Items, PMon1} = emqx_pmon:erase_all(ChanPids, PMon),

    CmTabs = cmtabs(GwName),
    ok = emqx_pool:async_submit(fun do_unregister_channel_task/3, [Items, GwName, CmTabs]),
    {noreply, State#state{chan_pmon = PMon1}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{registry = Registry, gwname = GwName}) ->
    _ = gen_server:stop(Registry),
    _ = ekka_locker:stop(lockername(GwName)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_unregister_channel_task(Items, GwName, CmTabs) ->
    lists:foreach(
        fun({ChanPid, ClientId}) ->
            try
                do_unregister_channel(GwName, {ClientId, ChanPid}, CmTabs)
            catch
                error:badarg -> ok
            end
        end,
        Items
    ).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

do_unregister_channel(GwName, Chan, {ChanTab, ConnTab, InfoTab}) ->
    ok = emqx_gateway_cm_registry:unregister_channel(GwName, Chan),
    true = ets:delete(ConnTab, Chan),
    true = ets:delete(InfoTab, Chan),
    ets:delete_object(ChanTab, Chan).
