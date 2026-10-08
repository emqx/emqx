%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The Gateway Channel Manager
%%
%% For a certain type of protocol, this is a single instance of the manager.
%% It means that no matter how many instances of the stomp gateway are created,
%% they all share a single this Connection-Manager
-module(emqx_gateway_cm).

-feature(maybe_expr, enable).

-behaviour(gen_server).

-include("emqx_gateway.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% APIs
-export([start_link/1]).

-export([
    open_session/6,
    discard_session/2,
    kick_session/2,
    kick_session/3,
    register_channel/4,
    unregister_channel/2,
    insert_channel_info/4,
    lookup_by_clientid/2,
    set_chan_info/3,
    set_chan_info/4,
    get_chan_info/2,
    get_chan_info/3,
    get_chan_info/4,
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
    request_stepdown/4,
    do_get_chann_conn_mod/3,
    do_call/4,
    do_call/5,
    do_cast/4
]).

-export_type([
    gateway_name/0,
    open_mode/0
]).

-type open_mode() :: clean | {takeover, emqx_gateway_cm_takeover:mode()}.

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
    wrap_rpc(emqx_gateway_cm_proto_v1:get_chan_info(GwName, ClientId, ChanPid)).

-spec get_chan_info(gateway_name(), emqx_types:clientid(), pid(), timeout()) ->
    emqx_types:infos() | undefined.
get_chan_info(GwName, ClientId, ChanPid, Timeout) ->
    wrap_rpc(
        emqx_gateway_cm_proto_v2:get_chan_info(GwName, ClientId, ChanPid, Timeout)
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
connection_closed(_GwName, _ClientId) ->
    %% Transport close may keep the channel/session alive. The registry/chan/conn/info
    %% cleanup is done by unregister_channel/2 or the DOWN path in do_unregister_channel/3.
    true.

-spec open_session(
    GwName :: gateway_name(),
    Mode :: open_mode(),
    ClientInfo :: emqx_types:clientinfo(),
    ConnInfo :: emqx_types:conninfo(),
    CreateSessionFun ::
        undefined
        | fun(
            (
                emqx_types:clientinfo(),
                emqx_types:conninfo()
            ) -> Session
        ),
    SessionMod :: module()
) ->
    {ok, #{
        session := Session,
        present := boolean(),
        pendings => list(),
        atom() => term()
    }}
    | {error, any()}.

-doc """
Open a session under the ClientId lock.
* Mode `clean` discards existing channels and creates a session.
* Mode `{takeover, force}` attempts ordinary takeover with fallback to a new session.
* Mode `{takeover, {resume, Request}}` only resumes an existing session and passes
  `Request` to the gateway channel takeover handler.
""".
open_session(GwName, clean, ClientInfo, ConnInfo, CreateSessionFun, SessionMod) ->
    #{clientid := ClientId} = ClientInfo,
    locker_trans(GwName, ClientId, fun(_) ->
        _ = discard_session(GwName, ClientId),
        create_register(GwName, ClientInfo, ConnInfo, CreateSessionFun, SessionMod)
    end);
open_session(GwName, {takeover, Mode}, ClientInfo, ConnInfo, CreateSessionFun, SessionMod) ->
    #{clientid := ClientId} = ClientInfo,
    locker_trans(GwName, ClientId, fun(_) ->
        case open_existing_session(GwName, Mode, ClientInfo, ConnInfo, SessionMod) of
            {ok, _} = Result ->
                Result;
            {error, _} when Mode =:= force ->
                create_register(GwName, ClientInfo, ConnInfo, CreateSessionFun, SessionMod);
            {error, _} = Error ->
                Error
        end
    end).

create_register(GwName, ClientInfo, ConnInfo, CreateSessionFun, SessionMod) ->
    #{clientid := ClientId} = ClientInfo,
    Session = create_session(GwName, ClientInfo, ConnInfo, CreateSessionFun, SessionMod),
    register_channel(GwName, ClientId, self(), ConnInfo),
    {ok, #{session => Session, present => false}}.

open_existing_session(GwName, Mode, ClientInfo = #{clientid := ClientId}, ConnInfo, SessionMod) ->
    case select_takeover_candidate(GwName, ClientId) of
        {ok, ChanPid, StalePids} ->
            open_existing_session(
                GwName, Mode, ClientInfo, ConnInfo, SessionMod, ChanPid, StalePids
            );
        {error, _} = Error ->
            Error
    end.

open_existing_session(
    GwName,
    Mode,
    ClientInfo = #{clientid := ClientId},
    ConnInfo,
    SessionMod,
    ChanPid,
    StalePids
) ->
    Attempt = make_ref(),
    case emqx_gateway_cm_takeover:begin_(GwName, ClientId, ChanPid, Mode, Attempt) of
        {ok, Takeover, Data} ->
            case resume_session(GwName, Mode, ClientInfo, ConnInfo, SessionMod, Data) of
                {ok, Resumption} ->
                    discard_stale_channels(GwName, ClientId, StalePids),
                    case emqx_gateway_cm_takeover:finish(GwName, Takeover, Mode, Attempt) of
                        {ok, Pendings} ->
                            {ok, Resumption#{present => true, pendings => Pendings}};
                        {error, Reason} ->
                            %% Preserve the old owner's registration so it can roll back.
                            cleanup_open_registration(GwName, ClientId),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    %% Discard duplicate registrations even if force-takeover fails:
                    discard_stale_channels_on_failure(Mode, GwName, ClientId, StalePids),
                    {error, Reason}
            end;
        {error, Reason} ->
            %% Discard duplicate registrations even if force-takeover fails:
            discard_stale_channels_on_failure(Mode, GwName, ClientId, StalePids),
            {error, Reason}
    end.

resume_session(GwName, force, ClientInfo, ConnInfo, SessionMod, Data) ->
    #{clientid := ClientId} = ClientInfo,
    try
        Session = SessionMod:resume(ClientInfo, maps:get(session, Data)),
        register_channel(GwName, ClientId, self(), ConnInfo),
        {ok, #{session => Session}}
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "gateway_resume_session_failed",
                clientid => ClientId,
                reason => {Class, Reason},
                stacktrace => Stacktrace
            }),
            {error, {resume_failed, Reason}}
    end;
resume_session(GwName, {resume, _}, ClientInfo, ConnInfo, SessionMod, Data) ->
    #{clientid := ClientId} = ClientInfo,
    try
        %% NOTE
        %% The session implementation validates and restores protocol-specific metadata.
        Resumption = SessionMod:resume(ClientInfo, ConnInfo, Data),
        NConnInfo = maps:get(conninfo, Resumption, ConnInfo),
        register_channel(GwName, ClientId, self(), NConnInfo),
        {ok, Resumption}
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "gateway_resume_session_failed",
                clientid => ClientId,
                reason => {Class, Reason},
                stacktrace => Stacktrace
            }),
            {error, {resume_failed, Reason}}
    end.

select_takeover_candidate(GwName, ClientId) ->
    case lookup_channels(GwName, ClientId) of
        [] ->
            {error, not_found};
        ChanPids ->
            [ChanPid | OtherPids] = lists:reverse(ChanPids),
            length(ChanPids) > 1 andalso
                ?SLOG(warning, #{msg => "more_than_one_channel_found", chan_pids => ChanPids}),
            {ok, ChanPid, OtherPids}
    end.

discard_stale_channels_on_failure(_Mode = force, GwName, ClientId, ChanPids) ->
    %% Discard duplicate registrations even if force-takeover fails:
    discard_stale_channels(GwName, ClientId, ChanPids);
discard_stale_channels_on_failure(_Mode, _GwName, _ClientId, _ChanPids) ->
    ok.

discard_stale_channels(GwName, ClientId, ChanPids) ->
    lists:foreach(fun(ChanPid) -> _ = discard_session(GwName, ClientId, ChanPid) end, ChanPids).

cleanup_open_registration(GwName, ClientId) ->
    try
        unregister_channel(GwName, ClientId)
    catch
        _:_ -> ok
    end.

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
        Ctx = #{
            conninfo => ConnInfo
        },
        ok = emqx_hooks:run('session.created', Ctx, [ClientInfo, SessionInfo]),
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

%% Legacy takeover endpoint @ `emqx_gateway_cm_proto_v1`.
%% Current requesters use the separate takeover BPAPI when the owner advertises it.
do_takeover_session(GwName, ClientId, ChanPid) when node(ChanPid) == node() ->
    emqx_gateway_cm_takeover:begin_rpc_legacy(GwName, ClientId, ChanPid).

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

-type from() :: {pid(), reference()}.
-type stepdown_action() ::
    kick
    | discard
    | {takeover, 'begin', from(), undefined | map()}
    | {takeover, 'end', from()}
    %% Legacy forms
    | {takeover, 'begin'}
    | {takeover, 'end'}
    | {takeover, 'begin', map()}.

%% @private Force a stale channel to step down, killing it if the call fails.
-spec request_stepdown(stepdown_action(), module(), pid()) ->
    ok | {ok, term()} | {error, term()}.
request_stepdown(Action, ConnMod, Pid) ->
    request_stepdown(Action, ConnMod, Pid, kill).

%% @private Wakeup takeovers keep the old channel alive on failure for rollback.
-spec request_stepdown(stepdown_action(), module(), pid(), kill | keep) ->
    ok | {ok, term()} | {error, term()}.
request_stepdown(Action, ConnMod, Pid, FailurePolicy) ->
    Timeout =
        case Action == kick orelse Action == discard of
            true -> ?T_KICK;
            _ -> ?T_TAKEOVER
        end,
    Return =
        %% Call directly so legacy channels observe the actual requester as owner.
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
                ok = maybe_kill(Pid, FailurePolicy),
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
                ok = maybe_kill(Pid, FailurePolicy),
                %% Preserve the stepdown error shape regardless of failure policy.
                {error, Error}
        end,
    case Action == kick orelse Action == discard of
        true -> ok;
        _ -> Return
    end.

maybe_kill(Pid, kill) ->
    exit(Pid, kill),
    ok;
maybe_kill(_Pid, keep) ->
    ok.

stale_channel_info(Pid) when node(Pid) =:= node() ->
    process_info(Pid, [status, message_queue_len, current_stacktrace]);
stale_channel_info(_Pid) ->
    remote.

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

-spec do_get_chann_conn_mod(gateway_name(), emqx_types:clientid(), pid()) ->
    atom() | undefined.
do_get_chann_conn_mod(GwName, ClientId, ChanPid) ->
    Chan = {ClientId, ChanPid},
    try
        [ConnMod] = ets:lookup_element(tabname(conn, GwName), Chan, 2),
        ConnMod
    catch
        error:badarg -> undefined
    end.

-spec get_chann_conn_mod(gateway_name(), emqx_types:clientid(), pid()) ->
    atom() | undefined.
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

    %% Interval update stats
    %% TODO: v0.2
    %ok = emqx_stats:update_interval(chan_stats, fun ?MODULE:stats_fun/0),

    case start_cm_children(GwName) of
        {ok, Registry} ->
            {ok, #state{
                gwname = GwName,
                registry = Registry,
                chan_pmon = emqx_pmon:new()
            }};
        {error, Reason} ->
            {stop, Reason}
    end.

%% Start the per-gateway registry and locker processes.
%%
%% XXX: These are linked to this gen_server rather than supervised as proper
%% children (tracked as a follow-up). Because they are not supervised, a
%% gateway load that aborts partway through can leave the named locker behind;
%% the next load would then crash with `{already_started, _}'. To keep
%% (re)loads robust we reclaim any leftover locker, and we tear down whatever
%% we already started should a later step fail.
start_cm_children(GwName) ->
    case emqx_gateway_cm_registry:start_link(GwName) of
        {ok, Registry} ->
            case ensure_locker_started(GwName) of
                ok ->
                    {ok, Registry};
                {error, _} = Err ->
                    _ = gen_server:stop(Registry),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

ensure_locker_started(GwName) ->
    LockerName = lockername(GwName),
    case ekka_locker:start_link(LockerName) of
        {ok, _Pid} ->
            ok;
        ignore ->
            ok;
        {error, {already_started, OldPid}} ->
            %% A locker left behind by a previously aborted load of this
            %% gateway. There is only one cm per gateway, so this registered
            %% name can only belong to an orphan; reclaim it and retry once.
            ok = reclaim_orphan(OldPid),
            case ekka_locker:start_link(LockerName) of
                {ok, _Pid} -> ok;
                ignore -> ok;
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% Synchronously take down an orphan process and wait until it is gone so its
%% registered name is freed before we retry.
reclaim_orphan(Pid) ->
    MRef = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', MRef, process, Pid, _Reason} -> ok
    after 5000 ->
        erlang:demonitor(MRef, [flush]),
        ok
    end.

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
