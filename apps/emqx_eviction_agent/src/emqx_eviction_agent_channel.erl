%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% MQTT Channel
-module(emqx_eviction_agent_channel).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_channel.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    start_link/1,
    start_supervised/1,
    call/2,
    call/3,
    cast/2,
    stop/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-type opts() :: #{
    conninfo := emqx_types:conninfo(),
    clientinfo := emqx_types:clientinfo(),
    will_message => emqx_maybe:t(emqx_types:message())
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_supervised(opts()) -> supervisor:startchild_ret().
start_supervised(#{clientinfo := #{clientid := ClientId}} = Opts) ->
    RandomId = integer_to_binary(erlang:unique_integer([positive])),
    ClientIdBin = bin_clientid(ClientId),
    Id = <<ClientIdBin/binary, "-", RandomId/binary>>,
    ChildSpec = #{
        id => Id,
        start => {?MODULE, start_link, [Opts]},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    },
    supervisor:start_child(
        emqx_eviction_agent_conn_sup,
        ChildSpec
    ).

-spec start_link(opts()) -> startlink_ret().
start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

-spec cast(pid(), term()) -> ok.
cast(Pid, Req) ->
    gen_server:cast(Pid, Req).

-spec call(pid(), term()) -> term().
call(Pid, Req) ->
    call(Pid, Req, infinity).

-spec call(pid(), term(), timeout()) -> term().
call(Pid, Req, Timeout) ->
    gen_server:call(Pid, Req, Timeout).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% gen_server API
%%--------------------------------------------------------------------

init([#{conninfo := OldConnInfo, clientinfo := #{clientid := ClientId} = OldClientInfo} = Opts]) ->
    process_flag(trap_exit, true),
    ClientInfo = clientinfo(OldClientInfo),
    ConnInfo = conninfo(OldConnInfo),
    MaybeWillMsg = maps:get(will_message, Opts, undefined),
    case open_session(ConnInfo, ClientInfo, MaybeWillMsg) of
        {ok, Channel0} ->
            case set_expiry_timer(Channel0) of
                {ok, Channel1} ->
                    ?SLOG(
                        info,
                        #{
                            msg => "channel_initialized",
                            clientid => ClientId,
                            node => node()
                        }
                    ),
                    ok = emqx_cm:mark_channel_disconnected(self()),
                    {ok, Channel1, hibernate};
                {error, Reason} ->
                    {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(kick, _From, Channel) ->
    {stop, kicked, ok, Channel};
handle_call(discard, _From, Channel) ->
    {stop, discarded, ok, Channel};
handle_call({takeover, 'begin'}, _From, #{session := Session} = Channel) ->
    {reply, Session, Channel#{takeover => true}};
handle_call(
    {takeover, 'end'},
    _From,
    #{
        session := Session,
        clientinfo := #{clientid := ClientId},
        pendings := Pendings
    } = Channel
) ->
    % NOTE
    % This is essentially part of `emqx_session_mem` logic, thus call it directly.
    ok = emqx_session_mem:takeover(Session),
    %% TODO: Should not drain deliver here (side effect)
    Delivers = emqx_utils:drain_deliver(),
    AllPendings = lists:append(Delivers, Pendings),
    ?tp(
        debug,
        emqx_channel_takeover_end,
        #{clientid => ClientId}
    ),
    {stop, normal, AllPendings, Channel};
handle_call(list_acl_cache, _From, Channel) ->
    {reply, [], Channel};
handle_call({quota, _Policy}, _From, Channel) ->
    {reply, ok, Channel};
handle_call(Req, _From, Channel) ->
    ?SLOG(
        error,
        #{
            msg => "unexpected_call",
            req => Req
        }
    ),
    {reply, ignored, Channel}.

handle_info(Deliver = {deliver, _Topic, _Msg}, Channel) ->
    Delivers = [Deliver | emqx_utils:drain_deliver()],
    {noreply, handle_deliver(Delivers, Channel)};
handle_info(expire_session, Channel) ->
    {stop, expired, Channel};
handle_info(Info, Channel) ->
    ?SLOG(
        error,
        #{
            msg => "unexpected_info",
            info => Info
        }
    ),
    {noreply, Channel}.

handle_cast(Msg, Channel) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, Channel}.

terminate(Reason, #{clientinfo := ClientInfo, session := Session} = Channel) ->
    ok = cancel_expiry_timer(Channel),
    emqx_session:terminate(ClientInfo, Reason, Session).

code_change(_OldVsn, Channel, _Extra) ->
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_deliver(
    Delivers,
    #{
        takeover := true,
        pendings := Pendings
    } = Channel
) ->
    %% NOTE: Order is important here. While the takeover is in
    %% progress, the session cannot enqueue messages, since it already
    %% passed on the queue to the new connection in the session state.
    NPendings = lists:append(Pendings, emqx_channel:maybe_nack(Delivers)),
    Channel#{pendings => NPendings};
handle_deliver(
    Delivers,
    #{
        takeover := false,
        session := Session,
        clientinfo := ClientInfo
    } = Channel
) ->
    % NOTE
    % This is essentially part of `emqx_session_mem` logic, thus call it directly.
    Delivers1 = emqx_channel:maybe_nack(Delivers),
    Messages = emqx_session:enrich_delivers(ClientInfo, Delivers1, Session),
    NSession = emqx_session_mem:enqueue(ClientInfo, Messages, Session),
    Channel#{session := NSession}.

cancel_expiry_timer(#{expiry_timer := TRef}) when is_reference(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ok;
cancel_expiry_timer(_) ->
    ok.

set_expiry_timer(#{conninfo := ConnInfo} = Channel) ->
    case maps:get(expiry_interval, ConnInfo) of
        ?EXPIRE_INTERVAL_INFINITE ->
            {ok, Channel};
        I when I > 0 ->
            Timer = erlang:send_after(I, self(), expire_session),
            {ok, Channel#{expiry_timer => Timer}};
        _ ->
            {error, should_be_expired}
    end.

open_session(ConnInfo, #{clientid := ClientId} = ClientInfo, MaybeWillMsg) ->
    Channel = channel(ConnInfo, ClientInfo),
    case emqx_cm:open_session(_CleanSession = false, ClientInfo, ConnInfo, MaybeWillMsg) of
        {ok, #{present := false}} ->
            ?SLOG(
                info,
                #{
                    msg => "no_session",
                    clientid => ClientId,
                    node => node()
                }
            ),
            {error, no_session};
        {ok, #{session := Session, present := true, replay := Pendings}} ->
            ?SLOG(
                info,
                #{
                    msg => "session_opened",
                    clientid => ClientId,
                    node => node()
                }
            ),
            % NOTE
            % Here we aggregate and deduplicate remote and local pending deliveries,
            % throwing away any local deliveries that are part of some shared
            % subscription. Remote deliviries pertaining to shared subscriptions should
            % already have been thrown away by `emqx_channel:handle_deliver/2`.
            % See also: `emqx_channel:maybe_resume_session/1`, `emqx_session_mem:replay/3`.
            DeliversLocal = emqx_channel:maybe_nack(emqx_utils:drain_deliver()),
            PendingsAll = emqx_session_mem:dedup(ClientInfo, Pendings, DeliversLocal, Session),
            NSession = emqx_session_mem:enqueue(ClientInfo, PendingsAll, Session),
            NChannel = Channel#{session => NSession},
            ok = emqx_cm:insert_channel_info(ClientId, info(NChannel), stats(NChannel)),
            ?SLOG(
                info,
                #{
                    msg => "channel_info_updated",
                    clientid => ClientId,
                    node => node()
                }
            ),
            {ok, NChannel};
        {error, Reason} = Error ->
            ?SLOG(
                error,
                #{
                    msg => "session_open_failed",
                    clientid => ClientId,
                    node => node(),
                    reason => Reason
                }
            ),
            Error
    end.

conninfo(OldConnInfo) ->
    DisconnectedAt = maps:get(disconnected_at, OldConnInfo, erlang:system_time(millisecond)),
    ConnInfo0 = maps:with(
        [
            socktype,
            sockname,
            peername,
            peercert,
            clientid,
            clean_start,
            receive_maximum,
            expiry_interval,
            connected_at,
            disconnected_at,
            keepalive
        ],
        OldConnInfo
    ),
    ConnInfo0#{
        conn_mod => ?MODULE,
        connected => false,
        disconnected_at => DisconnectedAt
    }.

clientinfo(OldClientInfo) ->
    maps:with(
        [
            zone,
            protocol,
            peerhost,
            peername,
            sockport,
            clientid,
            username,
            is_bridge,
            is_superuser,
            mountpoint
        ],
        OldClientInfo
    ).

channel(ConnInfo, ClientInfo) ->
    #{
        conninfo => ConnInfo,
        clientinfo => ClientInfo,
        expiry_timer => undefined,
        takeover => false,
        resuming => false,
        pendings => []
    }.

info(Channel) ->
    #{
        conninfo => maps:get(conninfo, Channel, undefined),
        clientinfo => maps:get(clientinfo, Channel, undefined),
        session => emqx_utils:maybe_apply(
            fun emqx_session:info/1,
            maps:get(session, Channel, undefined)
        ),
        conn_state => disconnected
    }.

stats(#{session := Session}) ->
    lists:append(emqx_session:stats(Session), emqx_pd:get_counters(?CHANNEL_METRICS)).

bin_clientid(ClientId) when is_binary(ClientId) ->
    ClientId;
bin_clientid(ClientId) when is_atom(ClientId) ->
    atom_to_binary(ClientId).
