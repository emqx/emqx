%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%%--------------------------------------------------------------------
%% @doc
%% A stateful interaction between a Client and a Server. Some Sessions
%% last only as long as the Network Connection, others can span multiple
%% consecutive Network Connections between a Client and a Server.
%%
%% The Session State in the Server consists of:
%%
%% The existence of a Session, even if the rest of the Session State is empty.
%%
%% The Clients subscriptions, including any Subscription Identifiers.
%%
%% QoS 1 and QoS 2 messages which have been sent to the Client, but have not
%% been completely acknowledged.
%%
%% QoS 1 and QoS 2 messages pending transmission to the Client and OPTIONALLY
%% QoS 0 messages pending transmission to the Client.
%%
%% QoS 2 messages which have been received from the Client, but have not been
%% completely acknowledged.The Will Message and the Will Delay Interval
%%
%% If the Session is currently not connected, the time at which the Session
%% will end and Session State will be discarded.
%% @end
%%--------------------------------------------------------------------

%% MQTT Session
-module(emqx_session).

-include("logger.hrl").
-include("types.hrl").
-include("emqx.hrl").
-include("emqx_session.hrl").
-include("emqx_mqtt.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([
    create/3,
    open/3,
    destroy/1,
    destroy/2
]).

-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    subscribe/4,
    unsubscribe/4
]).

-export([
    publish/4,
    puback/3,
    pubrec/3,
    pubrel/3,
    pubcomp/3,
    replay/3
]).

-export([
    deliver/3,
    handle_info/3,
    handle_timeout/3,
    disconnect/3,
    terminate/3
]).

%% Will message handling
-export([
    clear_will_message/1,
    publish_will_message_now/2
]).

% Timers
-export([
    ensure_timer/3,
    reset_timer/3,
    cancel_timer/2
]).

% Foreign session implementations
-export([
    enrich_delivers/3,
    enrich_message/4
]).

% Utilities
-export([should_keep/1]).

% Tests only
-export([get_session_conf/1]).

-export_type([
    t/0,
    conf/0,
    conninfo/0,
    clientinfo/0,
    reply/0,
    replies/0,
    common_timer_name/0,
    custom_timer_name/0,
    session_id/0,
    session/0
]).

-type session_id() :: _TODO.

-type clientinfo() :: emqx_types:clientinfo().
-type conninfo() ::
    emqx_types:conninfo()
    | #{
        %% Subset of `emqx_types:conninfo()` properties
        receive_maximum => non_neg_integer(),
        expiry_interval => non_neg_integer()
    }.

-type common_timer_name() :: retry_delivery | expire_awaiting_rel.
-type custom_timer_name() :: atom().

-type message() :: emqx_types:message().
-type publish() :: {option(emqx_types:packet_id()), emqx_types:message()}.
-type pubrel() :: {pubrel, emqx_types:packet_id()}.
-type reply() :: publish() | pubrel().
-type replies() :: [reply()] | reply().

-type conf() :: #{
    %% Max subscriptions allowed
    max_subscriptions := non_neg_integer() | infinity,
    %% Maximum number of awaiting QoS2 messages allowed
    max_awaiting_rel := non_neg_integer() | infinity,
    %% Upgrade QoS?
    upgrade_qos := boolean(),
    %% Retry interval for redelivering QoS1/2 messages (Unit: millisecond)
    retry_interval := timeout(),
    %% Awaiting PUBREL Timeout (Unit: millisecond)
    await_rel_timeout := timeout()
}.

-type session() :: t().

-type t() ::
    emqx_session_mem:session()
    | emqx_persistent_session_ds:session().

-define(INFO_KEYS, [
    id,
    created_at,
    is_persistent,
    subscriptions,
    upgrade_qos,
    retry_interval,
    await_rel_timeout
]).

-define(IMPL(S), (get_impl_mod(S))).

%%--------------------------------------------------------------------
%% Behaviour
%% -------------------------------------------------------------------

-callback create(clientinfo(), conninfo(), emqx_maybe:t(message()), conf()) ->
    t().

-callback open(clientinfo(), conninfo(), emqx_maybe:t(message()), conf()) ->
    {_IsPresent :: true, t(), _ReplayContext} | false.

-callback destroy(t() | clientinfo()) -> ok.

-callback clear_will_message(t()) -> t().

-callback publish_will_message_now(t(), message()) -> t().

-callback handle_timeout(clientinfo(), common_timer_name() | custom_timer_name(), t()) ->
    {ok, replies(), t()}
    | {ok, replies(), timeout(), t()}.

-callback handle_info(term(), t(), clientinfo()) -> t().

-callback get_subscription(emqx_types:topic(), t()) ->
    emqx_types:subopts() | undefined.

-callback subscribe(emqx_types:topic(), emqx_types:subopts(), t()) ->
    {ok, t()} | {error, emqx_types:reason_code()}.

-callback unsubscribe(emqx_types:topic(), t()) ->
    {ok, t(), emqx_types:subopts()}
    | {error, emqx_types:reason_code()}.

-callback publish(emqx_types:packet_id(), emqx_types:message(), t()) ->
    {ok, emqx_types:publish_result(), t()}
    | {error, emqx_types:reason_code()}.

-callback puback(clientinfo(), emqx_types:packet_id(), t()) ->
    {ok, emqx_types:message(), replies(), t()}
    | {error, emqx_types:reason_code()}.

-callback pubrec(emqx_types:packet_id(), t()) ->
    {ok, emqx_types:message(), t()}
    | {error, emqx_types:reason_code()}.

-callback pubrel(emqx_types:packet_id(), t()) ->
    {ok, t()}
    | {error, emqx_types:reason_code()}.

-callback pubcomp(clientinfo(), emqx_types:packet_id(), t()) ->
    {ok, replies(), t()}
    | {error, emqx_types:reason_code()}.

-callback replay(clientinfo(), [emqx_types:message()], t()) ->
    {ok, replies(), t()}.

-callback deliver(clientinfo(), [emqx_types:deliver()], t()) ->
    {ok, replies(), t()}.

-callback info(atom(), t()) -> term().

-callback stats(t()) -> emqx_types:stats().

-callback disconnect(t(), conninfo()) -> {idle | shutdown, t()}.

-callback terminate(Reason :: term(), t()) -> ok.

%%--------------------------------------------------------------------
%% Create a Session
%%--------------------------------------------------------------------

-spec create(clientinfo(), conninfo(), emqx_maybe:t(message())) -> t().
create(ClientInfo, ConnInfo, MaybeWillMsg) ->
    Conf = get_session_conf(ClientInfo),
    % FIXME error conditions
    create(
        hd(choose_impl_candidates(ClientInfo, ConnInfo)), ClientInfo, ConnInfo, MaybeWillMsg, Conf
    ).

create(Mod, ClientInfo, ConnInfo, MaybeWillMsg, Conf) ->
    % FIXME error conditions
    Session = Mod:create(ClientInfo, ConnInfo, MaybeWillMsg, Conf),
    ok = emqx_metrics:inc('session.created'),
    ok = emqx_hooks:run('session.created', [ClientInfo, info(Session)]),
    Session.

-spec open(clientinfo(), conninfo(), emqx_maybe:t(message())) ->
    {_IsPresent :: true, t(), _ReplayContext} | {_IsPresent :: false, t()}.
open(ClientInfo, ConnInfo, MaybeWillMsg) ->
    Conf = get_session_conf(ClientInfo),
    Mods = [Default | _] = choose_impl_candidates(ClientInfo, ConnInfo),
    %% NOTE
    %% Try to look the existing session up in session stores corresponding to the given
    %% `Mods` in order, starting from the last one.
    case try_open(Mods, ClientInfo, ConnInfo, MaybeWillMsg, Conf) of
        {_IsPresent = true, _, _} = Present ->
            Present;
        false ->
            %% NOTE
            %% Nothing was found, create a new session with the `Default` implementation.
            {false, create(Default, ClientInfo, ConnInfo, MaybeWillMsg, Conf)}
    end.

try_open([Mod | Rest], ClientInfo, ConnInfo, MaybeWillMsg, Conf) ->
    case try_open(Rest, ClientInfo, ConnInfo, MaybeWillMsg, Conf) of
        {_IsPresent = true, _, _} = Present ->
            Present;
        false ->
            Mod:open(ClientInfo, ConnInfo, MaybeWillMsg, Conf)
    end;
try_open([], _ClientInfo, _ConnInfo, _MaybeWillMsg, _Conf) ->
    false.

-spec get_session_conf(clientinfo()) -> conf().
get_session_conf(_ClientInfo = #{zone := Zone}) ->
    #{
        max_subscriptions => get_mqtt_conf(Zone, max_subscriptions),
        max_awaiting_rel => get_mqtt_conf(Zone, max_awaiting_rel),
        upgrade_qos => get_mqtt_conf(Zone, upgrade_qos),
        retry_interval => get_mqtt_conf(Zone, retry_interval),
        await_rel_timeout => get_mqtt_conf(Zone, await_rel_timeout)
    }.

get_mqtt_conf(Zone, Key) ->
    emqx_config:get_zone_conf(Zone, [mqtt, Key]).

%%--------------------------------------------------------------------
%% Existing sessions
%% -------------------------------------------------------------------

-spec destroy(clientinfo(), conninfo()) -> ok.
destroy(ClientInfo, ConnInfo) ->
    %% When destroying/discarding a session, the current `ClientInfo' might suggest an
    %% implementation which does not correspond to the one previously used by this client.
    %% An example of this is a client that first connects with `Session-Expiry-Interval' >
    %% 0, and later reconnects with `Session-Expiry-Interval' = 0 and `clean_start' =
    %% true.  So we may simply destroy sessions from all implementations, since the key
    %% (ClientID) is the same.
    Mods = choose_impl_candidates(ClientInfo, ConnInfo),
    lists:foreach(fun(Mod) -> Mod:destroy(ClientInfo) end, Mods).

-spec destroy(t()) -> ok.
destroy(Session) ->
    ?IMPL(Session):destroy(Session).

%%--------------------------------------------------------------------
%% Subscriptions
%% -------------------------------------------------------------------

-spec subscribe(
    clientinfo(),
    emqx_types:topic() | emqx_types:share(),
    emqx_types:subopts(),
    t()
) ->
    {ok, t()} | {error, emqx_types:reason_code()}.
subscribe(ClientInfo, TopicFilter, SubOpts, Session) ->
    SubOpts0 = ?IMPL(Session):get_subscription(TopicFilter, Session),
    case ?IMPL(Session):subscribe(TopicFilter, SubOpts, Session) of
        {ok, Session1} ->
            ok = emqx_hooks:run(
                'session.subscribed',
                [ClientInfo, TopicFilter, SubOpts#{is_new => (SubOpts0 == undefined)}]
            ),
            {ok, Session1};
        {error, RC} ->
            {error, RC}
    end.

-spec unsubscribe(
    clientinfo(),
    emqx_types:topic() | emqx_types:share(),
    emqx_types:subopts(),
    t()
) ->
    {ok, t()} | {error, emqx_types:reason_code()}.
unsubscribe(
    ClientInfo,
    TopicFilter,
    UnSubOpts,
    Session
) ->
    case ?IMPL(Session):unsubscribe(TopicFilter, Session) of
        {ok, Session1, SubOpts} ->
            ok = emqx_hooks:run(
                'session.unsubscribed',
                [ClientInfo, TopicFilter, maps:merge(SubOpts, UnSubOpts)]
            ),
            {ok, Session1};
        {error, RC} ->
            {error, RC}
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBLISH
%%--------------------------------------------------------------------

-spec publish(clientinfo(), emqx_types:packet_id(), emqx_types:message(), t()) ->
    {ok, emqx_types:publish_result(), t()}
    | {error, emqx_types:reason_code()}.
publish(_ClientInfo, PacketId, Msg, Session) ->
    case ?IMPL(Session):publish(PacketId, Msg, Session) of
        {ok, _Result, _Session} = Ok ->
            % TODO: only timers are allowed for now
            Ok;
        {error, RC} = Error when Msg#message.qos =:= ?QOS_2 ->
            on_dropped_qos2_msg(PacketId, Msg, RC),
            Error;
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBACK
%%--------------------------------------------------------------------

-spec puback(clientinfo(), emqx_types:packet_id(), t()) ->
    {ok, message(), replies(), t()}
    | {error, emqx_types:reason_code()}.
puback(ClientInfo, PacketId, Session) ->
    case ?IMPL(Session):puback(ClientInfo, PacketId, Session) of
        {ok, Msg, Replies, Session1} = Ok ->
            _ = on_delivery_completed(Msg, ClientInfo, Session1),
            _ = on_replies_delivery_completed(Replies, ClientInfo, Session1),
            Ok;
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREC / PUBREL / PUBCOMP
%%--------------------------------------------------------------------

-spec pubrec(clientinfo(), emqx_types:packet_id(), t()) ->
    {ok, message(), t()}
    | {error, emqx_types:reason_code()}.
pubrec(_ClientInfo, PacketId, Session) ->
    case ?IMPL(Session):pubrec(PacketId, Session) of
        {ok, _Msg, _Session} = Ok ->
            Ok;
        {error, _} = Error ->
            Error
    end.

-spec pubrel(clientinfo(), emqx_types:packet_id(), t()) ->
    {ok, t()}
    | {error, emqx_types:reason_code()}.
pubrel(_ClientInfo, PacketId, Session) ->
    case ?IMPL(Session):pubrel(PacketId, Session) of
        {ok, _Session} = Ok ->
            Ok;
        {error, _} = Error ->
            Error
    end.

-spec pubcomp(clientinfo(), emqx_types:packet_id(), t()) ->
    {ok, replies(), t()}
    | {error, emqx_types:reason_code()}.
pubcomp(ClientInfo, PacketId, Session) ->
    case ?IMPL(Session):pubcomp(ClientInfo, PacketId, Session) of
        {ok, Msg, Replies, Session1} ->
            _ = on_delivery_completed(Msg, ClientInfo, Session1),
            _ = on_replies_delivery_completed(Replies, ClientInfo, Session1),
            {ok, Replies, Session1};
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------

-spec replay(clientinfo(), [emqx_types:message()], t()) ->
    {ok, replies(), t()}.
replay(ClientInfo, ReplayContext, Session) ->
    ?IMPL(Session):replay(ClientInfo, ReplayContext, Session).

%%--------------------------------------------------------------------
%% Broker -> Client: Deliver
%%--------------------------------------------------------------------

-spec deliver(clientinfo(), [emqx_types:deliver()], t()) ->
    {ok, replies(), t()}.
deliver(ClientInfo, Delivers, Session) ->
    Messages = enrich_delivers(ClientInfo, Delivers, Session),
    ?IMPL(Session):deliver(ClientInfo, Messages, Session).

%%--------------------------------------------------------------------

enrich_delivers(ClientInfo, Delivers, Session) ->
    UpgradeQoS = ?IMPL(Session):info(upgrade_qos, Session),
    enrich_delivers(ClientInfo, Delivers, UpgradeQoS, Session).

enrich_delivers(_ClientInfo, [], _UpgradeQoS, _Session) ->
    [];
enrich_delivers(ClientInfo, [D | Rest], UpgradeQoS, Session) ->
    enrich_deliver(ClientInfo, D, UpgradeQoS, Session) ++
        enrich_delivers(ClientInfo, Rest, UpgradeQoS, Session).

enrich_deliver(ClientInfo, {deliver, Topic, Msg}, UpgradeQoS, Session) ->
    SubOpts =
        case Msg of
            #message{headers = #{redispatch_to := ?REDISPATCH_TO(Group, T)}} ->
                ?IMPL(Session):get_subscription(emqx_topic:make_shared_record(Group, T), Session);
            _ ->
                ?IMPL(Session):get_subscription(Topic, Session)
        end,
    enrich_message(ClientInfo, Msg, SubOpts, UpgradeQoS).

%% Caution: updating this function _may_ break consistency of replay
%% for persistent sessions. Persistent sessions expect it to return
%% the same result during replay. If it changes the behavior between
%% releases, sessions restored from the cold storage may end up
%% replaying messages with different QoS, etc.
enrich_message(
    ClientInfo = #{clientid := ClientId},
    Msg = #message{from = ClientId},
    #{nl := 1},
    _UpgradeQoS
) ->
    _ = emqx_session_events:handle_event(ClientInfo, {dropped, Msg, no_local}),
    [];
enrich_message(_ClientInfo, MsgIn, SubOpts = #{}, UpgradeQoS) ->
    [
        maps:fold(
            fun(SubOpt, V, Msg) -> enrich_subopts(SubOpt, V, Msg, UpgradeQoS) end,
            MsgIn,
            SubOpts
        )
    ];
enrich_message(_ClientInfo, Msg, undefined, _UpgradeQoS) ->
    [Msg].

enrich_subopts(nl, 1, Msg, _) ->
    emqx_message:set_flag(nl, Msg);
enrich_subopts(nl, 0, Msg, _) ->
    Msg;
enrich_subopts(qos, SubQoS, Msg = #message{qos = PubQoS}, _UpgradeQoS = true) ->
    Msg#message{qos = max(SubQoS, PubQoS)};
enrich_subopts(qos, SubQoS, Msg = #message{qos = PubQoS}, _UpgradeQoS = false) ->
    Msg#message{qos = min(SubQoS, PubQoS)};
enrich_subopts(rap, 1, Msg, _) ->
    Msg;
enrich_subopts(rap, 0, Msg = #message{headers = #{retained := true}}, _) ->
    Msg;
enrich_subopts(rap, 0, Msg, _) ->
    emqx_message:set_flag(retain, false, Msg);
enrich_subopts(subid, SubId, Msg, _) ->
    Props = emqx_message:get_header(properties, Msg, #{}),
    emqx_message:set_header(properties, Props#{'Subscription-Identifier' => SubId}, Msg);
enrich_subopts(_Opt, _V, Msg, _) ->
    Msg.

%%--------------------------------------------------------------------
%% Timeouts
%%--------------------------------------------------------------------

-spec handle_timeout(clientinfo(), common_timer_name() | custom_timer_name(), t()) ->
    {ok, replies(), t()}
    %% NOTE: only relevant for `common_timer_name()`
    | {ok, replies(), timeout(), t()}.
handle_timeout(ClientInfo, Timer, Session) ->
    ?IMPL(Session):handle_timeout(ClientInfo, Timer, Session).

%%--------------------------------------------------------------------
%% Generic Messages
%%--------------------------------------------------------------------

-spec handle_info(term(), t(), clientinfo()) -> t().
handle_info(Info, Session, ClientInfo) ->
    ?IMPL(Session):handle_info(Info, Session, ClientInfo).

%%--------------------------------------------------------------------

-spec ensure_timer(custom_timer_name(), timeout(), map()) ->
    map().
ensure_timer(Name, Time, Timers = #{}) when Time >= 0 ->
    TRef = emqx_utils:start_timer(Time, {?MODULE, Name}),
    Timers#{Name => TRef}.

-spec reset_timer(custom_timer_name(), timeout(), map()) ->
    map().
reset_timer(Name, Time, Timers) ->
    ensure_timer(Name, Time, cancel_timer(Name, Timers)).

-spec cancel_timer(custom_timer_name(), map()) ->
    map().
cancel_timer(Name, Timers0) ->
    case maps:take(Name, Timers0) of
        {TRef, Timers} ->
            ok = emqx_utils:cancel_timer(TRef),
            Timers;
        error ->
            Timers0
    end.

%%--------------------------------------------------------------------

-spec disconnect(clientinfo(), conninfo(), t()) ->
    {idle | shutdown, t()}.
disconnect(_ClientInfo, ConnInfo, Session) ->
    ?IMPL(Session):disconnect(Session, ConnInfo).

-spec terminate(clientinfo(), Reason :: term(), t()) ->
    ok.
terminate(ClientInfo, Reason, Session) ->
    _ = run_terminate_hooks(ClientInfo, Reason, Session),
    _ = ?IMPL(Session):terminate(Reason, Session),
    ok.

run_terminate_hooks(ClientInfo, discarded, Session) ->
    run_hook('session.discarded', [ClientInfo, info(Session)]);
run_terminate_hooks(ClientInfo, takenover, Session) ->
    run_hook('session.takenover', [ClientInfo, info(Session)]);
run_terminate_hooks(ClientInfo, Reason, Session) ->
    run_hook('session.terminated', [ClientInfo, Reason, info(Session)]).

%%--------------------------------------------------------------------
%% Session Info
%% -------------------------------------------------------------------

-spec info(t()) -> emqx_types:infos().
info(Session) ->
    maps:from_list(info(?INFO_KEYS, Session)).

-spec info
    ([atom()], t()) -> [{atom(), _Value}];
    (atom() | {atom(), _Meta}, t()) -> _Value.
info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(impl, Session) ->
    get_impl_mod(Session);
info(Key, Session) ->
    ?IMPL(Session):info(Key, Session).

-spec stats(t()) -> emqx_types:stats().
stats(Session) ->
    ?IMPL(Session):stats(Session).

%%--------------------------------------------------------------------
%% Common message events
%%--------------------------------------------------------------------

on_delivery_completed(Msg, #{clientid := ClientId}, Session) ->
    emqx_hooks:run(
        'delivery.completed',
        [
            Msg,
            #{
                session_birth_time => ?IMPL(Session):info(created_at, Session),
                clientid => ClientId
            }
        ]
    ).

on_replies_delivery_completed(Replies, ClientInfo, Session) ->
    lists:foreach(
        fun({_PacketId, Msg}) ->
            case Msg of
                #message{qos = ?QOS_0} ->
                    on_delivery_completed(Msg, ClientInfo, Session);
                _ ->
                    ok
            end
        end,
        Replies
    ).

on_dropped_qos2_msg(PacketId, Msg, RC) ->
    ?SLOG(
        warning,
        #{
            msg => "dropped_qos2_packet",
            reason => emqx_reason_codes:name(RC),
            packet_id => PacketId
        },
        #{topic => Msg#message.topic}
    ),
    ok = emqx_metrics:inc('messages.dropped'),
    ok = emqx_hooks:run('message.dropped', [Msg, #{node => node()}, emqx_reason_codes:name(RC)]),
    ok.

%%--------------------------------------------------------------------

-spec should_keep(message() | emqx_types:deliver()) -> boolean().
should_keep(MsgDeliver) ->
    not is_banned_msg(MsgDeliver).

is_banned_msg(#message{from = ClientId}) ->
    emqx_banned:check_clientid(ClientId).

%%--------------------------------------------------------------------

-spec get_impl_mod(t()) -> module().
get_impl_mod(Session) when ?IS_SESSION_IMPL_MEM(Session) ->
    emqx_session_mem;
get_impl_mod(Session) when ?IS_SESSION_IMPL_DS(Session) ->
    emqx_persistent_session_ds;
get_impl_mod(Session) ->
    maybe_mock_impl_mod(Session).

-ifdef(TEST).
maybe_mock_impl_mod({Mock, _State}) when is_atom(Mock) ->
    Mock.
-else.
-spec maybe_mock_impl_mod(_Session) -> no_return().
maybe_mock_impl_mod(Session) ->
    error(noimpl, [Session]).
-endif.

choose_impl_candidates(#{zone := Zone}, #{expiry_interval := EI}) ->
    case emqx_persistent_message:is_persistence_enabled(Zone) of
        false ->
            [emqx_session_mem];
        true ->
            Force = emqx_persistent_message:force_ds(Zone),
            case EI of
                0 when not Force ->
                    %% NOTE
                    %% If ExpiryInterval is 0, the natural choice is
                    %% `emqx_session_mem'. Yet we still need to look
                    %% the existing session up in the
                    %% `emqx_persistent_session_ds' store first,
                    %% because previous connection may have set
                    %% ExpiryInterval to a non-zero value.
                    [emqx_session_mem, emqx_persistent_session_ds];
                _ ->
                    [emqx_persistent_session_ds]
            end
    end.

-compile({inline, [run_hook/2]}).
run_hook(Name, Args) ->
    ok = emqx_metrics:inc(Name),
    emqx_hooks:run(Name, Args).

%%--------------------------------------------------------------------
%% Will message handling
%%--------------------------------------------------------------------

-spec clear_will_message(t()) -> t().
clear_will_message(Session) ->
    ?IMPL(Session):clear_will_message(Session).

-spec publish_will_message_now(t(), message()) -> t().
publish_will_message_now(Session, WillMsg) ->
    ?IMPL(Session):publish_will_message_now(Session, WillMsg).
