%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_session_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(mock_modules,
        [ emqx_metrics
        , emqx_broker
        , emqx_misc
        , emqx_message
        , emqx_hooks
        , emqx_zone
        ]).

all() -> emqx_ct:all(?MODULE).

t_proper_session(_) ->
    Opts = [{numtests, 100}, {to_file, user}],
    ok = emqx_logger:set_log_level(emergency),
    ok = before_proper(),
    ?assert(proper:quickcheck(prop_session(), Opts)),
    ok = after_proper().

before_proper() ->
    load(?mock_modules).

after_proper() ->
    unload(?mock_modules),
    emqx_logger:set_log_level(error).

prop_session() ->
    ?FORALL({Session, OpList}, {session(), session_op_list()},
            begin
                try
                    apply_ops(Session, OpList),
                    true
                after
                    true
                end
            end).

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%

apply_ops(Session, []) ->
    ?assertEqual(session, element(1, Session));
apply_ops(Session, [Op | Rest]) ->
    NSession = apply_op(Session, Op),
    apply_ops(NSession, Rest).

apply_op(Session, info) ->
    Info = emqx_session:info(Session),
    ?assert(is_map(Info)),
    ?assert(maps:size(Info) > 0),
    Session;
apply_op(Session, attrs) ->
    Attrs = emqx_session:attrs(Session),
    ?assert(is_map(Attrs)),
    ?assert(maps:size(Attrs) > 0),
    Session;
apply_op(Session, stats) ->
    Stats = emqx_session:stats(Session),
    ?assert(is_list(Stats)),
    ?assert(length(Stats) > 0),
    Session;
apply_op(Session, {info, InfoArg}) ->
    _Ret = emqx_session:info(InfoArg, Session),
    Session;
apply_op(Session, {subscribe, {Client, TopicFilter, SubOpts}}) ->
    case emqx_session:subscribe(Client, TopicFilter, SubOpts, Session) of
        {ok, NSession} ->
            NSession;
        {error, ?RC_QUOTA_EXCEEDED} ->
            Session
    end;
apply_op(Session, {unsubscribe, {Client, TopicFilter}}) ->
    case emqx_session:unsubscribe(Client, TopicFilter, Session) of
        {ok, NSession} ->
            NSession;
        {error, ?RC_NO_SUBSCRIPTION_EXISTED} ->
            Session
    end;
apply_op(Session, {publish, {PacketId, Msg}}) ->
    case emqx_session:publish(PacketId, Msg, Session) of
        {ok, _Msg} ->
            Session;
        {ok, _Deliver, NSession} ->
            NSession;
        {error, _ErrorCode} ->
            Session
    end;
apply_op(Session, {puback, PacketId}) ->
    case emqx_session:puback(PacketId, Session) of
        {ok, _Msg, NSession} ->
            NSession;
        {ok, _Msg, _Publishes, NSession} ->
            NSession;
        {error, _ErrorCode} ->
            Session
    end;
apply_op(Session, {pubrec, PacketId}) ->
    case emqx_session:pubrec(PacketId, Session) of
        {ok, _Msg, NSession} ->
            NSession;
        {error, _ErrorCode} ->
            Session
    end;
apply_op(Session, {pubrel, PacketId}) ->
    case emqx_session:pubrel(PacketId, Session) of
        {ok, NSession} ->
            NSession;
        {error, _ErrorCode} ->
            Session
    end;
apply_op(Session, {pubcomp, PacketId}) ->
    case emqx_session:pubcomp(PacketId, Session) of
        {ok, _Msgs} ->
            Session;
        {ok, _Msgs, NSession} ->
            NSession;
        {error, _ErrorCode} ->
            Session
    end;
apply_op(Session, {deliver, Delivers}) ->
    {ok, _Msgs, NSession} = emqx_session:deliver(Delivers, Session),
    NSession.

%%%%%%%%%%%%%%%%%%
%%% Generators %%%
%%%%%%%%%%%%%%%%%%
session_op_list() ->
    Union = [info,
             attrs,
             stats,
             {info, info_args()},
             {subscribe, sub_args()},
             {unsubscribe, unsub_args()},
             {publish, publish_args()},
             {puback, puback_args()},
             {pubrec, pubrec_args()},
             {pubrel, pubrel_args()},
             {pubcomp, pubcomp_args()},
             {deliver, deliver_args()}
            ],
    list(?LAZY(oneof(Union))).

deliver_args() ->
    list({deliver, topic(), message()}).

info_args() ->
    oneof([subscriptions,
           max_subscriptions,
           upgrade_qos,
           inflight,
           max_inflight,
           retry_interval,
           mqueue_len,
           max_mqueue,
           mqueue_dropped,
           next_pkt_id,
           awaiting_rel,
           max_awaiting_rel,
           await_rel_timeout,
           created_at
          ]).

sub_args() ->
    ?LET({ClientId, TopicFilter, SubOpts},
         {clientid(), topic(), sub_opts()},
         {#{clientid => ClientId}, TopicFilter, SubOpts}).

unsub_args() ->
    ?LET({ClientId, TopicFilter},
         {clientid(), topic()},
         {#{clientid => ClientId}, TopicFilter}).

publish_args() ->
    ?LET({PacketId, Message},
         {packetid(), message()},
         {PacketId, Message}).

puback_args() ->
    packetid().

pubrec_args() ->
    packetid().

pubrel_args() ->
    packetid().

pubcomp_args() ->
    packetid().

sub_opts() ->
    ?LET({RH, RAP, NL, QOS, SHARE, SUBID},
         {rh(), rap(), nl(), qos(), share(), subid()}
        , make_subopts(RH, RAP, NL, QOS, SHARE, SUBID)).

message() ->
    ?LET({QoS, Topic, Payload},
         {qos(), topic(), payload()},
         emqx_message:make(proper, QoS, Topic, Payload)).

subid() -> integer().

rh() -> oneof([0, 1, 2]).

rap() -> oneof([0, 1]).

nl() -> oneof([0, 1]).

qos() -> oneof([0, 1, 2]).

share() -> binary().

clientid() -> binary().

topic() -> ?LET(No, choose(1, 10),
                begin
                    NoBin = integer_to_binary(No),
                    <<"topic/", NoBin/binary>>
                end).

payload() -> binary().

packetid() -> choose(1, 30).

zone() ->
    ?LET(Zone, [{max_subscriptions, max_subscription()},
                {upgrade_qos, upgrade_qos()},
                {retry_interval, retry_interval()},
                {max_awaiting_rel, max_awaiting_rel()},
                {await_rel_timeout, await_rel_timeout()}]
        , maps:from_list(Zone)).

max_subscription() ->
    frequency([{33, 0},
               {33, 1},
               {34, choose(0,10)}]).

upgrade_qos() -> bool().

retry_interval() -> ?LET(Interval, choose(0, 20), Interval*1000).

max_awaiting_rel() -> choose(0, 10).

await_rel_timeout() -> ?LET(Interval, choose(0, 150), Interval*1000).

max_inflight() -> choose(0, 10).

option() ->
    ?LET(Option, [{receive_maximum , max_inflight()}],
         maps:from_list(Option)).

session() ->
    ?LET({Zone, Options},
         {zone(), option()},
         begin
             Session = emqx_session:init(#{zone => Zone}, Options),
             emqx_session:set_field(next_pkt_id, 16#ffff, Session)
         end).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal functions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

make_subopts(RH, RAP, NL, QOS, SHARE, SubId) ->
    #{rh => RH,
      rap => RAP,
      nl => NL,
      qos => QOS,
      share => SHARE,
      subid => SubId}.


load(Modules) ->
    [mock(Module) || Module <- Modules],
    ok.

unload(Modules) ->
    lists:foreach(fun(Module) ->
                          ok = meck:unload(Module)
                  end, Modules).

mock(Module) ->
    ok = meck:new(Module, [passthrough, no_history]),
    do_mock(Module).

do_mock(emqx_metrics) ->
    meck:expect(emqx_metrics, inc, fun(_Anything) -> ok end);
do_mock(emqx_broker) ->
    meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    meck:expect(emqx_broker, set_subopts, fun(_, _) -> ok end),
    meck:expect(emqx_broker, unsubscribe, fun(_) -> ok end),
    meck:expect(emqx_broker, publish, fun(_) -> ok end);
do_mock(emqx_misc) ->
    meck:expect(emqx_misc, start_timer, fun(_, _) -> tref end);
do_mock(emqx_message) ->
    meck:expect(emqx_message, set_header, fun(_Hdr, _Val, Msg) -> Msg end),
    meck:expect(emqx_message, is_expired, fun(_Msg) -> (rand:uniform(16) > 8) end);
do_mock(emqx_hooks) ->
    meck:expect(emqx_hooks, run, fun(_Hook, _Args) -> ok end);
do_mock(emqx_zone) ->
    meck:expect(emqx_zone, get_env, fun(Env, Key, Default) -> maps:get(Key, Env, Default) end).

