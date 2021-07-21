%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_channel).

-behavior(emqx_gateway_channel).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_gateway/src/coap/include/emqx_coap.hrl").

%% API
-export([]).

-export([ info/1
        , info/2
        , stats/1
        , auth_publish/2
        , auth_subscribe/2
        , reply/4
        , ack/4
        , transfer_result/3]).

-export([ init/2
        , handle_in/2
        , handle_deliver/2
        , handle_timeout/3
        , terminate/2
        ]).

-export([ handle_call/2
        , handle_cast/2
        , handle_info/2
        ]).

-export_type([channel/0]).

-record(channel, {
                  %% Context
                  ctx           :: emqx_gateway_ctx:context(),
                  %% Connection Info
                  conninfo      :: emqx_types:conninfo(),
                  %% Client Info
                  clientinfo    :: emqx_types:clientinfo(),
                  %% Session
                  session       :: emqx_coap_session:session() | undefined,
                  %% Keepalive
                  keepalive     :: emqx_keepalive:keepalive() | undefined,
                  %% Timer
                  timers :: #{atom() => disable | undefined | reference()},
                  config :: hocon:config()
                 }).

-type channel() :: #channel{}.
-define(DISCONNECT_WAIT_TIME, timer:seconds(10)).
-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session]).

%%%===================================================================
%%% API
%%%===================================================================

info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];

info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(conn_state, _) ->
    connected;
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, #channel{session = Session}) ->
    emqx_misc:maybe_apply(fun emqx_session:info/1, Session);
info(clientid, #channel{clientinfo = #{clientid := ClientId}}) ->
    ClientId;
info(ctx, #channel{ctx = Ctx}) ->
    Ctx.

stats(_) ->
    [].

init(ConnInfo = #{peername := {PeerHost, _},
                  sockname := {_, SockPort}},
     #{ctx := Ctx} = Config) ->
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Mountpoint = maps:get(mountpoint, Config, undefined),
    ClientInfo = set_peercert_infos(
                   Peercert,
                   #{ zone => default
                    , protocol => 'mqtt-coap'
                    , peerhost => PeerHost
                    , sockport => SockPort
                    , clientid => emqx_guid:to_base62(emqx_guid:gen())
                    , username => undefined
                    , is_bridge => false
                    , is_superuser => false
                    , mountpoint => Mountpoint
                    }
                  ),

    #channel{ ctx = Ctx
            , conninfo = ConnInfo
            , clientinfo = ClientInfo
            , timers = #{}
            , session = emqx_coap_session:new()
            , config = Config#{clientinfo => ClientInfo,
                               ctx => Ctx}
            , keepalive = emqx_keepalive:init(maps:get(heartbeat, Config))
            }.

auth_publish(Topic,
             #{ctx := Ctx,
               clientinfo := ClientInfo}) ->
    emqx_gateway_ctx:authorize(Ctx, ClientInfo, publish, Topic).

auth_subscribe(Topic,
               #{ctx := Ctx,
                 clientinfo := ClientInfo}) ->
    emqx_gateway_ctx:authorize(Ctx, ClientInfo, subscribe, Topic).

transfer_result(Result, From, Value) ->
    ?TRANSFER_RESULT(Result, [out], From, Value).

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------
%% treat post to root path as a heartbeat
%% treat post to root path with query string as a command
handle_in(#coap_message{method = post,
                        options = Options} = Msg, ChannelT) ->
    Channel = ensure_keepalive_timer(ChannelT),
    case maps:get(uri_path, Options, <<>>) of
        <<>> ->
            handle_command(Msg, Channel);
        _ ->
            call_session(Channel, received, [Msg])
    end;

handle_in(Msg, Channel) ->
    call_session(ensure_keepalive_timer(Channel), received, [Msg]).

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------
handle_deliver(Delivers, Channel) ->
    call_session(Channel, deliver, [Delivers]).

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------
handle_timeout(_, {keepalive, NewVal}, #channel{keepalive = KeepAlive} = Channel) ->
    case emqx_keepalive:check(NewVal, KeepAlive) of
        {ok, NewKeepAlive} ->
            Channel2 = ensure_keepalive_timer(Channel, fun make_timer/4),
            {ok, Channel2#channel{keepalive = NewKeepAlive}};
        {error, timeout} ->
            {shutdown, timeout, Channel}
    end;

handle_timeout(_, {transport, Msg}, Channel) ->
    call_session(Channel, timeout, [Msg]);

handle_timeout(_, disconnect, Channel) ->
    {shutdown, normal, Channel};

handle_timeout(_, _, Channel) ->
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------
handle_call(Req, Channel) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, Channel}.

%%--------------------------------------------------------------------
%% Handle Cast
%%--------------------------------------------------------------------
handle_cast(Req, Channel) ->
    ?LOG(error, "Unexpected cast: ~p", [Req]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------
handle_info(Info, Channel) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------
terminate(_Reason, _Channel) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
set_peercert_infos(NoSSL, ClientInfo)
  when NoSSL =:= nossl;
       NoSSL =:= undefined ->
    ClientInfo;
set_peercert_infos(Peercert, ClientInfo) ->
    {DN, CN} = {esockd_peercert:subject(Peercert),
                esockd_peercert:common_name(Peercert)},
    ClientInfo#{dn => DN, cn => CN}.

ensure_timer(Name, Time, Msg, #channel{timers = Timers} = Channel) ->
    case maps:get(Name, Timers, undefined) of
        undefined ->
            make_timer(Name, Time, Msg, Channel);
        _ ->
            Channel
    end.

make_timer(Name, Time, Msg, Channel = #channel{timers = Timers}) ->
    TRef = emqx_misc:start_timer(Time, Msg),
    Channel#channel{timers = Timers#{Name => TRef}}.

ensure_keepalive_timer(Channel) ->
    ensure_keepalive_timer(Channel, fun ensure_timer/4).

ensure_keepalive_timer(#channel{config = Cfg} = Channel, Fun) ->
    Interval = maps:get(heartbeat, Cfg),
    Fun(keepalive, Interval, keepalive, Channel).

handle_command(#coap_message{options = Options} = Msg, Channel) ->
    case maps:get(uri_query, Options, []) of
        [] ->
            %% heartbeat
            ack(Channel, {ok, valid}, <<>>, Msg);
        QueryPairs ->
            Queries = lists:foldl(fun(Pair, Acc) ->
                                          [{K, V}] = cow_qs:parse_qs(Pair),
                                          Acc#{K => V}
                                  end,
                                  #{},
                                  QueryPairs),
            case maps:get(<<"action">>, Queries, undefined) of
                undefined ->
                    ack(Channel, {error, bad_request}, <<"command without actions">>, Msg);
                Action ->
                    handle_command(Action, Queries, Msg, Channel)
            end
    end.

handle_command(<<"connect">>, Queries, Msg, Channel) ->
    case emqx_misc:pipeline(
           [ fun run_conn_hooks/2
           , fun enrich_clientinfo/2
           , fun set_log_meta/2
           , fun auth_connect/2
           ],
           {Queries, Msg},
           Channel) of
        {ok, _Input, NChannel} ->
            process_connect(ensure_connected(NChannel), Msg);
        {error, ReasonCode, NChannel} ->
            ErrMsg = io_lib:format("Login Failed: ~s", [ReasonCode]),
            ack(NChannel, {error, bad_request}, ErrMsg, Msg)
    end;

handle_command(<<"disconnect">>, _, Msg, Channel) ->
    Channel2 = ensure_timer(disconnect, ?DISCONNECT_WAIT_TIME, disconnect, Channel),
    ack(Channel2, {ok, deleted}, <<>>, Msg);

handle_command(_, _, Msg, Channel) ->
    ack(Channel, {error, bad_request}, <<"invalid action">>, Msg).

run_conn_hooks(Input, Channel = #channel{ctx = Ctx,
                                         conninfo = ConnInfo}) ->
    ConnProps = #{},
    case run_hooks(Ctx, 'client.connect', [ConnInfo], ConnProps) of
                   Error = {error, _Reason} -> Error;
                   _NConnProps ->
                       {ok, Input, Channel}
    end.

enrich_clientinfo({Queries, Msg},
                  Channel = #channel{clientinfo = ClientInfo0,
                                     config = Cfg}) ->
    case Queries of
        #{<<"username">> := UserName,
          <<"password">> := Password,
          <<"clientid">> := ClientId} ->
            ClientInfo = ClientInfo0#{username => UserName,
                                      password => Password,
                                      clientid => ClientId},
            {ok, NClientInfo} = fix_mountpoint(Msg, ClientInfo),
            {ok, Channel#channel{clientinfo = NClientInfo,
                                 config = Cfg#{clientinfo := NClientInfo}}};
        _ ->
            {error, "invalid queries", Channel}
    end.

set_log_meta(_Input, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId),
    ok.

auth_connect(_Input, Channel = #channel{ctx = Ctx,
                                        clientinfo = ClientInfo}) ->
    #{clientid := ClientId,
      username := Username} = ClientInfo,
    case emqx_gateway_ctx:authenticate(Ctx, ClientInfo) of
        {ok, NClientInfo} ->
            {ok, Channel#channel{clientinfo = NClientInfo}};
        {error, Reason} ->
            ?LOG(warning, "Client ~s (Username: '~s') login failed for ~0p",
                 [ClientId, Username, Reason]),
            {error, Reason}
    end.

fix_mountpoint(_Packet, #{mountpoint := undefined}) -> ok;
fix_mountpoint(_Packet, ClientInfo = #{mountpoint := Mountpoint}) ->
    %% TODO: Enrich the varibale replacement????
    %%       i.e: ${ClientInfo.auth_result.productKey}
    Mountpoint1 = emqx_mountpoint:replvar(Mountpoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := Mountpoint1}}.

ensure_connected(Channel = #channel{ctx = Ctx,
                                    conninfo = ConnInfo,
                                    clientinfo = ClientInfo}) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    ok = run_hooks(Ctx, 'client.connected', [ClientInfo, NConnInfo]),
    Channel#channel{conninfo = NConnInfo}.

process_connect(Channel = #channel{ctx = Ctx,
                                   session = Session,
                                   conninfo = ConnInfo,
                                   clientinfo = ClientInfo},
                Msg) ->
    SessFun = fun(_,_) -> Session end,
    case emqx_gateway_ctx:open_session(
           Ctx,
           true,
           ClientInfo,
           ConnInfo,
           SessFun
          ) of
        {ok, _Sess} ->
            ack(Channel, {ok, created}, <<"connected">>, Msg);
        {error, Reason} ->
            ?LOG(error, "Failed to open session du to ~p", [Reason]),
            ack(Channel, {error, bad_request}, <<>>, Msg)
    end.

run_hooks(Ctx, Name, Args) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run(Name, Args).

run_hooks(Ctx, Name, Args, Acc) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run_fold(Name, Args, Acc).

reply(Channel, Method, Payload, Req) ->
    call_session(Channel, reply, [Req, Method, Payload]).

ack(Channel, Method, Payload, Req) ->
    call_session(Channel, piggyback, [Req, Method, Payload]).

call_session(#channel{session = Session,
                      config = Cfg} = Channel, F, A) ->
    case erlang:apply(emqx_coap_session, F, [Session, Cfg | A]) of
        #{out := Out,
          session := Session2} ->
            {ok, {outgoing, Out}, Channel#channel{session = Session2}};
        #{out := Out} ->
            {ok, {outgoing, Out}, Channel};
        #{session := Session2} ->
            {ok, Channel#channel{session = Session2}};
        _ ->
            {ok, Channel}
    end.
