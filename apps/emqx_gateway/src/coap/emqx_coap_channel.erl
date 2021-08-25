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
        , validator/3
        , get_clientinfo/1
        , get_config/2
        , get_config/3
        , result_keys/0
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
                  token :: binary() | undefined,
                  config :: hocon:config()
                 }).

%% the execuate context for session call
-record(exec_ctx, { config :: hocon:config(),
                    ctx :: emqx_gateway_ctx:context(),
                    clientinfo :: emqx_types:clientinfo()
                  }).

-type channel() :: #channel{}.
-define(DISCONNECT_WAIT_TIME, timer:seconds(10)).
-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

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
    EnableAuth = is_authentication_enabled(Config),
    ClientInfo = set_peercert_infos(
                   Peercert,
                   #{ zone => default
                    , protocol => 'coap'
                    , peerhost => PeerHost
                    , sockport => SockPort
                    , clientid => if EnableAuth ->
                                          undefined;
                                     true ->
                                          emqx_guid:to_base62(emqx_guid:gen())
                                  end
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
            , config = Config
            , session = emqx_coap_session:new()
            , keepalive = emqx_keepalive:init(maps:get(heartbeat, Config))
            }.

is_authentication_enabled(Cfg) ->
    case maps:get(authentication, Cfg, #{enable => false}) of
        AuthCfg when is_map(AuthCfg) ->
            maps:get(enable, AuthCfg, true);
        _ -> false
    end.

validator(Type, Topic, #exec_ctx{ctx = Ctx,
                                 clientinfo = ClientInfo}) ->
    emqx_gateway_ctx:authorize(Ctx, ClientInfo, Type, Topic).

get_clientinfo(#exec_ctx{clientinfo = ClientInfo}) ->
    ClientInfo.

get_config(Key, Ctx) ->
    get_config(Key, Ctx, undefined).

get_config(Key, #exec_ctx{config = Cfg}, Def) ->
    maps:get(Key, Cfg, Def).

result_keys() ->
    [out, connection].

transfer_result(From, Value, Result) ->
    ?TRANSFER_RESULT(From, Value, Result).

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------
handle_in(Msg, ChannleT) ->
    Channel = ensure_keepalive_timer(ChannleT),
    case convert_queries(Msg) of
        {ok, Msg2} ->
            case emqx_coap_message:is_request(Msg2) of
                true ->
                    check_auth_state(Msg2, Channel);
                _ ->
                    call_session(handle_response, Msg2, Channel)
            end;
        _ ->
            response({error, bad_request}, <<"bad uri_query">>, Msg, Channel)
    end.

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------
handle_deliver(Delivers, Channel) ->
    call_session(deliver, Delivers, Channel).

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------
handle_timeout(_, {keepalive, NewVal}, #channel{keepalive = KeepAlive} = Channel) ->
    case emqx_keepalive:check(NewVal, KeepAlive) of
        {ok, NewKeepAlive} ->
            Channel2 = ensure_keepalive_timer(fun make_timer/4, Channel),
            {ok, Channel2#channel{keepalive = NewKeepAlive}};
        {error, timeout} ->
            {shutdown, timeout, Channel}
    end;

handle_timeout(_, {transport, Msg}, Channel) ->
    call_session(timeout, Msg, Channel);

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

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
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
    ensure_keepalive_timer(fun ensure_timer/4, Channel).

ensure_keepalive_timer(Fun, #channel{config = Cfg} = Channel) ->
    Interval = maps:get(heartbeat, Cfg),
    Fun(keepalive, Interval, keepalive, Channel).

call_session(Fun,
             Msg,
             #channel{session = Session} = Channel) ->
    Ctx = new_exec_ctx(Channel),
    Result = erlang:apply(emqx_coap_session, Fun, [Msg, Ctx, Session]),
    process_result([session, connection, out], Result, Msg, Channel).

process_result([Key | T], Result, Msg, Channel) ->
    case handle_result(Key, Result, Msg, Channel) of
        {ok, Channel2} ->
            process_result(T, Result, Msg, Channel2);
        Other ->
            Other
    end;

process_result(_, _, _, Channel) ->
    {ok, Channel}.

handle_result(session, #{session := Session}, _, Channel) ->
    {ok, Channel#channel{session = Session}};

handle_result(connection, #{connection := open}, Msg, Channel) ->
    do_connect(Msg, Channel);

handle_result(connection, #{connection := close}, Msg, Channel) ->
    Reply = emqx_coap_message:piggyback({ok, deleted}, Msg),
    {shutdown, close, {outgoing, Reply}, Channel};

handle_result(out, #{out := Out}, _, Channel) ->
    {ok, {outgoing, Out}, Channel};

handle_result(_, _, _, Channel) ->
    {ok, Channel}.

check_auth_state(Msg, #channel{config = Cfg} = Channel) ->
    Enable = is_authentication_enabled(Cfg),
    check_token(Enable, Msg, Channel).

check_token(true,
            #coap_message{options = Options} = Msg,
            #channel{token = Token,
                     clientinfo = ClientInfo} = Channel) ->
    #{clientid := ClientId} = ClientInfo,
    case maps:get(uri_query, Options, undefined) of
        #{<<"clientid">> := ClientId,
          <<"token">> := Token} ->
            call_session(handle_request, Msg, Channel);
        #{<<"clientid">> := DesireId} ->
            try_takeover(ClientId, DesireId, Msg, Channel);
        _ ->
            response({error, unauthorized}, Msg, Channel)
    end;

check_token(false,
            #coap_message{options = Options} = Msg,
            Channel) ->
    case maps:get(uri_query, Options, undefined) of
        #{<<"clientid">> := _} ->
            response({error, unauthorized}, Msg, Channel);
        #{<<"token">> := _} ->
            response({error, unauthorized}, Msg, Channel);
        _ ->
            call_session(handle_request, Msg, Channel)
    end.

response(Method, Req, Channel) ->
    response(Method, <<>>, Req, Channel).

response(Method, Payload, Req, Channel) ->
    Reply = emqx_coap_message:piggyback(Method, Payload, Req),
    call_session(handle_out, Reply, Channel).

try_takeover(undefined,
             DesireId,
             #coap_message{options = Opts} = Msg,
             Channel) ->
    case maps:get(uri_path, Opts, []) of
          [<<"mqtt">>, <<"connection">> | _] ->
            %% may be is a connect request
            %% TODO need check repeat connect, unless we implement the
            %% udp connection baseon the clientid
            call_session(handle_request, Msg, Channel);
        _ ->
            do_takeover(DesireId, Msg, Channel)
    end;

try_takeover(_, DesireId, Msg, Channel) ->
    do_takeover(DesireId, Msg, Channel).

do_takeover(_DesireId, Msg, Channel) ->
    %% TODO completed the takeover, now only reset the message
    Reset = emqx_coap_message:reset(Msg),
    call_session(handle_out, Reset, Channel).

new_exec_ctx(#channel{config = Cfg,
                      ctx = Ctx,
                      clientinfo = ClientInfo}) ->
    #exec_ctx{config = Cfg,
              ctx = Ctx,
              clientinfo = ClientInfo}.

do_connect(#coap_message{options = Opts} = Req, Channel) ->
    Queries = maps:get(uri_query, Opts),
    case emqx_misc:pipeline(
           [ fun run_conn_hooks/2
           , fun enrich_clientinfo/2
           , fun set_log_meta/2
           , fun auth_connect/2
           ],
           {Queries, Req},
           Channel) of
        {ok, _Input, NChannel} ->
            process_connect(ensure_connected(NChannel), Req);
        {error, ReasonCode, NChannel} ->
            ErrMsg = io_lib:format("Login Failed: ~s", [ReasonCode]),
            response({error, bad_request}, ErrMsg, Req, NChannel)
    end.

run_conn_hooks(Input, Channel = #channel{ctx = Ctx,
                                         conninfo = ConnInfo}) ->
    ConnProps = #{},
    case run_hooks(Ctx, 'client.connect', [ConnInfo], ConnProps) of
        Error = {error, _Reason} -> Error;
        _NConnProps ->
            {ok, Input, Channel}
    end.

enrich_clientinfo({Queries, Msg},
                  Channel = #channel{clientinfo = ClientInfo0}) ->
    case Queries of
        #{<<"username">> := UserName,
          <<"password">> := Password,
          <<"clientid">> := ClientId} ->
            ClientInfo = ClientInfo0#{username => UserName,
                                      password => Password,
                                      clientid => ClientId},
            {ok, NClientInfo} = fix_mountpoint(Msg, ClientInfo),
            {ok, Channel#channel{clientinfo = NClientInfo}};
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

fix_mountpoint(_Packet, #{mountpoint := undefined} = ClientInfo) ->
    {ok, ClientInfo};
fix_mountpoint(_Packet, ClientInfo = #{mountpoint := Mountpoint}) ->
    %% TODO: Enrich the varibale replacement????
    %%       i.e: ${ClientInfo.auth_result.productKey}
    Mountpoint1 = emqx_mountpoint:replvar(Mountpoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := Mountpoint1}}.

ensure_connected(Channel = #channel{ctx = Ctx,
                                    conninfo = ConnInfo,
                                    clientinfo = ClientInfo}) ->
    NConnInfo = ConnInfo#{ connected_at => erlang:system_time(millisecond)
                         , proto_name => <<"COAP">>
                         , proto_ver => <<"1">>
                         },
    ok = run_hooks(Ctx, 'client.connected', [ClientInfo, NConnInfo]),
    Channel#channel{conninfo = NConnInfo}.

process_connect(Channel = #channel{ctx = Ctx,
                                   session = Session,
                                   conninfo = ConnInfo,
                                   clientinfo = ClientInfo},
                Msg) ->
    %% inherit the old session
    SessFun = fun(_,_) -> Session end,
    case emqx_gateway_ctx:open_session(
           Ctx,
           true,
           ClientInfo,
           ConnInfo,
           SessFun,
           emqx_coap_session
          ) of
        {ok, _Sess} ->
            response({ok, created}, <<"connected">>, Msg, Channel);
        {error, Reason} ->
            ?LOG(error, "Failed to open session du to ~p", [Reason]),
            response({error, bad_request}, Msg, Channel)
    end.

run_hooks(Ctx, Name, Args) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run(Name, Args).

run_hooks(Ctx, Name, Args, Acc) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run_fold(Name, Args, Acc).

convert_queries(#coap_message{options = Opts} = Msg) ->
    case maps:get(uri_query, Opts, undefined) of
        undefined ->
            {ok, Msg#coap_message{options = Opts#{uri_query => #{}}}};
        Queries ->
            convert_queries(Queries, #{}, Msg)
    end.

convert_queries([H | T], Queries, Msg) ->
    case re:split(H, "=") of
        [Key, Val] ->
            convert_queries(T, Queries#{Key => Val}, Msg);
        _ ->
            error
    end;
convert_queries([], Queries, #coap_message{options = Opts} = Msg) ->
    {ok, Msg#coap_message{options = Opts#{uri_query => Queries}}}.
