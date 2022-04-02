%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_authn_http_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(PATH, [?CONF_NS_ATOM]).

-define(HTTP_PORT, 33333).
-define(HTTP_PATH, "/auth").

-define(checkMatch(Guard),
    (fun(Expr) ->
        case (Expr) of
            Guard ->
                ok;
            X__V ->
                erlang:error(
                    {assertMatch, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {expression, (??Expr)},
                        {pattern, (??Guard)},
                        {value, X__V}
                    ]}
                )
        end
    end)
).
-define(FUNCTOR(Expr), fun() -> Expr end).
-define(FUNCTOR(Arg, Expr), fun(Arg) -> Expr end).

-define(PROTOCOLS, [coap, lwm2m, 'mqtt-sn', stomp, exproto]).

-define(CONFS, [
    emqx_coap_SUITE,
    emqx_lwm2m_SUITE,
    emqx_sn_protocol_SUITE,
    emqx_stomp_SUITE,
    emqx_exproto_SUITE
]).

-define(CASES, [
    fun case_coap/0,
    fun case_lwm2m/0,
    fun case_emqx_sn/0,
    fun case_stomp/0,
    fun case_exproto/0
]).

-define(AUTHNS, [fun set_http_authn/1]).

-type auth_controller() :: fun((start | stop) -> ok).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    ok = emqx_common_test_helpers:load_config(emqx_gateway_schema, init_conf()),
    emqx_common_test_helpers:start_apps([emqx_authn, emqx_gateway]),
    application:ensure_all_started(cowboy),
    Config.

end_per_suite(_) ->
    clear_authn(),
    ok = emqx_common_test_helpers:load_config(emqx_gateway_schema, <<>>),
    emqx_common_test_helpers:stop_apps([emqx_authn, emqx_gateway]),
    application:stop(cowboy),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

end_per_testcase(_Case, Config) ->
    Config.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------
t_authn(_) ->
    test_gateway_with_auths(?CASES, ?AUTHNS).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

case_coap() ->
    Login = fun(URI, Checker) ->
        Action = fun(Channel) ->
            Req = emqx_coap_SUITE:make_req(post),
            Checker(emqx_coap_SUITE:do_request(Channel, URI, Req))
        end,
        emqx_coap_SUITE:do(Action)
    end,
    Prefix = emqx_coap_SUITE:mqtt_prefix(),
    RightUrl =
        Prefix ++
            "/connection?clientid=client1&username=admin&password=public",
    Login(RightUrl, ?checkMatch({ok, created, _Data})),

    LeftUrl =
        Prefix ++
            "/connection?clientid=client1&username=bad&password=bad",
    Login(LeftUrl, ?checkMatch({error, bad_request, _Data})),
    ok.

-record(coap_content, {content_format, payload = <<>>}).

case_lwm2m() ->
    MsgId = 12,
    Mod = emqx_lwm2m_SUITE,
    Epn = "urn:oma:lwm2m:oma:3",
    Port = emqx_lwm2m_SUITE:default_port(),
    Login = fun(URI, Checker) ->
        with_resource(
            ?FUNCTOR(gen_udp:open(0, [binary, {active, false}])),
            ?FUNCTOR(Socket, gen_udp:close(Socket)),
            fun(Socket) ->
                Mod:test_send_coap_request(
                    Socket,
                    post,
                    Mod:sprintf(URI, [Port, Epn]),
                    #coap_content{
                        content_format = <<"text/plain">>,
                        payload = <<"</1>, </2>, </3>, </4>, </5>">>
                    },
                    [],
                    MsgId
                ),

                Checker(Mod:test_recv_coap_response(Socket))
            end
        )
    end,

    MakeCheker = fun(Type, Method) ->
        fun(Msg) ->
            ?assertEqual(Type, emqx_coap_SUITE:get_field(type, Msg)),
            ?assertEqual(Method, emqx_coap_SUITE:get_field(method, Msg))
        end
    end,

    RightUrl = "coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1&imei=admin&password=public",
    Login(RightUrl, MakeCheker(ack, {ok, created})),

    LeftUrl = "coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1&imei=bad&password=bad",
    Login(LeftUrl, MakeCheker(ack, {error, bad_request})),

    NoInfoUrl = "coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1",
    Login(NoInfoUrl, MakeCheker(ack, {error, bad_request})),
    ok.

-define(SN_CONNACK, 16#05).

case_emqx_sn() ->
    Mod = emqx_sn_protocol_SUITE,
    Login = fun(Expect) ->
        with_resource(
            ?FUNCTOR(gen_udp:open(0, [binary])),
            ?FUNCTOR(Socket, gen_udp:close(Socket)),
            fun(Socket) ->
                Mod:send_connect_msg(Socket, <<"client_id_test1">>),
                ?assertEqual(Expect, Mod:receive_response(Socket))
            end
        )
    end,
    Login(<<>>),

    RawCfg = emqx_conf:get_raw([gateway, mqttsn], #{}),
    NewCfg = RawCfg#{
        <<"clientinfo_override">> => #{
            <<"username">> => <<"admin">>,
            <<"password">> => <<"public">>
        }
    },
    emqx_gateway_conf:update_gateway(mqttsn, NewCfg),
    Login(<<3, ?SN_CONNACK, 0>>),
    ok.

case_stomp() ->
    Mod = emqx_stomp_SUITE,
    Login = fun(Username, Password, Checker) ->
        Fun = fun(Sock) ->
            gen_tcp:send(
                Sock,
                Mod:serialize(
                    <<"CONNECT">>,
                    [
                        {<<"accept-version">>, Mod:stomp_ver()},
                        {<<"host">>, <<"127.0.0.1:61613">>},
                        {<<"login">>, Username},
                        {<<"passcode">>, Password},
                        {<<"heart-beat">>, <<"1000,2000">>}
                    ]
                )
            ),
            {ok, Data} = gen_tcp:recv(Sock, 0),
            {ok, Frame, _, _} = Mod:parse(Data),
            Checker(Frame)
        end,
        Mod:with_connection(Fun)
    end,
    Login(
        <<"admin">>,
        <<"public">>,
        ?FUNCTOR(
            Frame,
            ?assertEqual(<<"CONNECTED">>, Mod:get_field(command, Frame))
        )
    ),
    Login(<<"bad">>, <<"bad">>, fun(Frame) ->
        ?assertEqual(<<"ERROR">>, Mod:get_field(command, Frame)),
        ?assertEqual(<<"Login Failed: not_authorized">>, Mod:get_field(body, Frame))
    end),

    ok.

case_exproto() ->
    Mod = emqx_exproto_SUITE,
    SvrMod = emqx_exproto_echo_svr,
    Svrs = SvrMod:start(),
    Login = fun(Username, Password, Expect) ->
        with_resource(
            ?FUNCTOR(Mod:open(tcp)),
            ?FUNCTOR(Sock, Mod:close(Sock)),
            fun(Sock) ->
                Client = #{
                    proto_name => <<"demo">>,
                    proto_ver => <<"v0.1">>,
                    clientid => <<"test_client_1">>,
                    username => Username
                },

                ConnBin = SvrMod:frame_connect(Client, Password),

                Mod:send(Sock, ConnBin),
                {ok, Recv} = Mod:recv(Sock, 5000),
                C = ?FUNCTOR(Bin, emqx_json:decode(Bin, [return_maps])),
                ?assertEqual(C(Expect), C(Recv))
            end
        )
    end,
    Login(<<"admin">>, <<"public">>, SvrMod:frame_connack(0)),
    Login(<<"bad">>, <<"bad">>, SvrMod:frame_connack(1)),
    SvrMod:stop(Svrs),
    ok.

%%------------------------------------------------------------------------------
%% Authenticators
%%------------------------------------------------------------------------------

raw_http_auth_config() ->
    #{
        mechanism => <<"password_based">>,
        enable => <<"true">>,

        backend => <<"http">>,
        method => <<"get">>,
        url => <<"http://127.0.0.1:33333/auth">>,
        body => #{<<"username">> => ?PH_USERNAME, <<"password">> => ?PH_PASSWORD},
        headers => #{<<"X-Test-Header">> => <<"Test Value">>}
    }.

set_http_authn(start) ->
    {ok, _} = emqx_authn_http_test_server:start_link(?HTTP_PORT, ?HTTP_PATH),

    AuthConfig = raw_http_auth_config(),

    Set = fun(Protocol) ->
        Chain = emqx_authentication:global_chain(Protocol),
        emqx_authn_test_lib:delete_authenticators([authentication], Chain),

        {ok, _} = emqx:update_config(
            ?PATH,
            {create_authenticator, Chain, AuthConfig}
        ),

        {ok, [#{provider := emqx_authn_http}]} = emqx_authentication:list_authenticators(Chain)
    end,
    lists:foreach(Set, ?PROTOCOLS),

    Handler = fun(Req0, State) ->
        ct:pal("Req:~p State:~p~n", [Req0, State]),
        case cowboy_req:match_qs([username, password], Req0) of
            #{
                username := <<"admin">>,
                password := <<"public">>
            } ->
                Req = cowboy_req:reply(200, Req0);
            _ ->
                Req = cowboy_req:reply(400, Req0)
        end,
        {ok, Req, State}
    end,
    emqx_authn_http_test_server:set_handler(Handler);
set_http_authn(stop) ->
    ok = emqx_authn_http_test_server:stop().

clear_authn() ->
    Clear = fun(Protocol) ->
        Chain = emqx_authentication:global_chain(Protocol),
        emqx_authn_test_lib:delete_authenticators([authentication], Chain)
    end,
    lists:foreach(Clear, ?PROTOCOLS).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------
-spec test_gateway_with_auths(_, list(auth_controller())) -> ok.
test_gateway_with_auths(Gateways, Authenticators) ->
    Cases = [{Auth, Gateways} || Auth <- Authenticators],
    test_gateway_with_auths(Cases).

test_gateway_with_auths([{Auth, Gateways} | T]) ->
    {name, Name} = erlang:fun_info(Auth, name),
    ct:pal("start auth:~p~n", [Name]),
    Auth(start),
    lists:foreach(
        fun(Gateway) ->
            {name, GwName} = erlang:fun_info(Gateway, name),
            ct:pal("start gateway case:~p~n", [GwName]),
            Gateway()
        end,
        Gateways
    ),
    ct:pal("stop auth:~p~n", [Name]),
    Auth(stop),
    test_gateway_with_auths(T);
test_gateway_with_auths([]) ->
    ok.

init_conf() ->
    merge_conf([X:default_config() || X <- ?CONFS], []).

merge_conf([Conf | T], Acc) ->
    case re:run(Conf, "\s*gateway\\.(.*)", [global, {capture, all_but_first, list}, dotall]) of
        {match, [[Content]]} ->
            merge_conf(T, [Content | Acc]);
        _ ->
            merge_conf(T, Acc)
    end;
merge_conf([], Acc) ->
    erlang:list_to_binary("gateway{" ++ string:join(Acc, ",") ++ "}").

with_resource(Init, Close, Fun) ->
    Res =
        case Init() of
            {ok, X} -> X;
            Other -> Other
        end,
    try
        Fun(Res)
    catch
        C:R:S ->
            Close(Res),
            erlang:raise(C, R, S)
    end.
