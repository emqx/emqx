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

-module(emqx_gateway_test_utils).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

assert_confs(Expected0, Effected) ->
    Expected = maybe_unconvert_listeners(Expected0),
    case do_assert_confs(root, Expected, Effected) of
        false ->
            io:format(
                standard_error,
                "Expected config: ~p,\n"
                "Effected config: ~p",
                [Expected, Effected]
            ),
            exit(conf_not_match);
        true ->
            ok
    end.

do_assert_confs(_Key, Expected, Effected) when
    is_map(Expected),
    is_map(Effected)
->
    Ks1 = maps:keys(Expected),
    lists:all(
        fun(K) ->
            do_assert_confs(
                K,
                maps:get(K, Expected),
                maps:get(K, Effected, undefined)
            )
        end,
        Ks1
    );
do_assert_confs(Key, Expected, Effected) when
    Key == cacertfile;
    Key == <<"cacertfile">>;
    Key == certfile;
    Key == <<"certfile">>;
    Key == keyfile;
    Key == <<"keyfile">>
->
    case Expected == Effected of
        true ->
            true;
        false ->
            case file:read_file(Effected) of
                {ok, Content} -> Expected == Content;
                _ -> false
            end
    end;
do_assert_confs(Key, [Expected | More1], [Effected | More2]) ->
    do_assert_confs(Key, Expected, Effected) andalso
        do_assert_confs(Key, More1, More2);
do_assert_confs(_Key, [], []) ->
    true;
do_assert_confs(Key, Expected, Effected) ->
    Res = Expected =:= Effected,
    Res == false andalso
        ct:pal(
            "Errors: ~p value not match, "
            "expected: ~p, got: ~p~n",
            [Key, Expected, Effected]
        ),
    Res.

maybe_unconvert_listeners(Conf) when is_map(Conf) ->
    case maps:take(<<"listeners">>, Conf) of
        {Ls, Conf1} when is_list(Ls) ->
            Conf1#{
                <<"listeners">> =>
                    emqx_gateway_conf:unconvert_listeners(Ls)
            };
        _ ->
            Conf
    end;
maybe_unconvert_listeners(Conf) ->
    Conf.

assert_fields_exist(Ks, Map) ->
    lists:foreach(
        fun(K) ->
            _ = maps:get(K, Map)
        end,
        Ks
    ).

load_all_gateway_apps() ->
    emqx_cth_suite:load_apps(all_gateway_apps()).

all_gateway_apps() ->
    [
        emqx_gateway_stomp,
        emqx_gateway_mqttsn,
        emqx_gateway_coap,
        emqx_gateway_lwm2m,
        emqx_gateway_exproto
    ].

%%--------------------------------------------------------------------
%% http

-define(http_api_host, "http://127.0.0.1:18083/api/v5").

request(delete = Mth, Path) ->
    do_request(Mth, req(Path, []));
request(get = Mth, Path) ->
    do_request(Mth, req(Path, [])).

request(get = Mth, Path, Qs) ->
    do_request(Mth, req(Path, Qs));
request(put = Mth, Path, Body) ->
    do_request(Mth, req(Path, [], Body));
request(post = Mth, Path, Body) ->
    do_request(Mth, req(Path, [], Body)).

%%--------------------------------------------------------------------
%% default pems

ssl_server_opts() ->
    #{
        cacertfile => file_content(cert_path("cacert.pem")),
        certfile => file_content(cert_path("cert.pem")),
        keyfile => file_content(cert_path("key.pem"))
    }.

ssl_client_opts() ->
    #{
        cacertfile => file_content(cert_path("cacert.pem")),
        certfile => file_content(cert_path("client-cert.pem")),
        keyfile => file_content(cert_path("client-key.pem"))
    }.

cert_path(Name) ->
    filename:join(["../../lib/emqx/etc/certs/", Name]).

file_content(Filename) ->
    case file:read_file(Filename) of
        {ok, Bin} -> Bin;
        Err -> error(Err)
    end.

do_request(Mth, Req) ->
    case httpc:request(Mth, Req, [], [{body_format, binary}]) of
        {ok, {{_Vsn, Code, _Text}, _, Resp}} ->
            NResp =
                case Resp of
                    <<>> ->
                        #{};
                    _ ->
                        emqx_utils_maps:unsafe_atom_key_map(
                            emqx_utils_json:decode(Resp, [return_maps])
                        )
                end,
            {Code, NResp};
        {error, Reason} ->
            error({failed_to_request, Reason})
    end.

req(Path, Qs) ->
    {url(Path, Qs), auth([])}.

req(Path, Qs, Body) ->
    {url(Path, Qs), auth([]), "application/json", emqx_utils_json:encode(Body)}.

url(Path, []) ->
    lists:concat([?http_api_host, Path]);
url(Path, Qs) ->
    lists:concat([?http_api_host, Path, "?", binary_to_list(cow_qs:qs(Qs))]).

auth(Headers) ->
    [emqx_mgmt_api_test_util:auth_header_() | Headers].

sn_client_connect(ClientId) ->
    {ok, Socket} = gen_udp:open(0, [binary]),
    _ = emqx_sn_protocol_SUITE:send_connect_msg(Socket, ClientId),
    ?assertEqual(
        <<3, 16#05, 0>>,
        emqx_sn_protocol_SUITE:receive_response(Socket)
    ),
    Socket.

sn_client_disconnect(Socket) ->
    _ = emqx_sn_protocol_SUITE:send_disconnect_msg(Socket, undefined),
    gen_udp:close(Socket),
    ok.

meck_emqx_hook_calls() ->
    Self = self(),
    ok = meck:new(emqx_hooks, [passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_hooks,
        run,
        fun(A1, A2) ->
            Self ! {hook_call, A1},
            meck:passthrough([A1, A2])
        end
    ),

    ok = meck:expect(
        emqx_hooks,
        run_fold,
        fun(A1, A2, A3) ->
            Self ! {hook_call, A1},
            meck:passthrough([A1, A2, A3])
        end
    ).

collect_emqx_hooks_calls() ->
    collect_emqx_hooks_calls([]).

collect_emqx_hooks_calls(Acc) ->
    receive
        {hook_call, Args} ->
            collect_emqx_hooks_calls([Args | Acc])
    after 1000 ->
        L = lists:reverse(Acc),
        meck:unload(emqx_hooks),
        L
    end.
