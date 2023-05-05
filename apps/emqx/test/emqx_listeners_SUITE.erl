%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_listeners_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CERTS_PATH(CertName), filename:join(["../../lib/emqx/etc/certs/", CertName])).

-define(SERVER_KEY_PASSWORD, "sErve7r8Key$!").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    NewConfig = generate_config(),
    application:ensure_all_started(esockd),
    application:ensure_all_started(quicer),
    application:ensure_all_started(cowboy),
    generate_tls_certs(Config),
    lists:foreach(fun set_app_env/1, NewConfig),
    Config.

end_per_suite(_Config) ->
    application:stop(esockd),
    application:stop(cowboy).

init_per_testcase(Case, Config) when
    Case =:= t_max_conns_tcp; Case =:= t_current_conns_tcp
->
    catch emqx_config_handler:stop(),
    Port = emqx_common_test_helpers:select_free_port(tcp),
    {ok, _} = emqx_config_handler:start_link(),
    PrevListeners = emqx_config:get([listeners], #{}),
    PureListeners = remove_default_limiter(PrevListeners),
    PureListeners2 = PureListeners#{
        tcp => #{
            listener_test => #{
                bind => {"127.0.0.1", Port},
                max_connections => 4321,
                limiter => #{}
            }
        }
    },
    emqx_config:put([listeners], PureListeners2),

    ok = emqx_listeners:start(),
    [
        {prev_listener_conf, PrevListeners},
        {tcp_port, Port}
        | Config
    ];
init_per_testcase(t_wss_conn, Config) ->
    catch emqx_config_handler:stop(),
    Port = emqx_common_test_helpers:select_free_port(ssl),
    {ok, _} = emqx_config_handler:start_link(),
    PrevListeners = emqx_config:get([listeners], #{}),
    PureListeners = remove_default_limiter(PrevListeners),
    PureListeners2 = PureListeners#{
        wss => #{
            listener_test => #{
                bind => {{127, 0, 0, 1}, Port},
                limiter => #{},
                ssl_options => #{
                    cacertfile => ?CERTS_PATH("cacert.pem"),
                    certfile => ?CERTS_PATH("cert.pem"),
                    keyfile => ?CERTS_PATH("key.pem")
                }
            }
        }
    },
    emqx_config:put([listeners], PureListeners2),

    ok = emqx_listeners:start(),
    [
        {prev_listener_conf, PrevListeners},
        {wss_port, Port}
        | Config
    ];
init_per_testcase(_, Config) ->
    catch emqx_config_handler:stop(),
    {ok, _} = emqx_config_handler:start_link(),
    PrevListeners = emqx_config:get([listeners], #{}),
    PureListeners = remove_default_limiter(PrevListeners),
    emqx_config:put([listeners], PureListeners),
    [
        {prev_listener_conf, PrevListeners}
        | Config
    ].

end_per_testcase(Case, Config) when
    Case =:= t_max_conns_tcp; Case =:= t_current_conns_tcp
->
    PrevListener = ?config(prev_listener_conf, Config),
    emqx_listeners:stop(),
    emqx_config:put([listeners], PrevListener),
    _ = emqx_config_handler:stop(),
    ok;
end_per_testcase(t_wss_conn, Config) ->
    PrevListener = ?config(prev_listener_conf, Config),
    emqx_listeners:stop(),
    emqx_config:put([listeners], PrevListener),
    _ = emqx_config_handler:stop(),
    ok;
end_per_testcase(_, Config) ->
    PrevListener = ?config(prev_listener_conf, Config),
    emqx_config:put([listeners], PrevListener),
    _ = emqx_config_handler:stop(),
    ok.

t_start_stop_listeners(_) ->
    ok = emqx_listeners:start(),
    ?assertException(error, _, emqx_listeners:start_listener({ws, {"127.0.0.1", 8083}, []})),
    ok = emqx_listeners:stop().

t_restart_listeners(_) ->
    ok = emqx_listeners:start(),
    ok = emqx_listeners:stop(),
    %% flakyness: eaddrinuse
    timer:sleep(timer:seconds(2)),
    ok = emqx_listeners:restart(),
    ok = emqx_listeners:stop().

t_restart_listeners_with_hibernate_after_disabled(_Config) ->
    OldLConf = emqx_config:get([listeners]),
    maps:foreach(
        fun(LType, Listeners) ->
            maps:foreach(
                fun(Name, Opts) ->
                    case maps:is_key(ssl_options, Opts) of
                        true ->
                            emqx_config:put(
                                [
                                    listeners,
                                    LType,
                                    Name,
                                    ssl_options,
                                    hibernate_after
                                ],
                                undefined
                            );
                        _ ->
                            skip
                    end
                end,
                Listeners
            )
        end,
        OldLConf
    ),
    ok = emqx_listeners:start(),
    ok = emqx_listeners:stop(),
    %% flakyness: eaddrinuse
    timer:sleep(timer:seconds(2)),
    ok = emqx_listeners:restart(),
    ok = emqx_listeners:stop(),
    emqx_config:put([listeners], OldLConf).

t_max_conns_tcp(Config) ->
    %% Note: Using a string representation for the bind address like
    %% "127.0.0.1" does not work
    ?assertEqual(
        4321,
        emqx_listeners:max_conns('tcp:listener_test', {{127, 0, 0, 1}, ?config(tcp_port, Config)})
    ).

t_current_conns_tcp(Config) ->
    ?assertEqual(
        0,
        emqx_listeners:current_conns('tcp:listener_test', {
            {127, 0, 0, 1}, ?config(tcp_port, Config)
        })
    ).

t_wss_conn(Config) ->
    {ok, Socket} = ssl:connect(
        {127, 0, 0, 1}, ?config(wss_port, Config), [{verify, verify_none}], 1000
    ),
    ok = ssl:close(Socket).

t_quic_conn(Config) ->
    Port = emqx_common_test_helpers:select_free_port(quic),
    DataDir = ?config(data_dir, Config),
    SSLOpts = #{
        password => ?SERVER_KEY_PASSWORD,
        certfile => filename:join(DataDir, "server-password.pem"),
        cacertfile => filename:join(DataDir, "ca.pem"),
        keyfile => filename:join(DataDir, "server-password.key")
    },
    emqx_common_test_helpers:ensure_quic_listener(?FUNCTION_NAME, Port, #{ssl_options => SSLOpts}),
    ct:pal("~p", [emqx_listeners:list()]),
    {ok, Conn} = quicer:connect(
        {127, 0, 0, 1},
        Port,
        [
            {verify, verify_none},
            {alpn, ["mqtt"]}
        ],
        1000
    ),
    ok = quicer:close_connection(Conn),
    emqx_listeners:stop_listener(quic, ?FUNCTION_NAME, #{bind => Port}).

t_ssl_password_cert(Config) ->
    Port = emqx_common_test_helpers:select_free_port(ssl),
    DataDir = ?config(data_dir, Config),
    SSLOptsPWD = #{
        password => ?SERVER_KEY_PASSWORD,
        certfile => filename:join(DataDir, "server-password.pem"),
        cacertfile => filename:join(DataDir, "ca.pem"),
        keyfile => filename:join(DataDir, "server-password.key")
    },
    LConf = #{
        enabled => true,
        bind => {{127, 0, 0, 1}, Port},
        mountpoint => <<>>,
        zone => default,
        ssl_options => SSLOptsPWD
    },
    ok = emqx_listeners:start_listener(ssl, ?FUNCTION_NAME, LConf),
    {ok, SSLSocket} = ssl:connect("127.0.0.1", Port, [{verify, verify_none}]),
    ssl:close(SSLSocket),
    emqx_listeners:stop_listener(ssl, ?FUNCTION_NAME, LConf).

t_format_bind(_) ->
    ?assertEqual(
        ":1883",
        lists:flatten(emqx_listeners:format_bind(1883))
    ),
    ?assertEqual(
        "0.0.0.0:1883",
        lists:flatten(emqx_listeners:format_bind({{0, 0, 0, 0}, 1883}))
    ),
    ?assertEqual(
        "[::]:1883",
        lists:flatten(emqx_listeners:format_bind({{0, 0, 0, 0, 0, 0, 0, 0}, 1883}))
    ),
    ?assertEqual(
        "127.0.0.1:1883",
        lists:flatten(emqx_listeners:format_bind({{127, 0, 0, 1}, 1883}))
    ),
    ?assertEqual(
        ":1883",
        lists:flatten(emqx_listeners:format_bind("1883"))
    ),
    ?assertEqual(
        ":1883",
        lists:flatten(emqx_listeners:format_bind(":1883"))
    ).

render_config_file() ->
    Path = local_path(["etc", "emqx.conf"]),
    {ok, Temp} = file:read_file(Path),
    Vars0 = mustache_vars(),
    Vars = [{atom_to_list(N), iolist_to_binary(V)} || {N, V} <- Vars0],
    Targ = bbmustache:render(Temp, Vars),
    NewName = Path ++ ".rendered",
    ok = file:write_file(NewName, Targ),
    NewName.

mustache_vars() ->
    [
        {platform_data_dir, local_path(["data"])},
        {platform_etc_dir, local_path(["etc"])}
    ].

generate_config() ->
    ConfFile = render_config_file(),
    {ok, Conf} = hocon:load(ConfFile, #{format => richmap}),
    hocon_tconf:generate(emqx_schema, Conf).

set_app_env({App, Lists}) ->
    lists:foreach(
        fun
            ({authz_file, _Var}) ->
                application:set_env(App, authz_file, local_path(["etc", "authz.conf"]));
            ({Par, Var}) ->
                application:set_env(App, Par, Var)
        end,
        Lists
    ).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

local_path(Components) ->
    local_path(Components, ?MODULE).

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

get_base_dir() ->
    get_base_dir(?MODULE).

remove_default_limiter(Listeners) ->
    maps:map(
        fun(_, X) ->
            maps:map(
                fun(_, E) ->
                    maps:remove(limiter, E)
                end,
                X
            )
        end,
        Listeners
    ).

generate_tls_certs(Config) ->
    DataDir = ?config(data_dir, Config),
    emqx_common_test_helpers:gen_ca(DataDir, "ca"),
    emqx_common_test_helpers:gen_host_cert("server-password", "ca", DataDir, #{
        password => ?SERVER_KEY_PASSWORD
    }).
