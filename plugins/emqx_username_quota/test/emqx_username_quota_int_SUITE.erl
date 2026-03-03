%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_int_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    EmqxConfig = #{
        listeners => #{
            tcp => #{default => #{bind => 18884}},
            ssl => #{default => #{bind => 0}},
            ws => #{default => #{bind => 0}},
            wss => #{default => #{bind => 0}}
        }
    },
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, #{config => EmqxConfig}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
            emqx_username_quota
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    ok = emqx_username_quota:reset(),
    ok = emqx_username_quota_config:update(#{
        <<"max_sessions_per_username">> => 100,
        <<"snapshot_request_timeout_ms">> => 60000
    }),
    Config.

t_quota_enforced_with_real_mqtt_and_http(_Config) ->
    Port = mqtt_tcp_port(),
    User = <<"alice">>,
    Clients = [connect_client(Port, User, new_clientid(N)) || N <- lists:seq(1, 100)],
    ClientIds = [new_clientid(N) || N <- lists:seq(1, 100)],
    try
        ok = wait_client_count(User, 100, 400),
        {error, _} = connect_client_expect_error(Port, User, <<"new-client">>),
        {204, []} = kickout_clients(ClientIds),
        ok = wait_client_count(User, 0, 400),
        Client = connect_client(Port, User, <<"new-client">>),
        stop_client(Client)
    after
        stop_clients(Clients)
    end.

t_config_change_max_sessions_runtime(_Config) ->
    Port = mqtt_tcp_port(),
    User = <<"cfg-limit-user">>,
    ok = emqx_username_quota_config:update(#{
        <<"max_sessions_per_username">> => 1
    }),
    C1 = connect_client(Port, User, <<"cfg-c1">>),
    try
        ok = wait_client_count(User, 1, 200),
        {error, _} = connect_client_expect_error(Port, User, <<"cfg-c2">>),
        ok = emqx_username_quota_config:update(#{
            <<"max_sessions_per_username">> => 2
        }),
        C2 = connect_client(Port, User, <<"cfg-c2">>),
        stop_client(C2)
    after
        stop_client(C1)
    end.

t_config_change_whitelist_runtime(_Config) ->
    Port = mqtt_tcp_port(),
    User = <<"cfg-vip-user">>,
    ok = emqx_username_quota_config:update(#{
        <<"max_sessions_per_username">> => 1,
        <<"username_white_list">> => [#{<<"username">> => User}]
    }),
    C1 = connect_client(Port, User, <<"vip-c1">>),
    C2 = connect_client(Port, User, <<"vip-c2">>),
    try
        ok = wait_client_count(User, 2, 200)
    after
        stop_client(C1),
        stop_client(C2)
    end.

t_api_404_without_mocks(_Config) ->
    Missing = <<"no_existed_clientid">>,
    {404, #{<<"code">> := <<"CLIENTID_NOT_FOUND">>}} = get_client(Missing),
    {404, #{<<"code">> := <<"CLIENTID_NOT_FOUND">>}} = delete_client(Missing).

t_list_clients_with_real_http(_Config) ->
    Port = mqtt_tcp_port(),
    User = <<"http-user">>,
    Client = connect_client(Port, User, <<"c1">>),
    try
        ok = wait_client_count(User, 1, 200),
        {200, #{<<"data">> := Data, <<"meta">> := #{<<"count">> := 1}}} = list_clients_by_username(
            User
        ),
        ?assert(
            lists:any(
                fun(#{<<"username">> := U, <<"clientid">> := C}) ->
                    U =:= User andalso C =:= <<"c1">>
                end,
                Data
            )
        )
    after
        stop_client(Client)
    end.

mqtt_tcp_port() ->
    18884.

new_clientid(N) ->
    list_to_binary(io_lib:format("c~p", [N])).

connect_client(Port, Username, ClientId) ->
    Opts = [
        {host, "127.0.0.1"},
        {port, Port},
        {proto_ver, v5},
        {clientid, ClientId},
        {username, Username}
    ],
    {ok, Pid} = emqtt:start_link(Opts),
    monitor(process, Pid),
    unlink(Pid),
    case emqtt:connect(Pid) of
        {ok, _} ->
            Pid;
        {error, Reason} ->
            stop_client(Pid),
            erlang:error({connect_failed, Reason})
    end.

connect_client_expect_error(Port, Username, ClientId) ->
    Opts = [
        {host, "127.0.0.1"},
        {port, Port},
        {proto_ver, v5},
        {clientid, ClientId},
        {username, Username}
    ],
    {ok, Pid} = emqtt:start_link(Opts),
    monitor(process, Pid),
    unlink(Pid),
    case emqtt:connect(Pid) of
        {error, _} = Error ->
            stop_client(Pid),
            Error;
        {ok, _} ->
            stop_client(Pid),
            erlang:error(connect_should_fail)
    end.

stop_clients(Clients) ->
    lists:foreach(fun stop_client/1, Clients).

stop_client(Pid) ->
    catch emqtt:stop(Pid),
    receive
        {'DOWN', _, process, Pid, _Reason} -> ok
    after 1000 ->
        ok
    end.

wait_client_count(User, Expected, Retries) when Retries > 0 ->
    case list_clients_by_username(User) of
        {200, #{<<"meta">> := #{<<"count">> := Expected}}} ->
            ok;
        _ ->
            timer:sleep(25),
            wait_client_count(User, Expected, Retries - 1)
    end;
wait_client_count(User, Expected, 0) ->
    {200, #{<<"meta">> := #{<<"count">> := Expected}}} = list_clients_by_username(User),
    ok.

list_clients_by_username(Username) ->
    request(get, [<<"clients">>], #{<<"username">> => Username, <<"limit">> => <<"1000">>}, []).

kickout_clients(ClientIds) ->
    request(post, [<<"clients">>, <<"kickout">>, <<"bulk">>], [], ClientIds).

get_client(ClientId) ->
    request(get, [<<"clients">>, ClientId], [], []).

delete_client(ClientId) ->
    request(delete, [<<"clients">>, ClientId], [], []).

request(Method, Parts, QueryParams, Body) ->
    Path = emqx_mgmt_api_test_util:api_path(Parts),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    Res = emqx_mgmt_api_test_util:request_api(Method, Path, QueryParams, Auth, Body, Opts),
    emqx_mgmt_api_test_util:simplify_decode_result(Res).
