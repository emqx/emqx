%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(NEW_CLIENTID(I),
    iolist_to_binary("c-" ++ atom_to_list(?FUNCTION_NAME) ++ "-" ++ integer_to_list(I))
).

-define(NEW_USERNAME(), iolist_to_binary("u-" ++ atom_to_list(?FUNCTION_NAME))).

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
            {emqx_mt, "multi_tenancy.default_max_sessions = 10"},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(Case, Config) ->
    snabbkaffe:start_trace(),
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    snabbkaffe:stop(),
    ?MODULE:Case({'end', Config}),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connect(ClientId, Username) ->
    Opts = [
        {clientid, ClientId},
        {username, Username},
        {password, "123456"},
        {proto_ver, v5}
    ],
    {ok, Pid} = emqtt:start_link(Opts),
    monitor(process, Pid),
    unlink(Pid),
    case emqtt:connect(Pid) of
        {ok, _} ->
            Pid;
        {error, _Reason} = E ->
            stop_client(Pid),
            erlang:error(E)
    end.

stop_client(Pid) ->
    catch emqtt:stop(Pid),
    receive
        {'DOWN', _, process, Pid, _, _} -> ok
    after 3000 ->
        exit(Pid, kill)
    end.

url(Path) ->
    emqx_mgmt_api_test_util:api_path(["mt", Path]).

ns_url(Ns, Path) ->
    emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, Path]).

count_clients(Ns) ->
    URL = ns_url(Ns, "client_count"),
    simple_request(#{method => get, url => URL}).

list_clients(Ns, QueryParams) ->
    URL = ns_url(Ns, "client_list"),
    simple_request(#{method => get, url => URL, query_params => QueryParams}).

list_nss(QueryParams) ->
    URL = url("ns_list"),
    simple_request(#{method => get, url => URL, query_params => QueryParams}).

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

simplify_result(Res) ->
    case Res of
        {error, {{_, StatusCode, _}, Body}} ->
            {StatusCode, Body};
        {ok, {{_, StatusCode, _}, Body}} ->
            {StatusCode, Body}
    end.

simple_request(Params) ->
    emqx_mgmt_api_test_util:simple_request(Params).

simple_request(Method, Path, Body, QueryParams) ->
    emqx_mgmt_api_test_util:simple_request(Method, Path, Body, QueryParams).

simple_request(Method, Path, Body) ->
    emqx_mgmt_api_test_util:simple_request(Method, Path, Body).

get_tenant_limiter(Ns) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "limiter", "tenant"]),
    Res = simple_request(get, Path, ""),
    ct:pal("get tenant limiter result:\n  ~p", [Res]),
    Res.

create_tenant_limiter(Ns, Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "limiter", "tenant"]),
    Res = simple_request(post, Path, Params),
    ct:pal("create tenant limiter result:\n  ~p", [Res]),
    Res.

update_tenant_limiter(Ns, Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "limiter", "tenant"]),
    Res = simple_request(put, Path, Params),
    ct:pal("update tenant limiter result:\n  ~p", [Res]),
    Res.

delete_tenant_limiter(Ns) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "limiter", "tenant"]),
    Res = simple_request(delete, Path, ""),
    ct:pal("delete tenant limiter result:\n  ~p", [Res]),
    Res.

get_client_limiter(Ns) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "limiter", "client"]),
    Res = simple_request(get, Path, ""),
    ct:pal("get client limiter result:\n  ~p", [Res]),
    Res.

create_client_limiter(Ns, Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "limiter", "client"]),
    Res = simple_request(post, Path, Params),
    ct:pal("create client limiter result:\n  ~p", [Res]),
    Res.

update_client_limiter(Ns, Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "limiter", "client"]),
    Res = simple_request(put, Path, Params),
    ct:pal("update client limiter result:\n  ~p", [Res]),
    Res.

delete_client_limiter(Ns) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "limiter", "client"]),
    Res = simple_request(delete, Path, ""),
    ct:pal("delete client limiter result:\n  ~p", [Res]),
    Res.

tenant_limiter_params() ->
    tenant_limiter_params(_Overrides = #{}).

tenant_limiter_params(Overrides) ->
    Defaults = #{
        <<"bytes">> => #{
            <<"rate">> => <<"10MB/10s">>,
            <<"burst">> => <<"200MB/1m">>
        },
        <<"messages">> => #{
            <<"rate">> => <<"3000/1s">>,
            <<"burst">> => <<"40/1m">>
        }
    },
    emqx_utils_maps:deep_merge(Defaults, Overrides).

client_limiter_params() ->
    client_limiter_params(_Overrides = #{}).

client_limiter_params(Overrides) ->
    tenant_limiter_params(Overrides).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_list_apis({init, Config}) ->
    Config;
t_list_apis({'end', _Config}) ->
    ok;
t_list_apis(_Config) ->
    N = 9,
    ClientIds = [?NEW_CLIENTID(I) || I <- lists:seq(1, N)],
    Ns = ?NEW_USERNAME(),
    Clients = [connect(ClientId, Ns) || ClientId <- ClientIds],
    ?retry(200, 50, ?assertEqual({ok, N}, emqx_mt:count_clients(Ns))),
    ?assertMatch({200, #{<<"count">> := N}}, count_clients(Ns)),
    {200, ClientIds0} = list_clients(Ns, #{<<"limit">> => integer_to_binary(N div 2)}),
    LastClientId = lists:last(ClientIds0),
    {200, ClientIds1} =
        list_clients(Ns, #{
            <<"last_clientid">> => LastClientId,
            <<"limit">> => integer_to_binary(N)
        }),
    ?assertEqual(ClientIds, ClientIds0 ++ ClientIds1),
    ok = lists:foreach(fun stop_client/1, Clients),
    ?retry(
        200,
        50,
        ?assertMatch(
            {200, #{<<"count">> := 0}},
            count_clients(Ns)
        )
    ),
    ?assertMatch(
        {200, []},
        list_clients(Ns, #{})
    ),
    ?assertMatch(
        {200, [Ns]},
        list_nss(#{})
    ),
    ?assertMatch(
        {200, [Ns]},
        list_nss(#{<<"limit">> => <<"2">>})
    ),
    ?assertMatch(
        {200, []},
        list_nss(#{<<"last_ns">> => Ns, <<"limit">> => <<"1">>})
    ),
    ok.

%% Smoke CRUD operations test for tenant limiter.
t_tenant_limiter({init, Config}) ->
    Config;
t_tenant_limiter({'end', _Config}) ->
    ok;
t_tenant_limiter(_Config) ->
    Ns1 = <<"tns">>,
    Params1 = tenant_limiter_params(),

    ?assertMatch({404, _}, get_tenant_limiter(Ns1)),
    ?assertMatch({404, _}, update_tenant_limiter(Ns1, Params1)),
    ?assertMatch({204, _}, delete_tenant_limiter(Ns1)),

    ?assertMatch(
        {201, #{
            <<"bytes">> := #{<<"rate">> := <<"10mb/10s">>, <<"burst">> := <<"200mb/1m">>},
            <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"40/1m">>}
        }},
        create_tenant_limiter(Ns1, Params1)
    ),
    ?assertMatch({400, _}, create_tenant_limiter(Ns1, Params1)),
    ?assertMatch(
        {200, #{
            <<"bytes">> := #{<<"rate">> := <<"10mb/10s">>, <<"burst">> := <<"200mb/1m">>},
            <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"40/1m">>}
        }},
        get_tenant_limiter(Ns1)
    ),
    Params2 = tenant_limiter_params(#{
        <<"bytes">> => #{
            <<"rate">> => <<"infinity">>,
            <<"burst">> => <<"0/1d">>
        },
        <<"messages">> => #{
            <<"burst">> => <<"60/60s">>
        }
    }),
    ?assertMatch(
        {200, #{
            <<"bytes">> := #{<<"rate">> := <<"infinity">>, <<"burst">> := <<"0/1d">>},
            <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"60/1m">>}
        }},
        update_tenant_limiter(Ns1, Params2)
    ),
    ?assertMatch(
        {200, #{
            <<"bytes">> := #{<<"rate">> := <<"infinity">>, <<"burst">> := <<"0/1d">>},
            <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"60/1m">>}
        }},
        get_tenant_limiter(Ns1)
    ),

    ?assertMatch({204, _}, delete_tenant_limiter(Ns1)),
    ?assertMatch({404, _}, get_tenant_limiter(Ns1)),
    ?assertMatch({404, _}, update_tenant_limiter(Ns1, Params1)),

    ok.

%% Smoke CRUD operations test for client limiter.
t_client_limiter({init, Config}) ->
    Config;
t_client_limiter({'end', _Config}) ->
    ok;
t_client_limiter(_Config) ->
    Ns1 = <<"tns">>,
    Params1 = client_limiter_params(),

    ?assertMatch({404, _}, get_client_limiter(Ns1)),
    ?assertMatch({404, _}, update_client_limiter(Ns1, Params1)),
    ?assertMatch({204, _}, delete_client_limiter(Ns1)),

    ?assertMatch(
        {201, #{
            <<"bytes">> := #{<<"rate">> := <<"10mb/10s">>, <<"burst">> := <<"200mb/1m">>},
            <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"40/1m">>}
        }},
        create_client_limiter(Ns1, Params1)
    ),
    ?assertMatch({400, _}, create_client_limiter(Ns1, Params1)),
    ?assertMatch(
        {200, #{
            <<"bytes">> := #{<<"rate">> := <<"10mb/10s">>, <<"burst">> := <<"200mb/1m">>},
            <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"40/1m">>}
        }},
        get_client_limiter(Ns1)
    ),
    Params2 = client_limiter_params(#{
        <<"bytes">> => #{
            <<"rate">> => <<"infinity">>,
            <<"burst">> => <<"0/1d">>
        },
        <<"messages">> => #{
            <<"burst">> => <<"60/60s">>
        }
    }),
    ?assertMatch(
        {200, #{
            <<"bytes">> := #{<<"rate">> := <<"infinity">>, <<"burst">> := <<"0/1d">>},
            <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"60/1m">>}
        }},
        update_client_limiter(Ns1, Params2)
    ),
    ?assertMatch(
        {200, #{
            <<"bytes">> := #{<<"rate">> := <<"infinity">>, <<"burst">> := <<"0/1d">>},
            <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"60/1m">>}
        }},
        get_client_limiter(Ns1)
    ),

    ?assertMatch({204, _}, delete_client_limiter(Ns1)),
    ?assertMatch({404, _}, get_client_limiter(Ns1)),
    ?assertMatch({404, _}, update_client_limiter(Ns1, Params1)),

    ok.
