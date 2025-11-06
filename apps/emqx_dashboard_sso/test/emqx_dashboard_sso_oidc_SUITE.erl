%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dashboard_sso_oidc_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(ON_ALL(NODES, BODY), erpc:multicall(NODES, fun() -> BODY end)).

-define(AUTH_HEADER_FN_PD_KEY, {?MODULE, auth_header_fn}).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

start_apps(TestCase, TCConfig) ->
    AppSpecs = [
        emqx_conf,
        emqx_management,
        emqx_mgmt_api_test_util:emqx_dashboard(),
        {emqx_dashboard_sso, #{
            after_start => fun() ->
                #{started := Started} = emqx_dashboard:listeners_status(),
                ok = emqx_dashboard_dispatch:regenerate_dispatch(Started),
                ok
            end
        }}
    ],
    Apps = emqx_cth_suite:start(
        AppSpecs,
        #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}
    ),
    on_exit(fun() -> ok = emqx_cth_suite:stop(Apps) end),
    ok.

mk_cluster(TestCase, #{n := NumNodes} = _Opts, TCConfig) ->
    AppSpecs = [
        emqx_conf,
        emqx_management
    ],
    MkDashApp = fun(N) ->
        Port = 18083 + N - 1,
        PortStr = integer_to_list(Port),
        [
            emqx_mgmt_api_test_util:emqx_dashboard(
                "dashboard.listeners.http.bind = " ++ PortStr
            ),
            {emqx_dashboard_sso, #{
                after_start => fun() ->
                    #{started := Started} = emqx_dashboard:listeners_status(),
                    ok = emqx_dashboard_dispatch:regenerate_dispatch(Started),
                    ok
                end
            }}
        ]
    end,
    NodeSpecs0 = lists:map(
        fun(N) ->
            Name = mk_node_name(TestCase, N),
            {Name, #{apps => AppSpecs ++ MkDashApp(N)}}
        end,
        lists:seq(1, NumNodes)
    ),
    Nodes = emqx_cth_cluster:start(
        NodeSpecs0,
        #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}
    ),
    on_exit(fun() -> ok = emqx_cth_cluster:stop(Nodes) end),
    Nodes.

mk_node_name(TestCase, N) ->
    Name0 = iolist_to_binary([atom_to_binary(TestCase), "_", integer_to_binary(N)]),
    binary_to_atom(Name0).

simple_request(Params) ->
    emqx_mgmt_api_test_util:simple_request(Params).

auth_header() ->
    case get_auth_header_getter() of
        Fun when is_function(Fun, 0) ->
            Fun();
        _ ->
            emqx_mgmt_api_test_util:auth_header_()
    end.

get_auth_header_getter() ->
    get(?AUTH_HEADER_FN_PD_KEY).

%% Note: must be set in init_per_testcase, as this is stored in process dictionary.
set_auth_header_getter(Fun) ->
    _ = put(?AUTH_HEADER_FN_PD_KEY, Fun),
    ok.

clear_auth_header_getter() ->
    _ = erase(?AUTH_HEADER_FN_PD_KEY),
    ok.

get_http_dashboard_port(Node) ->
    ?ON(Node, emqx_config:get([dashboard, listeners, http, bind])).

host(Port) ->
    "http://127.0.0.1:" ++ integer_to_list(Port).

url(Node, Parts) ->
    Port = get_http_dashboard_port(Node),
    Host = host(Port),
    emqx_mgmt_api_test_util:api_path(Host, Parts).

get_ssos(Node, Opts) ->
    URL = url(Node, ["sso"]),
    simple_request(#{
        method => get,
        url => URL,
        auth_header => auth_header_lazy(Opts)
    }).

%% Create or update, actually...
create_backend(Node, Params, Opts) ->
    URL = url(Node, ["sso", "oidc"]),
    simple_request(#{
        method => put,
        url => URL,
        body => Params,
        auth_header => auth_header_lazy(Opts)
    }).

delete_backend(Node, Opts) ->
    URL = url(Node, ["sso", "oidc"]),
    simple_request(#{
        method => delete,
        url => URL,
        auth_header => auth_header_lazy(Opts)
    }).

login_sso(Node, Opts) ->
    URL = url(Node, ["sso", "login", "oidc"]),
    simple_request(#{
        return_headers => true,
        http_opts => [{autoredirect, false}],
        method => post,
        url => URL,
        body => #{<<"backend">> => <<"oidc">>},
        auth_header => auth_header_lazy(Opts)
    }).

oidc_mock_server_auth_req(QueryString) ->
    URL = "https://authn-server:5556/dex/auth/mock",
    simple_request(#{
        return_headers => true,
        http_opts => [{autoredirect, false}, {ssl, [{verify, verify_none}]}],
        method => get,
        url => URL,
        query_params => QueryString,
        auth_header => {"x", "x"}
    }).

oidc_callback_req(QueryString) ->
    URL = "https://authn-server:5556/dex/callback",
    simple_request(#{
        return_headers => true,
        http_opts => [{autoredirect, false}],
        method => get,
        url => URL,
        query_params => QueryString,
        auth_header => {"x", "x"}
    }).

oidc_approve_req(QueryString) ->
    URL = "https://authn-server:5556/dex/approval",
    #{"req" := Req} = maps:from_list(uri_string:dissect_query(QueryString)),
    Body =
        {raw,
            iolist_to_binary(
                uri_string:compose_query([{"req", Req}, {"approval", "approve"}])
            )},
    simple_request(#{
        return_headers => true,
        http_opts => [{autoredirect, false}],
        'content-type' => "application/x-www-form-urlencoded",
        method => post,
        url => URL,
        body => Body,
        query_params => QueryString,
        auth_header => [{"x", "x"}]
    }).

simple_login_get(URL) ->
    simple_request(#{
        return_headers => true,
        http_opts => [{autoredirect, false}],
        method => get,
        url => URL,
        auth_header => [{"x", "x"}]
    }).

get_nodes(Node, Opts) ->
    URL = url(Node, ["nodes"]),
    simple_request(#{
        method => get,
        url => URL,
        auth_header => auth_header_lazy(Opts)
    }).

auth_header_lazy(TCConfig) when is_list(TCConfig) ->
    auth_header_lazy(maps:from_list(TCConfig));
auth_header_lazy(#{} = Opts) ->
    emqx_utils_maps:get_lazy(
        auth_header,
        Opts,
        fun auth_header/0
    ).

oidc_provider_params() ->
    #{
        <<"enable">> => true,
        <<"backend">> => <<"oidc">>,

        %% See `.ci/docker-compose-file/dex/config.dev.yaml`
        <<"clientid">> => <<"example-app">>,
        <<"issuer">> => <<"https://authn-server:5556/dex">>,
        <<"secret">> => <<"ZXhhbXBsZS1hcHAtc2VjcmV0">>,

        <<"client_jwks">> => <<"none">>,
        <<"dashboard_addr">> => <<"http://emqx_lb:18083">>,
        <<"fallback_methods">> => [<<"RS256">>],
        <<"name_var">> => <<"${sub}">>,
        <<"preferred_auth_methods">> => [
            <<"client_secret_post">>,
            <<"client_secret_basic">>,
            <<"none">>
        ],
        <<"provider">> => <<"generic">>,
        <<"require_pkce">> => false,
        <<"scopes">> => [<<"openid">>],
        <<"session_expiry">> => 30,

        <<"ssl">> => #{
            <<"enable">> => true,
            <<"cacertfile">> => <<"${EMQX_ETC_DIR}/certs/cacert.pem">>
        }
    }.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Smoke test for performing OIDC login with a single-node cluster.
t_smoke_single_node(TCConfig) ->
    Opts = #{n => 1, repeat => 2},
    do_smoke_tests(?FUNCTION_NAME, Opts, TCConfig).

%% Smoke test for performing OIDC login with a three-node cluster.
t_smoke_three_nodes(TCConfig) ->
    Opts = #{n => 3, repeat => 5},
    do_smoke_tests(?FUNCTION_NAME, Opts, TCConfig).

do_smoke_tests(TestCase, Opts, TCConfig) ->
    #{n := NumNodes} = Opts,
    Repeat = maps:get(repeat, Opts, 1),
    case mk_cluster(TestCase, #{n => NumNodes}, TCConfig) of
        [Node] ->
            LoginNode = Node,
            FinalReqNode = Node;
        [Node, LoginNode] ->
            FinalReqNode = Node;
        [Node, LoginNode, FinalReqNode] ->
            ok
    end,
    AuthHeader = ?ON(Node, emqx_mgmt_api_test_util:auth_header_()),
    set_auth_header_getter(fun() -> AuthHeader end),
    lists:foreach(
        fun(_) ->
            do_smoke_tests1(Node, LoginNode, FinalReqNode, TCConfig)
        end,
        lists:seq(1, Repeat)
    ).

do_smoke_tests1(Node, LoginNode, FinalReqNode, _TCConfig) ->
    %% Create the provider
    ProviderParams = oidc_provider_params(),
    ?assertMatch({200, _}, create_backend(Node, ProviderParams, #{})),
    ?assertMatch(
        {200, [
            #{
                <<"backend">> := <<"oidc">>,
                <<"running">> := true
            }
        ]},
        get_ssos(Node, #{})
    ),

    %% Login
    {302, Headers1, Resp1} = login_sso(Node, #{}),
    ct:pal("returned headers1:\n  ~p\nbody:\n  ~p\n", [Headers1, Resp1]),
    {"location", OIDCURL1} = lists:keyfind("location", 1, Headers1),

    #{query := QueryParams1} = uri_string:parse(OIDCURL1),
    {302, Headers2, Resp2} = oidc_mock_server_auth_req(QueryParams1),
    ct:pal("returned headers2:\n  ~p\nbody:\n  ~p\n", [Headers2, Resp2]),
    {"location", OIDCURL2} = lists:keyfind("location", 1, Headers2),

    #{query := QueryParams2} = uri_string:parse(OIDCURL2),
    {303, Headers3, Resp3} = oidc_callback_req(QueryParams2),
    ct:pal("returned headers3:\n  ~p\nbody:\n  ~p\n", [Headers3, Resp3]),
    {"location", OIDCURL3} = lists:keyfind("location", 1, Headers3),

    #{query := QueryParams3} = uri_string:parse(OIDCURL3),
    {303, Headers4, Resp4} = oidc_approve_req(QueryParams3),
    ct:pal("returned headers4:\n  ~p\nbody:\n  ~p\n", [Headers4, Resp4]),
    {"location", LoginURL1} = lists:keyfind("location", 1, Headers4),

    %% Ensure callback falls onto login node
    CallbackURI = uri_string:parse(LoginURL1),
    LoginNodePort = get_http_dashboard_port(LoginNode),
    LoginURL1B = uri_string:recompose(CallbackURI#{
        host := "127.0.0.1",
        port := LoginNodePort
    }),
    {302, Headers5, Resp5} = ?retry(100, 10, begin
        Res5 = simple_login_get(LoginURL1B),
        ct:pal("callback response:\n  ~p", [Res5]),
        {302, _, _} = Res5
    end),
    ct:pal("returned headers5:\n  ~p\nbody:\n  ~p\n", [Headers5, Resp5]),
    {"location", LoginURL2} = lists:keyfind("location", 1, Headers5),

    #{query := QueryParams4} = uri_string:parse(LoginURL2),
    #{"login_meta" := Token0} = maps:from_list(uri_string:dissect_query(QueryParams4)),
    ct:pal("token0: ~s", [Token0]),
    #{<<"token">> := Token1} = emqx_utils_json:decode(base64:decode(Token0)),

    %% Finally, can now perform actions in the API
    FinalAuthHeader = {"Authorization", "Bearer " ++ binary_to_list(Token1)},
    ?assertMatch({200, _}, get_nodes(FinalReqNode, #{auth_header => FinalAuthHeader})),

    %% State must be deleted afterwards, so that it's not reusable, and does not leak
    %% resources.
    ?assertMatch({401, _, _}, simple_login_get(LoginURL1B)),
    ?assertEqual(
        lists:duplicate(3, {ok, []}),
        ?ON_ALL([Node, LoginNode, FinalReqNode], emqx_dashboard_sso_oidc_session:all())
    ),

    ok.

%% Checks that we actually stop workers when disabling oidc.
t_stop_cleanup(TCConfig) ->
    start_apps(?FUNCTION_NAME, TCConfig),
    N = node(),

    %% Initially, there should be no worker process under the supervisor.
    ?assertEqual([], supervisor:which_children(emqx_dashboard_sso_oidc_sup)),

    %% Create enabled; should start workers.
    ProviderParams = oidc_provider_params(),
    ?assertMatch({200, _}, create_backend(N, ProviderParams, #{})),

    ?assertMatch([_ | _], supervisor:which_children(emqx_dashboard_sso_oidc_sup)),

    %% Update to disabled; should stop workers.
    ?assertMatch({200, _}, create_backend(N, ProviderParams#{<<"enable">> => false}, #{})),
    ?assertEqual([], supervisor:which_children(emqx_dashboard_sso_oidc_sup)),

    %% Re-enable.
    ?assertMatch({200, _}, create_backend(N, ProviderParams, #{})),
    ?assertMatch([_ | _], supervisor:which_children(emqx_dashboard_sso_oidc_sup)),

    %% Delete; should stop workers.
    ?assertMatch({204, _}, delete_backend(N, #{})),
    ?assertEqual([], supervisor:which_children(emqx_dashboard_sso_oidc_sup)),

    ok.
