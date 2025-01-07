%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% end-to-end integration test
-module(emqx_authn_cinfo_int_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").

-define(AUTHN_ID, <<"mechanism:cinfo">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_auth,
            %% to load schema
            {emqx_auth_cinfo, #{start => false}},
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{
            work_dir => filename:join(?config(priv_dir, Config), ?MODULE)
        }
    ),
    _ = emqx_common_test_http:create_default_app(),
    ok = emqx_authn_chains:register_providers([{cinfo, emqx_authn_cinfo}]),
    ?AUTHN:delete_chain(?GLOBAL),
    {ok, Chains} = ?AUTHN:list_chains(),
    ?assertEqual(length(Chains), 0),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [?CONF_NS_ATOM],
        ?GLOBAL
    ),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create_ok(_Config) ->
    {ok, Config} = hocon:binary(config(?FUNCTION_NAME)),
    {ok, 200, _} = request(post, uri([?CONF_NS]), Config),
    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {username, <<"magic1">>},
        {password, <<"ignore">>}
    ]),
    unlink(Client1),
    {ok, Client2} = emqtt:start_link([
        {proto_ver, v5},
        {username, <<"magic2">>},
        {password, <<"ignore">>}
    ]),
    unlink(Client2),
    {ok, Client3} = emqtt:start_link([
        {proto_ver, v5},
        {username, <<"magic3">>},
        {password, <<"ignore">>}
    ]),
    unlink(Client3),
    ?assertMatch({ok, _}, emqtt:connect(Client1)),
    ok = emqtt:disconnect(Client1),
    ?assertMatch({error, {bad_username_or_password, #{}}}, emqtt:connect(Client2)),
    ?assertMatch({error, {not_authorized, #{}}}, emqtt:connect(Client3)),
    ok.

t_empty_checks_is_not_allowed(_Config) ->
    {ok, Config} = hocon:binary(config(?FUNCTION_NAME)),
    ?assertMatch(
        {ok, 400, _},
        request(post, uri([?CONF_NS]), Config)
    ),
    ok.

t_empty_is_match_not_allowed(_Config) ->
    {ok, Config} = hocon:binary(config(?FUNCTION_NAME)),
    ?assertMatch(
        {ok, 400, _},
        request(post, uri([?CONF_NS]), Config)
    ),
    ok.

t_expression_compile_error(_Config) ->
    {ok, Config} = hocon:binary(config(?FUNCTION_NAME)),
    ?assertMatch(
        {ok, 400, _},
        request(post, uri([?CONF_NS]), Config)
    ),
    ok.

%% erlfmt-ignore
config(t_create_ok) ->
    "{
        mechanism = cinfo,
        checks = [
          {
            is_match = \"str_eq(username,'magic1')\"
            result = allow
          },
          {
            is_match = \"str_eq(username, 'magic2')\"
            result = deny
          }
        ]
    }";
config(t_empty_checks_is_not_allowed) ->
    "{
        mechanism = cinfo,
        checks = []
    }";
config(t_empty_is_match_not_allowed) ->
    "{
        mechanism = cinfo,
        checks = [
          {
            is_match = []
            result = allow
          }
        ]
    }";
config(t_expression_compile_error) ->
    "{
        mechanism = cinfo,
        checks = [
          {
            is_match = \"1\"
            result = allow
          }
        ]
    }".

request(Method, Url, Body) ->
    emqx_mgmt_api_test_util:request(Method, Url, Body).

uri(Path) ->
    emqx_mgmt_api_test_util:uri(Path).
