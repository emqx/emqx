%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_api_endpoint_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SERVER, "http://127.0.0.1:18083/api/v5").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TC, Config) ->
    ok = meck:new(emqx_plugins, [non_strict, passthrough, no_link]),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = meck:unload(emqx_plugins).

t_plugin_api_ok(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(<<"fake">>, _Request, _Timeout) -> {200, #{ok => true}} end
    ),
    {200, Body} = request(get, ?SERVER ++ "/plugin_api/fake/ping"),
    ?assertEqual(#{<<"ok">> => true}, Body).

t_plugin_api_not_found(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(_, _Request, _Timeout) ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Plugin API Not Found">>}}
        end
    ),
    {404, Body} = request(get, ?SERVER ++ "/plugin_api/nope/ping"),
    ?assertMatch(
        #{<<"code">> := <<"NOT_FOUND">>},
        Body
    ).

t_plugin_api_unauthorized(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(_, _Request, _Timeout) -> {200, #{ok => true}} end
    ),
    {401, _Body} = request(get, ?SERVER ++ "/plugin_api/fake/ping", [{"x-test", "1"}]).

t_plugin_api_callback_crash(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(_, _Request, _Timeout) -> {500, #{code => <<"INTERNAL_ERROR">>}} end
    ),
    {500, Body} = request(get, ?SERVER ++ "/plugin_api/fake/ping"),
    ?assertMatch(
        #{<<"code">> := <<"INTERNAL_ERROR">>},
        Body
    ).

t_plugin_api_path_remainder_is_percent_decoded(_Config) ->
    ok = meck:expect(
        emqx_plugins,
        handle_api_call,
        fun(<<"fake">>, #{path := [<<"user/name">>]}, _Timeout) -> {200, #{ok => true}} end
    ),
    {200, Body} = request(get, ?SERVER ++ "/plugin_api/fake/user%2Fname"),
    ?assertEqual(#{<<"ok">> => true}, Body).

request(Method, Url) ->
    request(Method, Url, emqx_mgmt_api_test_util:auth_header_()).

request(Method, Url, AuthOrHeaders) ->
    Res = emqx_mgmt_api_test_util:request_api(
        Method,
        Url,
        [],
        AuthOrHeaders,
        [],
        #{return_all => true, httpc_req_opts => [{body_format, binary}]}
    ),
    case Res of
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Body}} ->
            {Code, maybe_decode(Body)};
        {error, {{"HTTP/1.1", Code, _}, _Headers, Body}} ->
            {Code, maybe_decode(Body)}
    end.

maybe_decode(Body) when is_binary(Body) ->
    case emqx_utils_json:safe_decode(Body) of
        {ok, Decoded} -> Decoded;
        {error, _} -> Body
    end;
maybe_decode(Body) ->
    Body.
