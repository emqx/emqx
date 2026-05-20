%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_schema_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_utils/include/emqx_http_api.hrl").

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
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

t_hotconf(_) ->
    assert_schema_response("hotconf").

t_actions(_) ->
    assert_schema_response("actions").

t_connectors(_) ->
    assert_schema_response("connectors").

assert_schema_response(Name) ->
    Url = ?SERVER ++ "/schemas/" ++ Name,
    {ok, {{_, 200, _}, Headers, Body}} = request_with_headers(get, Url),
    %% assert it's a valid json
    _ = emqx_utils_json:decode(Body),
    %% assert minirest reports JSON content-type, not the text/plain fallback
    ContentType = proplists:get_value("content-type", Headers),
    ?assertMatch(<<"application/json">>, iolist_to_binary(ContentType)),
    ok.

request_with_headers(Method, Url) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true, httpc_req_opts => [{body_format, binary}]},
    emqx_mgmt_api_test_util:request_api(Method, Url, [], AuthHeader, [], Opts).
