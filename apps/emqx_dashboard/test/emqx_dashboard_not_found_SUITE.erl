%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_not_found_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/http_api.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SERVER, "http://127.0.0.1:18083/").

-import(emqx_mgmt_api_test_util, [request/2]).

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

t_bad_api_path(_) ->
    Url = ?SERVER ++ "/for/test/some/path/not/exist",
    {ok, 404, _} = request(get, Url),
    ok.
