%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_plugin_api_endpoint_fake_api).

-export([handle/4]).

handle(get, [<<"ping">>], _Request, _Context) ->
    {ok, 200, #{}, #{ok => true}};
handle(get, [<<"crash">>], _Request, _Context) ->
    error(fake_crash);
handle(get, [<<"timeout">>], _Request, _Context) ->
    timer:sleep(6000),
    {ok, 200, #{}, #{ok => true}};
handle(_, _Path, _Request, _Context) ->
    {error, bad_request, <<"Unsupported operation">>}.
