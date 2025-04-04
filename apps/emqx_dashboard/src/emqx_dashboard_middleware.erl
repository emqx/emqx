%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_middleware).

-behaviour(cowboy_middleware).

-export([execute/2]).

execute(Req, Env) ->
    add_cors_flag(Req, Env).

add_cors_flag(Req, Env) ->
    CORS = emqx_conf:get([dashboard, cors], false),
    case CORS andalso cowboy_req:header(<<"origin">>, Req, undefined) =/= undefined of
        false ->
            {ok, Req, Env};
        true ->
            Req2 = cowboy_req:set_resp_header(<<"Access-Control-Allow-Origin">>, <<"*">>, Req),
            {ok, Req2, Env}
    end.
