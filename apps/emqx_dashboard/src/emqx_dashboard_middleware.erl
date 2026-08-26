%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_middleware).

-behaviour(cowboy_middleware).

-export([execute/2]).

-define(SECURITY_HEADERS, #{
    <<"x-content-type-options">> => <<"nosniff">>,
    <<"x-frame-options">> => <<"SAMEORIGIN">>,
    <<"referrer-policy">> => <<"no-referrer">>,
    <<"x-permitted-cross-domain-policies">> => <<"none">>,
    <<"x-download-options">> => <<"noopen">>
}).

execute(Req, Env) ->
    Req2 = cowboy_req:set_resp_headers(?SECURITY_HEADERS, Req),
    add_cors_flag(Req2, Env).

add_cors_flag(Req, Env) ->
    CORS = emqx_conf:get([dashboard, cors], false),
    case CORS andalso cowboy_req:header(<<"origin">>, Req, undefined) =/= undefined of
        false ->
            {ok, Req, Env};
        true ->
            Req2 = cowboy_req:set_resp_header(<<"Access-Control-Allow-Origin">>, <<"*">>, Req),
            {ok, Req2, Env}
    end.
