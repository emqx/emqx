%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_dashboard_middleware).

-behaviour(cowboy_middleware).

-export([execute/2]).

execute(Req, Env) ->
    case check_dispatch_ready(Env) of
        true -> add_cors_flag(Req, Env);
        false -> {stop, cowboy_req:reply(503, Req)}
    end.

add_cors_flag(Req, Env) ->
    CORS = emqx_conf:get([dashboard, cors], false),
    Origin = cowboy_req:header(<<"origin">>, Req, undefined),
    case CORS andalso Origin =/= undefined of
        false ->
            {ok, Req, Env};
        true ->
            Req2 = cowboy_req:set_resp_header(<<"Access-Control-Allow-Origin">>, <<"*">>, Req),
            {ok, Req2, Env}
    end.

check_dispatch_ready(Env) ->
    case maps:is_key(options, Env) of
        false ->
            true;
        true ->
            %% dashboard should always ready, if not, is_ready/1 will block until ready.
            emqx_dashboard_listener:is_ready(timer:seconds(15))
    end.
