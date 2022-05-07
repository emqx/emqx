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
    waiting_dispatch_ready(),
    CORS = emqx_conf:get([dashboard, cors], false),
    case CORS andalso cowboy_req:header(<<"origin">>, Req, undefined) of
        false ->
            {ok, Req, Env};
        undefined ->
            {ok, Req, Env};
        _ ->
            Req2 = cowboy_req:set_resp_header(<<"Access-Control-Allow-Origin">>, <<"*">>, Req),
            {ok, Req2, Env}
    end.

waiting_dispatch_ready() ->
    waiting_dispatch_ready(5).

waiting_dispatch_ready(0) ->
    ok;
waiting_dispatch_ready(Count) ->
    case emqx_sys:uptime() < timer:minutes(1) of
        true ->
            case emqx_dashboard_listener:is_ready() of
                true ->
                    ok;
                false ->
                    timer:sleep(100),
                    waiting_dispatch_ready(Count - 1)
            end;
        false ->
            ok
    end.
