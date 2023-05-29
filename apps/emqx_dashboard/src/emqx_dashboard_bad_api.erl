%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_bad_api).

-include_lib("emqx/include/logger.hrl").

-export([init/2]).

init(Req0, State) ->
    RedactedReq = emqx_utils:redact(Req0),
    ?SLOG(warning, #{msg => "unexpected_api_access", request => RedactedReq}),
    Req = cowboy_req:reply(
        404,
        #{<<"content-type">> => <<"application/json">>},
        <<"{\"code\": \"API_NOT_EXIST\", \"message\": \"Request Path Not Found\"}">>,
        RedactedReq
    ),
    {ok, Req, State}.
