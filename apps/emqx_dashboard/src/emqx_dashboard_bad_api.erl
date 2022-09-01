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

-module(emqx_dashboard_bad_api).

-include_lib("emqx/include/logger.hrl").

-export([init/2]).
-define(HEADERS, #{<<"content-type">> => <<"application/json">>}).

init(Req0, State) ->
    ?SLOG(warning, #{msg => "unexpected_api_access", request => Req0}),
    Req = cowboy_req:reply(404, ?HEADERS, body(), Req0),
    {ok, Req, State}.

body() ->
    Body =
        case mria_rlog:role() of
            replicant ->
                #{
                    <<"code">> => <<"API_NOT_FOUND">>,
                    <<"message">> => <<"Request Path Not Found">>
                };
            core ->
                #{
                    <<"code">> => <<"API_DISABLE">>,
                    <<"message">> =>
                        <<"Request is disabled on replicant nodes, visit it from core nodes">>
                }
        end,
    emqx_json:encode(Body).
