%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_rewrite_api).

-behaviour(minirest_api).
-include_lib("typerefl/include/types.hrl").

-export([api_spec/0, paths/0, schema/1]).

-export([topic_rewrite/2]).

-define(MAX_RULES_LIMIT, 20).

-define(EXCEED_LIMIT, 'EXCEED_LIMIT').

-import(emqx_mgmt_util, [ object_array_schema/1
                        , object_array_schema/2
                        , error_schema/2
                        , properties/1
                        ]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/mqtt/topic_rewrite"].

schema("/mqtt/topic_rewrite") ->
    #{
        operationId => topic_rewrite,
        get => #{
            tags => [mqtt],
            description => <<"List rewrite topic.">>,
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(emqx_modules_schema, "rewrite")),
                    #{desc => <<"List all rewrite rules">>})
            }
        },
        put => #{
            description => <<"Update rewrite topic">>,
            requestBody => hoconsc:mk(hoconsc:array(hoconsc:ref(emqx_modules_schema, "rewrite")),#{}),
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(emqx_modules_schema, "rewrite")),
                    #{desc => <<"Update rewrite topic success.">>}),
                413 => emqx_dashboard_swagger:error_codes([?EXCEED_LIMIT], <<"Rules count exceed max limit">>)
            }
        }
    }.

topic_rewrite(get, _Params) ->
    {200, emqx_rewrite:list()};

topic_rewrite(put, #{body := Body}) ->
    case length(Body) < ?MAX_RULES_LIMIT of
        true ->
            ok = emqx_rewrite:update(Body),
            {200, emqx_rewrite:list()};
        _ ->
            Message = iolist_to_binary(io_lib:format("Max rewrite rules count is ~p", [?MAX_RULES_LIMIT])),
            {413, #{code => ?EXCEED_LIMIT, message => Message}}
    end.
