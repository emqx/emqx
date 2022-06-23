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

-module(emqx_prometheus_api).

-behaviour(minirest_api).

-include("emqx_prometheus.hrl").

-import(hoconsc, [ref/2]).

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    prometheus/2,
    stats/2
]).

-define(SCHEMA_MODULE, emqx_prometheus_schema).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/prometheus",
        "/prometheus/stats"
    ].

schema("/prometheus") ->
    #{
        'operationId' => prometheus,
        get =>
            #{
                description => <<"Get Prometheus config info">>,
                responses =>
                    #{200 => prometheus_config_schema()}
            },
        put =>
            #{
                description => <<"Update Prometheus config">>,
                'requestBody' => prometheus_config_schema(),
                responses =>
                    #{200 => prometheus_config_schema()}
            }
    };
schema("/prometheus/stats") ->
    #{
        'operationId' => stats,
        get =>
            #{
                description => <<"Get Prometheus Data">>,
                security => [],
                responses =>
                    #{200 => prometheus_data_schema()}
            }
    }.

%%--------------------------------------------------------------------
%% API Handler funcs
%%--------------------------------------------------------------------

prometheus(get, _Params) ->
    {200, emqx:get_raw_config([<<"prometheus">>], #{})};
prometheus(put, #{body := Body}) ->
    case emqx_prometheus:update(Body) of
        {ok, NewConfig} ->
            {200, NewConfig};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Update config failed ~p", [Reason])),
            {500, 'INTERNAL_ERROR', Message}
    end.

stats(get, #{headers := Headers}) ->
    Type =
        case maps:get(<<"accept">>, Headers, <<"text/plain">>) of
            <<"application/json">> -> <<"json">>;
            _ -> <<"prometheus">>
        end,
    Data = emqx_prometheus:collect(Type),
    case Type of
        <<"json">> ->
            {200, Data};
        <<"prometheus">> ->
            {200, #{<<"content-type">> => <<"text/plain">>}, Data}
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

prometheus_config_schema() ->
    emqx_dashboard_swagger:schema_with_example(
        ref(?SCHEMA_MODULE, "prometheus"),
        prometheus_config_example()
    ).

prometheus_config_example() ->
    #{
        enable => true,
        interval => "15s",
        push_gateway_server => <<"http://127.0.0.1:9091">>
    }.

prometheus_data_schema() ->
    #{
        description => <<"Get Prometheus Data">>,
        content =>
            #{
                'application/json' =>
                    #{schema => #{type => object}},
                'text/plain' =>
                    #{schema => #{type => string}}
            }
    }.
