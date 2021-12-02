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

-module(emqx_prometheus_api).

-behaviour(minirest_api).

-include("emqx_prometheus.hrl").

-import(emqx_mgmt_util, [ schema/1]).

-export([api_spec/0]).

-export([ prometheus/2
        , stats/2
        ]).

api_spec() ->
    {[prometheus_api(), prometheus_data_api()], []}.

conf_schema() ->
    emqx_mgmt_api_configs:gen_schema(emqx:get_raw_config([prometheus])).

prometheus_api() ->
    Metadata = #{
        get => #{
            description => <<"Get Prometheus info">>,
            responses => #{<<"200">> => schema(conf_schema())}
        },
        put => #{
            description => <<"Update Prometheus">>,
            'requestBody' => schema(conf_schema()),
            responses => #{<<"200">> => schema(conf_schema())}
        }
    },
    {"/prometheus", Metadata, prometheus}.

prometheus_data_api() ->
    Metadata = #{
        get => #{
            description => <<"Get Prometheus Data">>,
            responses => #{<<"200">> =>
                #{content =>
                #{
                    'application/json' => #{schema => #{type => object}},
                    'text/plain' => #{schema => #{type => string}}
                }}
            }
        }
    },
    {"/prometheus/stats", Metadata, stats}.

prometheus(get, _Params) ->
    {200, emqx:get_raw_config([<<"prometheus">>], #{})};

prometheus(put, #{body := Body}) ->
    {ok, Config} = emqx:update_config([prometheus], Body),
    case maps:get(<<"enable">>, Body) of
        true ->
            _ = emqx_prometheus_sup:stop_child(?APP),
            emqx_prometheus_sup:start_child(?APP, maps:get(config, Config));
        false ->
            _ = emqx_prometheus_sup:stop_child(?APP),
            ok
        end,
    {200, emqx:get_raw_config([<<"prometheus">>], #{})}.

stats(get, #{headers := Headers}) ->
    Type =
        case maps:get(<<"accept">>, Headers, <<"text/plain">>) of
            <<"application/json">> -> <<"json">>;
            _ -> <<"prometheus">>
        end,
    Data = emqx_prometheus:collect(Type),
    case Type of
        <<"json">> -> {200, Data};
        <<"prometheus">> -> {200, #{<<"content-type">> => <<"text/plain">>}, Data}
    end.
