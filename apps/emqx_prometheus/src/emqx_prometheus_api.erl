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

-module(emqx_prometheus_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    setting/2,
    stats/2,
    auth/2
]).

-define(TAGS, [<<"Monitor">>]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/prometheus",
        "/prometheus/auth",
        "/prometheus/stats"
    ].

schema("/prometheus") ->
    #{
        'operationId' => setting,
        get =>
            #{
                description => ?DESC(get_prom_conf_info),
                tags => ?TAGS,
                responses =>
                    #{200 => prometheus_setting_response()}
            },
        put =>
            #{
                description => ?DESC(update_prom_conf_info),
                tags => ?TAGS,
                'requestBody' => prometheus_setting_request(),
                responses =>
                    #{200 => prometheus_setting_response()}
            }
    };
schema("/prometheus/auth") ->
    #{
        'operationId' => auth,
        get =>
            #{
                description => ?DESC(get_prom_auth_data),
                tags => ?TAGS,
                security => security(),
                responses =>
                    #{200 => prometheus_data_schema()}
            }
    };
schema("/prometheus/stats") ->
    #{
        'operationId' => stats,
        get =>
            #{
                description => ?DESC(get_prom_data),
                tags => ?TAGS,
                security => security(),
                responses =>
                    #{200 => prometheus_data_schema()}
            }
    }.

security() ->
    case emqx_config:get([prometheus, enable_basic_auth], false) of
        true -> [#{'basicAuth' => []}, #{'bearerAuth' => []}];
        false -> []
    end.
%%--------------------------------------------------------------------
%% API Handler funcs
%%--------------------------------------------------------------------

setting(get, _Params) ->
    Raw = emqx:get_raw_config([<<"prometheus">>], #{}),
    Conf =
        case emqx_prometheus_schema:is_recommend_type(Raw) of
            true -> Raw;
            false -> emqx_prometheus_config:to_recommend_type(Raw)
        end,
    {200, Conf};
setting(put, #{body := Body}) ->
    case emqx_prometheus_config:update(Body) of
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

auth(get, #{headers := Headers}) ->
    Type =
        case maps:get(<<"accept">>, Headers, <<"text/plain">>) of
            <<"application/json">> -> <<"json">>;
            _ -> <<"prometheus">>
        end,
    Data = emqx_prometheus_auth:collect(Type),
    case Type of
        <<"json">> ->
            {200, Data};
        <<"prometheus">> ->
            {200, #{<<"content-type">> => <<"text/plain">>}, Data}
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

prometheus_setting_request() ->
    [{prometheus, #{type := Setting}}] = emqx_prometheus_schema:roots(),
    emqx_dashboard_swagger:schema_with_examples(
        Setting,
        [
            recommend_setting_example(),
            legacy_setting_example()
        ]
    ).

%% Always return recommend setting
prometheus_setting_response() ->
    {_, #{value := Example}} = recommend_setting_example(),
    emqx_dashboard_swagger:schema_with_example(
        ?R_REF(emqx_prometheus_schema, recommend_setting),
        Example
    ).

legacy_setting_example() ->
    Summary = <<"legacy_deprecated_setting">>,
    {Summary, #{
        summary => Summary,
        value => #{
            enable => true,
            interval => <<"15s">>,
            push_gateway_server => <<"http://127.0.0.1:9091">>,
            headers => #{<<"Authorization">> => <<"Basic YWRtaW46Y2JraG55eWd5QDE=">>},
            job_name => <<"${name}/instance/${name}~${host}">>,
            vm_dist_collector => <<"disabled">>,
            vm_memory_collector => <<"disabled">>,
            vm_msacc_collector => <<"disabled">>,
            mnesia_collector => <<"disabled">>,
            vm_statistics_collector => <<"disabled">>,
            vm_system_info_collector => <<"disabled">>
        }
    }}.

recommend_setting_example() ->
    Summary = <<"recommend_setting">>,
    {Summary, #{
        summary => Summary,
        value => #{
            enable_basic_auth => false,
            push_gateway => #{
                interval => <<"15s">>,
                url => <<"http://127.0.0.1:9091">>,
                headers => #{<<"Authorization">> => <<"Basic YWRtaW46Y2JraG55eWd5QDE=">>},
                job_name => <<"${name}/instance/${name}~${host}">>
            },
            collectors => #{
                vm_dist => <<"disabled">>,
                vm_memory => <<"disabled">>,
                vm_msacc => <<"disabled">>,
                mnesia => <<"disabled">>,
                vm_statistics => <<"disabled">>,
                vm_system_info => <<"disabled">>
            }
        }
    }}.

prometheus_data_schema() ->
    #{
        description =>
            <<"Get Prometheus Data.">>,
        content =>
            [
                {'text/plain', #{schema => #{type => string}}},
                {'application/json', #{schema => #{type => object}}}
            ]
    }.
