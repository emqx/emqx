%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_otel_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/http_api.hrl").

-import(hoconsc, [ref/2]).

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([config/2]).

-define(TAGS, [<<"Monitor">>]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/opentelemetry"
    ].

schema("/opentelemetry") ->
    #{
        'operationId' => config,
        get =>
            #{
                description => "Get opentelmetry configuration",
                tags => ?TAGS,
                responses =>
                    #{200 => otel_config_schema()}
            },
        put =>
            #{
                description => "Update opentelmetry configuration",
                tags => ?TAGS,
                'requestBody' => otel_config_schema(),
                responses =>
                    #{
                        200 => otel_config_schema(),
                        400 =>
                            emqx_dashboard_swagger:error_codes(
                                [?BAD_REQUEST], <<"Update Config Failed">>
                            )
                    }
            }
    }.

%%--------------------------------------------------------------------
%% API Handler funcs
%%--------------------------------------------------------------------

config(get, _Params) ->
    {200, get_raw()};
config(put, #{body := Body}) ->
    case emqx_otel_config:update(Body) of
        {ok, NewConfig} ->
            {200, NewConfig};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Update config failed ~p", [Reason])),
            {400, ?BAD_REQUEST, Message}
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

get_raw() ->
    Path = <<"opentelemetry">>,
    #{Path := Conf} =
        emqx_config:fill_defaults(
            #{Path => emqx_conf:get_raw([Path])},
            #{obfuscate_sensitive_values => true}
        ),
    Conf.

otel_config_schema() ->
    emqx_dashboard_swagger:schema_with_example(
        ref(emqx_otel_schema, "opentelemetry"),
        otel_config_example()
    ).

otel_config_example() ->
    #{
        exporter => #{
            endpoint => "http://localhost:4317",
            ssl_options => #{}
        },
        logs => #{
            enable => true,
            level => warning
        },
        metrics => #{
            enable => true
        },
        traces => #{
            enable => true,
            filter => #{
                trace_all => false,
                trace_mode => legacy,
                e2e_tracing_options => #{
                    attribute_meta_value => "emqxcl",
                    msg_trace_level => 0,
                    clientid_match_rules_max => 30,
                    topic_match_rules_max => 30,
                    sample_ratio => "10%",
                    client_connect_disconnect => true,
                    client_subscribe_unsubscribe => true,
                    client_publish => true
                }
            }
        }
    }.
