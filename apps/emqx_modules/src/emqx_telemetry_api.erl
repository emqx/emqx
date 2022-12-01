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

-module(emqx_telemetry_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/1, ref/2, array/1]).

-export([
    status/2,
    data/2
]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1
]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/telemetry/status",
        "/telemetry/data"
    ].

schema("/telemetry/status") ->
    #{
        'operationId' => status,
        get =>
            #{
                description => ?DESC(get_telemetry_status_api),
                tags => [<<"Telemetry">>],
                responses =>
                    #{200 => status_schema(?DESC(get_telemetry_status_api))}
            },
        put =>
            #{
                description => ?DESC(update_telemetry_status_api),
                tags => [<<"Telemetry">>],
                'requestBody' => status_schema(?DESC(update_telemetry_status_api)),
                responses =>
                    #{
                        200 => status_schema(?DESC(update_telemetry_status_api)),
                        400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
                    }
            }
    };
schema("/telemetry/data") ->
    #{
        'operationId' => data,
        get =>
            #{
                description => ?DESC(get_telemetry_data_api),
                tags => [<<"Telemetry">>],
                responses =>
                    #{
                        200 => mk(ref(?MODULE, telemetry), #{desc => ?DESC(get_telemetry_data_api)}),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Telemetry is not enabled">>
                        )
                    }
            }
    }.

status_schema(Desc) ->
    mk(ref(?MODULE, status), #{in => body, desc => Desc}).

fields(status) ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    desc => ?DESC(enable),
                    default => true,
                    example => false
                }
            )}
    ];
fields(telemetry) ->
    [
        {emqx_version,
            mk(
                string(),
                #{
                    desc => ?DESC(emqx_version),
                    example => <<"5.0.0-beta.3-32d1547c">>
                }
            )},
        {license,
            mk(
                map(),
                #{
                    desc => ?DESC(license),
                    example => #{edition => <<"opensource">>}
                }
            )},
        {os_name,
            mk(
                string(),
                #{
                    desc => ?DESC(os_name),
                    example => <<"Linux">>
                }
            )},
        {os_version,
            mk(
                string(),
                #{
                    desc => ?DESC(os_version),
                    example => <<"20.04">>
                }
            )},
        {otp_version,
            mk(
                string(),
                #{
                    desc => ?DESC(otp_version),
                    example => <<"24">>
                }
            )},
        {up_time,
            mk(
                integer(),
                #{
                    desc => ?DESC(up_time),
                    example => 20220113
                }
            )},
        {uuid,
            mk(
                string(),
                #{
                    desc => ?DESC(uuid),
                    example => <<"AAAAAAAA-BBBB-CCCC-2022-DDDDEEEEFFF">>
                }
            )},
        {nodes_uuid,
            mk(
                array(binary()),
                #{
                    desc => ?DESC(nodes_uuid),
                    example => [
                        <<"AAAAAAAA-BBBB-CCCC-2022-DDDDEEEEFFF">>,
                        <<"ZZZZZZZZ-CCCC-BBBB-2022-DDDDEEEEFFF">>
                    ]
                }
            )},
        {active_plugins,
            mk(
                array(binary()),
                #{
                    desc => ?DESC(active_plugins),
                    example => [<<"Plugin A">>, <<"Plugin B">>]
                }
            )},
        {active_modules,
            mk(
                array(binary()),
                #{
                    desc => ?DESC(active_modules),
                    example => [<<"Module A">>, <<"Module B">>]
                }
            )},
        {num_clients,
            mk(
                integer(),
                #{
                    desc => ?DESC(num_clients),
                    example => 20220113
                }
            )},
        {messages_received,
            mk(
                integer(),
                #{
                    desc => ?DESC(messages_received),
                    example => 2022
                }
            )},
        {messages_sent,
            mk(
                integer(),
                #{
                    desc => ?DESC(messages_sent),
                    example => 2022
                }
            )}
    ].

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

status(get, _Params) ->
    {200, get_telemetry_status()};
status(put, #{body := Body}) ->
    Enable = maps:get(<<"enable">>, Body),
    case Enable =:= emqx_modules_conf:is_telemetry_enabled() of
        true ->
            Reason =
                case Enable of
                    true -> <<"Telemetry status is already enabled">>;
                    false -> <<"Telemetry status is already disabled">>
                end,
            {400, #{code => ?BAD_REQUEST, message => Reason}};
        false ->
            case enable_telemetry(Enable) of
                ok ->
                    {200, get_telemetry_status()};
                {error, Reason} ->
                    {400, #{
                        code => ?BAD_REQUEST,
                        message => Reason
                    }}
            end
    end.

data(get, _Request) ->
    case emqx_modules_conf:is_telemetry_enabled() of
        true ->
            {200, emqx_json:encode(get_telemetry_data())};
        false ->
            {404, #{
                code => ?NOT_FOUND,
                message => <<"Telemetry is not enabled">>
            }}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

enable_telemetry(Enable) ->
    emqx_modules_conf:set_telemetry_status(Enable).

get_telemetry_status() ->
    #{enable => emqx_modules_conf:is_telemetry_enabled()}.

get_telemetry_data() ->
    {ok, TelemetryData} = emqx_telemetry:get_telemetry(),
    TelemetryData.
