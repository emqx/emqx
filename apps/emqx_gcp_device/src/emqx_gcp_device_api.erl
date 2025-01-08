%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gcp_device_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("emqx/include/logger.hrl").

-define(TAGS, [<<"GCP Devices">>]).
-define(TAB, emqx_gcp_device).
-define(FORMAT_FUN, {emqx_gcp_device, format_device}).

-export([import_devices/1]).
-export([get_device/1, update_device/1, remove_device/1]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([
    '/gcp_devices'/2,
    '/gcp_devices/:deviceid'/2
]).

-type deviceid() :: emqx_gcp_device:deviceid().
-type formatted_device() :: emqx_gcp_device:formatted_device().
-type base64_encoded_config() :: emqx_gcp_device:encoded_config().
-type imported_key() :: #{
    binary() := binary() | non_neg_integer()
    % #{
    %     <<"key">> => binary(),
    %     <<"key_type">> => binary(),
    %     <<"expires_at">> => non_neg_integer()
    % }.
}.
-type key_fields() :: key | key_type | expires_at.
-type imported_device() :: #{
    binary() := deviceid() | binary() | [imported_key()] | base64_encoded_config() | boolean()
    % #{
    %     <<"deviceid">> => deviceid(),
    %     <<"project">> => binary(),
    %     <<"location">> => binary(),
    %     <<"registry">> => binary(),
    %     <<"keys">> => [imported_key()],
    %     <<"config">> => base64_encoded_config(),
    %     <<"blocked">> => boolean(),
    % }.
}.
-type device_fields() :: deviceid | project | location | registry | keys | config.
-type checked_device_fields() :: device_fields() | key_fields().
-type validated_device() :: #{checked_device_fields() := term()}.

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/gcp_devices",
        "/gcp_devices/:deviceid"
    ].

schema("/gcp_devices") ->
    #{
        'operationId' => '/gcp_devices',
        get => #{
            description => ?DESC(gcp_devices_get),
            tags => ?TAGS,
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, page),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 => [
                    {data, hoconsc:mk(hoconsc:array(hoconsc:ref(gcp_device_all_info)), #{})},
                    {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, meta), #{})}
                ]
            }
        },
        post => #{
            description => ?DESC(gcp_devices_post),
            tags => ?TAGS,
            'requestBody' => hoconsc:mk(hoconsc:array(?R_REF(gcp_exported_device)), #{}),
            responses =>
                #{
                    200 => hoconsc:ref(import_result),
                    400 => emqx_dashboard_swagger:error_codes(
                        ['BAD_REQUEST'],
                        <<"Bad Request">>
                    )
                }
        }
    };
schema("/gcp_devices/:deviceid") ->
    #{
        'operationId' => '/gcp_devices/:deviceid',
        get =>
            #{
                description => ?DESC(gcp_device_get),
                tags => ?TAGS,
                parameters => [deviceid(#{in => path})],
                responses =>
                    #{
                        200 => hoconsc:mk(
                            hoconsc:ref(gcp_device_all_info),
                            #{
                                desc => ?DESC(gcp_device_all_info)
                            }
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            ['NOT_FOUND'],
                            ?DESC(gcp_device_response404)
                        )
                    }
            },
        put =>
            #{
                description => ?DESC(gcp_device_put),
                tags => ?TAGS,
                parameters => [deviceid(#{in => path})],
                'requestBody' => hoconsc:ref(gcp_device),
                responses =>
                    #{
                        200 => hoconsc:mk(
                            hoconsc:ref(gcp_device_info),
                            #{
                                desc => ?DESC(gcp_device_info)
                            }
                        ),
                        400 => emqx_dashboard_swagger:error_codes(
                            ['BAD_REQUEST'],
                            <<"Bad Request">>
                        )
                    }
            },
        delete => #{
            description => ?DESC(gcp_device_delete),
            tags => ?TAGS,
            parameters => [deviceid(#{in => path})],
            responses => #{
                204 => <<"GCP device deleted">>
            }
        }
    }.

fields(gcp_device) ->
    [
        {registry,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC(registry),
                    default => <<>>,
                    example => <<"my-registry">>
                }
            )},
        {project,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC(project),
                    default => <<>>,
                    example => <<"iot-export">>
                }
            )},
        {location,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC(location),
                    default => <<>>,
                    example => <<"europe-west1">>
                }
            )},
        {keys,
            hoconsc:mk(
                ?ARRAY(hoconsc:ref(key)),
                #{
                    desc => ?DESC(keys),
                    default => []
                }
            )},
        {config,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC(config),
                    required => true,
                    example => <<"bXktY29uZmln">>
                }
            )}
    ];
fields(gcp_device_info) ->
    fields(deviceid) ++ fields(gcp_device);
fields(gcp_device_all_info) ->
    [
        {created_at,
            hoconsc:mk(
                non_neg_integer(),
                #{
                    desc => ?DESC(created_at),
                    required => true,
                    example => 1690484400
                }
            )}
    ] ++ fields(gcp_device_info);
fields(gcp_exported_device) ->
    [
        {blocked,
            hoconsc:mk(
                boolean(),
                #{
                    desc => ?DESC(blocked),
                    required => true,
                    example => false
                }
            )}
    ] ++ fields(deviceid) ++ fields(gcp_device);
fields(import_result) ->
    [
        {errors,
            hoconsc:mk(
                non_neg_integer(),
                #{
                    desc => ?DESC(imported_counter_errors),
                    required => true,
                    example => 0
                }
            )},
        {imported,
            hoconsc:mk(
                non_neg_integer(),
                #{
                    desc => ?DESC(imported_counter),
                    required => true,
                    example => 14
                }
            )}
    ];
fields(key) ->
    [
        {key,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC(key),
                    required => true,
                    example => <<"<DEVICE-PUBLIC-KEY>">>
                }
            )},
        {key_type,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC(key_type),
                    required => true,
                    example => <<"ES256_PEM">>
                }
            )},
        {expires_at,
            hoconsc:mk(
                non_neg_integer(),
                #{
                    desc => ?DESC(expires_at),
                    required => true,
                    example => 1706738400
                }
            )}
    ];
fields(deviceid) ->
    [
        deviceid()
    ].

'/gcp_devices'(get, #{query_string := Params}) ->
    Response = emqx_mgmt_api:paginate(?TAB, Params, ?FORMAT_FUN),
    {200, Response};
'/gcp_devices'(post, #{body := Body}) ->
    import_devices(Body).

'/gcp_devices/:deviceid'(get, #{bindings := #{deviceid := DeviceId}}) ->
    get_device(DeviceId);
'/gcp_devices/:deviceid'(put, #{bindings := #{deviceid := DeviceId}, body := Body}) ->
    update_device(maps:merge(Body, #{<<"deviceid">> => DeviceId}));
'/gcp_devices/:deviceid'(delete, #{bindings := #{deviceid := DeviceId}}) ->
    remove_device(DeviceId).

%%------------------------------------------------------------------------------
%% Handlers
%%------------------------------------------------------------------------------

-spec import_devices([imported_device()]) ->
    {200, #{imported := non_neg_integer(), errors := non_neg_integer()}}
    | {400, #{message := binary()}}.
import_devices(Devices) ->
    case validate_devices(Devices) of
        {ok, FormattedDevices} ->
            {NumImported, NumErrors} = emqx_gcp_device:import_devices(FormattedDevices),
            {200, #{imported => NumImported, errors => NumErrors}};
        {error, Reason} ->
            {400, #{message => Reason}}
    end.

-spec get_device(deviceid()) -> {200, formatted_device()} | {404, 'NOT_FOUND', binary()}.
get_device(DeviceId) ->
    case emqx_gcp_device:get_device(DeviceId) of
        {ok, Device} ->
            {200, Device};
        not_found ->
            Message = list_to_binary(io_lib:format("device not found: ~s", [DeviceId])),
            {404, 'NOT_FOUND', Message}
    end.

-spec update_device(imported_device()) -> {200, formatted_device()} | {400, binary()}.
update_device(Device) ->
    case validate_device(Device) of
        {ok, ValidatedDevice} ->
            ok = emqx_gcp_device:put_device(ValidatedDevice),
            {200, ValidatedDevice};
        {error, Reason} ->
            {400, Reason}
    end.

-spec remove_device(deviceid()) -> {204}.
remove_device(DeviceId) ->
    ok = emqx_gcp_device:remove_device(DeviceId),
    {204}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

-define(KEY_TYPES, [<<"RSA_PEM">>, <<"RSA_X509_PEM">>, <<"ES256_PEM">>, <<"ES256_X509_PEM">>]).

-spec deviceid() -> tuple().
deviceid() ->
    deviceid(#{}).

-spec deviceid(map()) -> tuple().
deviceid(Override) ->
    {deviceid,
        hoconsc:mk(
            binary(),
            maps:merge(
                #{
                    desc => ?DESC(deviceid),
                    required => true,
                    example => <<"c2-ec-x509">>
                },
                Override
            )
        )}.

-spec validate_devices([imported_device()]) -> {ok, [validated_device()]} | {error, binary()}.
validate_devices(Devices) ->
    validate_devices(Devices, []).

-spec validate_devices([imported_device()], [validated_device()]) ->
    {ok, [validated_device()]} | {error, binary()}.
validate_devices([], Validated) ->
    {ok, lists:reverse(Validated)};
validate_devices([Device | Devices], Validated) ->
    case validate_device(Device) of
        {ok, ValidatedDevice} ->
            validate_devices(Devices, [ValidatedDevice | Validated]);
        {error, _} = Error ->
            Error
    end.

-spec validate_device(imported_device()) -> {ok, validated_device()} | {error, binary()}.
validate_device(Device) ->
    validate([deviceid, project, location, registry, keys, config], Device).

-spec validate([checked_device_fields()], imported_device()) ->
    {ok, validated_device()} | {error, binary()}.
validate(Fields, Device) ->
    validate(Fields, Device, #{}).

-spec validate([checked_device_fields()], imported_device(), validated_device()) ->
    {ok, validated_device()} | {error, binary()}.
validate([], _Device, Validated) ->
    {ok, Validated};
validate([key_type | Fields], #{<<"key_type">> := KeyType} = Device, Validated) ->
    case lists:member(KeyType, ?KEY_TYPES) of
        true ->
            validate(Fields, Device, Validated#{key_type => KeyType});
        false ->
            {error, <<"invalid key_type">>}
    end;
validate([key | Fields], #{<<"key">> := Key} = Device, Validated) ->
    validate(Fields, Device, Validated#{key => Key});
validate([expires_at | Fields], #{<<"expires_at">> := Expire} = Device, Validated) when
    is_integer(Expire)
->
    validate(Fields, Device, Validated#{expires_at => Expire});
validate([expires_at | _Fields], #{<<"expires_at">> := _}, _Validated) ->
    {error, <<"invalid expires_at">>};
validate([expires_at | Fields], Device, Validated) ->
    validate(Fields, Device, Validated#{expires_at => 0});
validate([Field | Fields], Device, Validated) when Field =:= deviceid; Field =:= key ->
    FieldBin = atom_to_binary(Field),
    case maps:find(FieldBin, Device) of
        {ok, Value} when is_binary(Value) ->
            validate(Fields, Device, Validated#{Field => Value});
        _ ->
            {error, <<"invalid or missing field: ", FieldBin/binary>>}
    end;
validate([Field | Fields], Device, Validated) when
    Field =:= project; Field =:= location; Field =:= registry; Field =:= config
->
    FieldBin = atom_to_binary(Field),
    case maps:find(FieldBin, Device) of
        {ok, Value} when is_binary(Value) ->
            validate(Fields, Device, Validated#{Field => Value});
        error ->
            validate(Fields, Device, Validated#{Field => <<>>});
        _ ->
            {error, <<"invalid field: ", FieldBin/binary>>}
    end;
validate([keys | Fields], #{<<"keys">> := Keys} = Device, Validated) when is_list(Keys) ->
    case validate_keys(Keys) of
        {ok, ValidatedKeys} ->
            validate(Fields, Device, Validated#{keys => ValidatedKeys});
        {error, _} = Error ->
            Error
    end;
validate([Field | _Fields], _Device, _Validated) ->
    {error, <<"invalid or missing field: ", (atom_to_binary(Field))/binary>>}.

-spec validate_keys([imported_key()]) ->
    {ok, [validated_device()]} | {error, binary()}.
validate_keys(Keys) ->
    validate_keys(Keys, []).

-spec validate_keys([imported_key()], [validated_device()]) ->
    {ok, [validated_device()]} | {error, binary()}.
validate_keys([], Validated) ->
    {ok, lists:reverse(Validated)};
validate_keys([Key | Keys], Validated) ->
    case validate([key, key_type, expires_at], Key) of
        {ok, ValidatedKey} ->
            validate_keys(Keys, [ValidatedKey | Validated]);
        {error, _} = Error ->
            Error
    end.
