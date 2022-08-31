%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%%------------------------------------------------------------------------------
%% hocon_schema callbacks
%%------------------------------------------------------------------------------

-behaviour(hocon_schema).

-export([roots/0, fields/1, validations/0, desc/1]).

-export([
    license_type/0,
    key_license/0,
    file_license/0
]).

roots() ->
    [
        {license,
            hoconsc:mk(
                license_type(),
                #{
                    desc => ?DESC(license_root)
                }
            )}
    ].

fields(key_license) ->
    [
        {type, #{
            type => key,
            required => true,
            desc => ?DESC(license_type_field)
        }},
        {key, #{
            type => string(),
            %% so it's not logged
            sensitive => true,
            required => true,
            desc => ?DESC(key_field)
        }},
        {file, #{
            type => string(),
            required => false,
            desc => ?DESC(file_field)
        }}
        | common_fields()
    ];
fields(file_license) ->
    [
        {type, #{
            type => file,
            required => true,
            desc => ?DESC(license_type_field)
        }},
        {key, #{
            type => string(),
            %% so it's not logged
            sensitive => true,
            required => false,
            desc => ?DESC(key_field)
        }},
        {file, #{
            type => string(),
            desc => ?DESC(file_field)
        }}
        | common_fields()
    ].

desc(key_license) ->
    "License provisioned as a string.";
desc(file_license) ->
    "License provisioned as a file.";
desc(_) ->
    undefined.

common_fields() ->
    [
        {connection_low_watermark, #{
            type => emqx_schema:percent(),
            default => "75%",
            desc => ?DESC(connection_low_watermark_field)
        }},
        {connection_high_watermark, #{
            type => emqx_schema:percent(),
            default => "80%",
            desc => ?DESC(connection_high_watermark_field)
        }}
    ].

validations() ->
    [{check_license_watermark, fun check_license_watermark/1}].

license_type() ->
    hoconsc:union([
        key_license(),
        file_license()
    ]).

key_license() ->
    hoconsc:ref(?MODULE, key_license).

file_license() ->
    hoconsc:ref(?MODULE, file_license).

check_license_watermark(Conf) ->
    case hocon_maps:get("license.connection_low_watermark", Conf) of
        undefined ->
            true;
        Low ->
            High = hocon_maps:get("license.connection_high_watermark", Conf),
            case High =/= undefined andalso High > Low of
                true -> true;
                false -> {bad_license_watermark, #{high => High, low => Low}}
            end
    end.
