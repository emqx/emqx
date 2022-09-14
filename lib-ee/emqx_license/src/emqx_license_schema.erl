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
    key_license/0
]).

roots() ->
    [
        {license,
            hoconsc:mk(
                key_license(),
                #{
                    desc => ?DESC(license_root)
                }
            )}
    ].

fields(key_license) ->
    [
        {key, #{
            type => string(),
            default =>
                "MjIwMTExCjAKMTAKRXZhbHVhdGlvbgpjb250YWN0QGVtcXguaW8KZ"
                "GVmYXVsdAoyMDIyMDQxOQoxODI1CjEwMDAK.MEQCICbgRVijCQov2"
                "hrvZXR1mk9Oa+tyV1F5oJ6iOZeSHjnQAiB9dUiVeaZekDOjztk+NC"
                "Wjhk4PG8tWfw2uFZWruSzD6g==",
            %% so it's not logged
            sensitive => true,
            required => true,
            desc => ?DESC(key_field)
        }},
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

desc(key_license) ->
    "License provisioned as a string.";
desc(_) ->
    undefined.

validations() ->
    [{check_license_watermark, fun check_license_watermark/1}].

key_license() ->
    hoconsc:ref(?MODULE, key_license).

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
