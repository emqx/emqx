%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%%------------------------------------------------------------------------------
%% hocon_schema callbacks
%%------------------------------------------------------------------------------

-behaviour(hocon_schema).

-export([namespace/0, roots/0, fields/1, validations/0, desc/1, tags/0]).

-export([
    default_license/0
]).

namespace() -> "license".

roots() ->
    [
        {license,
            hoconsc:mk(
                hoconsc:ref(?MODULE, key_license),
                #{
                    desc => ?DESC(license_root)
                }
            )}
    ].

tags() ->
    [<<"License">>].

fields(key_license) ->
    [
        {key, #{
            type => hoconsc:union([default, binary()]),
            default => <<"default">>,
            %% so it's not logged
            sensitive => true,
            required => true,
            desc => ?DESC(key_field)
        }},
        {connection_low_watermark, #{
            type => emqx_schema:percent(),
            default => <<"75%">>,
            example => <<"75%">>,
            desc => ?DESC(connection_low_watermark_field)
        }},
        {connection_high_watermark, #{
            type => emqx_schema:percent(),
            default => <<"80%">>,
            example => <<"80%">>,
            desc => ?DESC(connection_high_watermark_field)
        }}
    ].

desc(key_license) ->
    "License provisioned as a string.";
desc(_) ->
    undefined.

validations() ->
    [{check_license_watermark, fun check_license_watermark/1}].

check_license_watermark(Conf) ->
    case hocon_maps:get("license.connection_low_watermark", Conf) of
        undefined ->
            true;
        Low ->
            case hocon_maps:get("license.connection_high_watermark", Conf) of
                undefined ->
                    {bad_license_watermark, #{high => undefined, low => Low}};
                High ->
                    {ok, HighFloat} = emqx_schema:to_percent(High),
                    {ok, LowFloat} = emqx_schema:to_percent(Low),
                    case HighFloat > LowFloat of
                        true -> true;
                        false -> {bad_license_watermark, #{high => High, low => Low}}
                    end
            end
    end.

%% @doc The default license key.
%% This default license has 25 connections limit.
%% Issued on 2023-12-08 and valid for 5 years (1825 days)
%% NOTE: when updating a new key, the schema doc in emqx_license_schema.hocon
%% should be updated accordingly
default_license() ->
    <<
        "MjIwMTExCjAKMTAKRXZhbHVhdGlvbgpjb250YWN0QGVtcXguaW8KdHJpYWwKMjAyMzEyMDgKMTgyNQoyNQo=."
        "MEUCIE271MtH+4bb39OZKD4mvVkurwZ3LX44KUvuOxkbjQz2AiEAqL7BP44PMUS5z5SAN1M4y3v3h47J8qORAqcuetnyexw="
    >>.
