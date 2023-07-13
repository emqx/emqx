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

-export([roots/0, fields/1, validations/0, desc/1, tags/0]).

-export([
    default_license/0
]).

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
            type => binary(),
            default => default_license(),
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
            High = hocon_maps:get("license.connection_high_watermark", Conf),
            case High =/= undefined andalso High > Low of
                true -> true;
                false -> {bad_license_watermark, #{high => High, low => Low}}
            end
    end.

%% @doc The default license key.
%% This default license has 1000 connections limit.
%% It is issued on 2023-01-09 and valid for 5 years (1825 days)
%% NOTE: when updating a new key, the schema doc in emqx_license_schema.hocon
%% should be updated accordingly
default_license() ->
    <<
        "MjIwMTExCjAKMTAKRXZhbHVhdGlvbgpjb250YWN0QGVtcXguaW8KZ"
        "GVmYXVsdAoyMDIzMDEwOQoxODI1CjEwMAo=.MEUCIG62t8W15g05f"
        "1cKx3tA3YgJoR0dmyHOPCdbUxBGxgKKAiEAhHKh8dUwhU+OxNEaOn"
        "8mgRDtiT3R8RZooqy6dEsOmDI="
    >>.
