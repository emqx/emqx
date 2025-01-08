%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    default_license/0,
    default_setting/0
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
        %% This feature is not made GA yet, hence hidden.
        %% When license is issued to cutomer-type BUSINESS_CRITICAL (code 3)
        %% This config is taken as the real max_connections limit.
        {dynamic_max_connections, #{
            type => non_neg_integer(),
            default => default(dynamic_max_connections),
            required => false,
            importance => ?IMPORTANCE_HIDDEN,
            desc => ?DESC(dynamic_max_connections)
        }},
        {connection_low_watermark, #{
            type => emqx_schema:percent(),
            default => default(connection_low_watermark),
            example => default(connection_low_watermark),
            desc => ?DESC(connection_low_watermark_field)
        }},
        {connection_high_watermark, #{
            type => emqx_schema:percent(),
            default => default(connection_high_watermark),
            example => default(connection_high_watermark),
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
%% Issued on 2024-04-18 and valid for 5 years (1825 days)
%%
%% NOTE: when updating a new key, below should be updated accordingly:
%% - emqx_license_schema.hocon default connections limit
%% - default(dynamic_max_connections) return value
default_license() ->
    <<
        "MjIwMTExCjAKMTAKRXZhbHVhdGlvbgpjb250YWN0QGVtcXguaW8KdHJpYWwKMjAyNDA0MTgKMTgyNQoyNQo="
        "."
        "MEUCICMWWkfrvyMwQaQAOXEsEcs+d6+5uXc1BDxR7j25fRy4AiEAmblQ4p+FFmdsvnKgcRRkv1zj7PExmZKVk3mVcxH3fgw="
    >>.

%% @doc Exported for testing
default_setting() ->
    Keys =
        [
            connection_low_watermark,
            connection_high_watermark,
            dynamic_max_connections
        ],
    maps:from_list(
        lists:map(
            fun(K) ->
                {K, default(K)}
            end,
            Keys
        )
    ).

default(connection_low_watermark) ->
    <<"75%">>;
default(connection_high_watermark) ->
    <<"80%">>;
default(dynamic_max_connections) ->
    %Must match the value encoded in default license.
    25.
