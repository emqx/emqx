%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_config).

-export([load/0, update/1]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

-define(NAME_VSN, <<"emqx_bcast-0.1.0">>).

-define(DEFAULT_MSG_TTL, 15 * 86400).
-define(DEFAULT_CLEANUP_INTERVAL, 60).

load() ->
    Config =
        try emqx_plugins:get_config(?NAME_VSN, #{}) of
            C -> C
        catch
            _:_ -> #{}
        end,
    update(Config).

update(Config) ->
    persistent_term:put({?APP, config}, normalize(Config)).

normalize(Config) ->
    #{
        broadcast_topic => maps:get(broadcast_topic, Config, <<"/sys/broadcast/${productKey}">>),
        batch_topic => maps:get(batch_topic, Config, <<"/${productKey}/${deviceName}/user/get">>),
        msg_ttl => duration_to_sec(msg_ttl, maps:get(msg_ttl, Config, <<"15d">>)),
        cleanup_interval => duration_to_sec(
            cleanup_interval, maps:get(cleanup_interval, Config, <<"60s">>)
        ),
        max_device_count => maps:get(max_device_count, Config, 10000),
        max_message_size_broadcast => maps:get(max_message_size_broadcast, Config, 65536),
        max_message_size_batch => maps:get(max_message_size_batch, Config, 10240),
        msg_warn_threshold => maps:get(msg_warn_threshold, Config, 100000),
        force_upgrade_qos => maps:get(force_upgrade_qos, Config, true)
    }.

duration_to_sec(Field, Value) when is_binary(Value) ->
    case parse_duration(Value) of
        {ok, Sec} ->
            Sec;
        error ->
            Default = field_default(Field),
            ?SLOG(warning, #{
                msg => "invalid_plugin_config_duration",
                field => Field,
                value => Value,
                default => Default
            }),
            Default
    end;
duration_to_sec(Field, Value) when is_integer(Value), Value > 0 ->
    Value;
duration_to_sec(Field, Value) ->
    Default = field_default(Field),
    ?SLOG(warning, #{
        msg => "invalid_plugin_config_duration",
        field => Field,
        value => Value,
        default => Default
    }),
    Default.

parse_duration(TTL) ->
    case re:run(TTL, <<"^(\\d+)([smhd])$">>, [{capture, [1, 2], binary}]) of
        {match, [N, <<"s">>]} -> {ok, binary_to_integer(N)};
        {match, [N, <<"m">>]} -> {ok, binary_to_integer(N) * 60};
        {match, [N, <<"h">>]} -> {ok, binary_to_integer(N) * 3600};
        {match, [N, <<"d">>]} -> {ok, binary_to_integer(N) * 86400};
        _ -> error
    end.

field_default(msg_ttl) -> ?DEFAULT_MSG_TTL;
field_default(cleanup_interval) -> ?DEFAULT_CLEANUP_INTERVAL.
