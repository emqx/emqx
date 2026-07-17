%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_config).

-export([load/0, update/1]).

-include("emqx_bcast.hrl").

-define(NAME_VSN, <<"emqx_bcast-0.1.0">>).

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
        msg_ttl => ttl_to_sec(maps:get(msg_ttl, Config, <<"15d">>)),
        cleanup_interval => ttl_to_sec(maps:get(cleanup_interval, Config, <<"60s">>)),
        max_device_count => maps:get(max_device_count, Config, 10000),
        max_message_size_broadcast => maps:get(max_message_size_broadcast, Config, 65536),
        max_message_size_batch => maps:get(max_message_size_batch, Config, 10240),
        msg_warn_threshold => maps:get(msg_warn_threshold, Config, 100000)
    }.

ttl_to_sec(TTL) when is_binary(TTL) ->
    parse_duration(TTL);
ttl_to_sec(_) ->
    default_ttl().

parse_duration(TTL) ->
    case re:run(TTL, <<"^(\\d+)([smhd])$">>, [{capture, [1, 2], binary}]) of
        {match, [N, <<"s">>]} -> binary_to_integer(N);
        {match, [N, <<"m">>]} -> binary_to_integer(N) * 60;
        {match, [N, <<"h">>]} -> binary_to_integer(N) * 3600;
        {match, [N, <<"d">>]} -> binary_to_integer(N) * 86400;
        _ -> default_ttl()
    end.

default_ttl() -> 15 * 86400.
