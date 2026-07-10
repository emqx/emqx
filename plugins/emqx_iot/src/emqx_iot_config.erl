%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_config).

-export([load/0, update/1]).

-include("emqx_iot.hrl").

-define(NAME_VSN, <<"emqx_iot-0.1.0">>).

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

ttl_to_sec(<<"15d">>) ->
    15 * 86400;
ttl_to_sec(<<"60s">>) ->
    60;
ttl_to_sec(TTL) when is_binary(TTL) ->
    case emqx_utils:parse_duration(TTL) of
        {ok, Sec} -> Sec;
        _ -> 15 * 86400
    end;
ttl_to_sec(_) ->
    15 * 86400.
