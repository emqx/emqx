%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Kafka connection configuration
-module(emqx_bridge_impl_kafka).
-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2,
    is_buffer_supported/0
]).

is_buffer_supported() -> true.

callback_mode() -> async_if_possible.

on_start(InstId, Config) ->
    emqx_bridge_impl_kafka_producer:on_start(InstId, Config).

on_stop(InstId, State) ->
    emqx_bridge_impl_kafka_producer:on_stop(InstId, State).

on_query(InstId, Msg, State) ->
    emqx_bridge_impl_kafka_producer:on_query(InstId, Msg, State).

on_get_status(InstId, State) ->
    emqx_bridge_impl_kafka_producer:on_get_status(InstId, State).
