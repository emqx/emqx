%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    on_query_async/4,
    on_get_status/2,
    is_buffer_supported/0
]).

is_buffer_supported() -> true.

callback_mode() -> async_if_possible.

on_start(InstId, Config) ->
    emqx_bridge_impl_kafka_producer:on_start(InstId, Config).

on_stop(InstId, State) ->
    emqx_bridge_impl_kafka_producer:on_stop(InstId, State).

on_query(InstId, Req, State) ->
    emqx_bridge_impl_kafka_producer:on_query(InstId, Req, State).

on_query_async(InstId, Req, ReplyFn, State) ->
    emqx_bridge_impl_kafka_producer:on_query_async(InstId, Req, ReplyFn, State).

on_get_status(InstId, State) ->
    emqx_bridge_impl_kafka_producer:on_get_status(InstId, State).
