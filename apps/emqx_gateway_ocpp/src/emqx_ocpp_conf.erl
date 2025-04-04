%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Conf modules for emqx-ocpp gateway
-module(emqx_ocpp_conf).

-export([
    default_heartbeat_interval/0,
    heartbeat_checking_times_backoff/0,
    retry_interval/0,
    message_format_checking/0,
    max_mqueue_len/0,
    strit_mode/1,
    uptopic/1,
    up_reply_topic/0,
    up_error_topic/0,
    dntopic/0
]).

-define(KEY(K), [gateway, ocpp, K]).

conf(K, Default) ->
    emqx_config:get(?KEY(K), Default).

-spec default_heartbeat_interval() -> pos_integer().
default_heartbeat_interval() ->
    conf(default_heartbeat_interval, 600).

-spec heartbeat_checking_times_backoff() -> pos_integer().
heartbeat_checking_times_backoff() ->
    conf(heartbeat_checking_times_backoff, 1).

-spec strit_mode(upstream | dnstream) -> boolean().
strit_mode(dnstream) ->
    dnstream(strit_mode, false);
strit_mode(upstream) ->
    upstream(strit_mode, false).

-spec retry_interval() -> pos_integer().
retry_interval() ->
    dnstream(retry_interval, 30).

-spec max_mqueue_len() -> pos_integer().
max_mqueue_len() ->
    dnstream(max_mqueue_len, 10).

-spec message_format_checking() ->
    all
    | upstream_only
    | dnstream_only
    | disable.
message_format_checking() ->
    conf(message_format_checking, disable).

uptopic(Action) ->
    Topic = upstream(topic),
    Mapping = upstream(topic_override_mapping, #{}),
    maps:get(Action, Mapping, Topic).

up_reply_topic() ->
    upstream(reply_topic).

up_error_topic() ->
    upstream(error_topic).

dntopic() ->
    dnstream(topic).

%%--------------------------------------------------------------------
%% internal funcs
%%--------------------------------------------------------------------

dnstream(K) ->
    dnstream(K, undefined).

dnstream(K, Def) ->
    emqx_config:get([gateway, ocpp, dnstream, K], Def).

upstream(K) ->
    upstream(K, undefined).

upstream(K, Def) ->
    emqx_config:get([gateway, ocpp, upstream, K], Def).
