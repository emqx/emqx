%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_funcs_demo).

-export([
    is_my_topic/1,
    duplicate_payload/2
]).

%% check if the topic is of 5 levels.
is_my_topic(Topic) ->
    emqx_topic:levels(Topic) =:= 5.

%% duplicate the payload, but only supports 2 or 3 copies.
duplicate_payload(Payload, 2) ->
    [Payload, Payload];
duplicate_payload(Payload, 3) ->
    [Payload, Payload, Payload].
