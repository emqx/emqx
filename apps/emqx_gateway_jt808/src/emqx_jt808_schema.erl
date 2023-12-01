%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_jt808_schema).

-include("emqx_jt808.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-export([fields/1, desc/1]).

-define(NOT_EMPTY(MSG), emqx_resource_validator:not_empty(MSG)).

fields(jt808) ->
    [
        {frame, sc(ref(jt808_frame))},
        {proto, sc(ref(jt808_proto))},
        {mountpoint, emqx_gateway_schema:mountpoint(?DEFAULT_MOUNTPOINT)},
        {retry_interval,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => <<"8s">>,
                    desc => ?DESC(retry_interval)
                }
            )},
        {max_retry_times,
            sc(
                non_neg_integer(),
                #{
                    default => 3,
                    desc => ?DESC(max_retry_times)
                }
            )},
        {message_queue_len,
            sc(
                non_neg_integer(),
                #{
                    default => 10,
                    desc => ?DESC(message_queue_len)
                }
            )},
        {listeners, sc(ref(emqx_gateway_schema, tcp_listeners), #{desc => ?DESC(tcp_listeners)})}
    ] ++ emqx_gateway_schema:gateway_common_options();
fields(jt808_frame) ->
    [
        {max_length, fun jt808_frame_max_length/1}
    ];
fields(jt808_proto) ->
    [
        {allow_anonymous, fun allow_anonymous/1},
        {registry, fun registry_url/1},
        {authentication, fun authentication_url/1},
        {up_topic, fun up_topic/1},
        {dn_topic, fun dn_topic/1}
    ].

jt808_frame_max_length(type) -> non_neg_integer();
jt808_frame_max_length(desc) -> ?DESC(?FUNCTION_NAME);
jt808_frame_max_length(default) -> 8192;
jt808_frame_max_length(required) -> false;
jt808_frame_max_length(_) -> undefined.

allow_anonymous(type) -> boolean();
allow_anonymous(desc) -> ?DESC(?FUNCTION_NAME);
allow_anonymous(default) -> true;
allow_anonymous(required) -> false;
allow_anonymous(_) -> undefined.

registry_url(type) -> binary();
registry_url(desc) -> ?DESC(?FUNCTION_NAME);
registry_url(validator) -> [?NOT_EMPTY("the value of the field 'url' cannot be empty")];
registry_url(required) -> false;
registry_url(_) -> undefined.

authentication_url(type) -> binary();
authentication_url(desc) -> ?DESC(?FUNCTION_NAME);
authentication_url(validator) -> [?NOT_EMPTY("the value of the field 'url' cannot be empty")];
authentication_url(required) -> false;
authentication_url(_) -> undefined.

up_topic(type) -> binary();
up_topic(desc) -> ?DESC(?FUNCTION_NAME);
up_topic(default) -> ?DEFAULT_UP_TOPIC;
up_topic(validator) -> [?NOT_EMPTY("the value of the field 'up_topic' cannot be empty")];
up_topic(required) -> true;
up_topic(_) -> undefined.

dn_topic(type) -> binary();
dn_topic(desc) -> ?DESC(?FUNCTION_NAME);
dn_topic(default) -> ?DEFAULT_DN_TOPIC;
dn_topic(validator) -> [?NOT_EMPTY("the value of the field 'dn_topic' cannot be empty")];
dn_topic(required) -> true;
dn_topic(_) -> undefined.

desc(jt808) ->
    "The JT/T 808 protocol gateway provides EMQX with the ability to access JT/T 808 protocol devices.";
desc(jt808_frame) ->
    "Limits for the JT/T 808 frames.";
desc(jt808_proto) ->
    "The JT/T 808 protocol options.";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% internal functions

sc(Type) ->
    sc(Type, #{}).

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
