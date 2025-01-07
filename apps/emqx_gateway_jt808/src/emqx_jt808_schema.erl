%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_jt808_schema).

-include("emqx_jt808.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).
-export([roots/0, namespace/0, fields/1, desc/1]).

-define(NOT_EMPTY(MSG), emqx_resource_validator:not_empty(MSG)).

roots() -> [].

namespace() -> gateway.

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
        {auth,
            sc(
                hoconsc:union([ref(anonymous_true), ref(anonymous_false)]),
                #{desc => ?DESC(jt808_auth)}
            )},
        {up_topic, fun up_topic/1},
        {dn_topic, fun dn_topic/1}
    ];
fields(anonymous_true) ->
    [
        {allow_anonymous,
            sc(
                hoconsc:union([true]),
                #{desc => ?DESC(jt808_allow_anonymous), required => true}
            )}
    ] ++ fields_reg_auth_required(false);
fields(anonymous_false) ->
    [
        {allow_anonymous,
            sc(
                hoconsc:union([false]),
                #{desc => ?DESC(jt808_allow_anonymous), required => true}
            )}
    ] ++ fields_reg_auth_required(true).

fields_reg_auth_required(Required) ->
    [
        {registry,
            sc(binary(), #{
                desc => ?DESC(registry_url),
                validator => [?NOT_EMPTY("the value of the field 'registry' cannot be empty")],
                required => Required
            })},
        {authentication,
            sc(
                binary(),
                #{
                    desc => ?DESC(authentication_url),
                    validator => [
                        ?NOT_EMPTY("the value of the field 'authentication' cannot be empty")
                    ],
                    required => Required
                }
            )}
    ].

jt808_frame_max_length(type) ->
    non_neg_integer();
jt808_frame_max_length(desc) ->
    ?DESC(?FUNCTION_NAME);
jt808_frame_max_length(default) ->
    8192;
jt808_frame_max_length(required) ->
    false;
jt808_frame_max_length(_) ->
    undefined.

up_topic(type) -> binary();
up_topic(desc) -> ?DESC(jt808_up_topic);
up_topic(default) -> ?DEFAULT_UP_TOPIC;
up_topic(validator) -> [?NOT_EMPTY("the value of the field 'up_topic' cannot be empty")];
up_topic(required) -> true;
up_topic(_) -> undefined.

dn_topic(type) -> binary();
dn_topic(desc) -> ?DESC(jt808_dn_topic);
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
desc(anonymous_false) ->
    ?DESC(jt808_allow_anonymous);
desc(anonymous_true) ->
    ?DESC(jt808_allow_anonymous);
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
