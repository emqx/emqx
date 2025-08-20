%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_payload_transform).
-moduledoc """
This module injects payload (de)serialization callbacks directly into DS.
Enabling it for a DB changes behavior of DS API:
some functions may return de-serialized data instead of normal TTV triples.

The following APIs are affected:

- `emqx_ds:tx_write` - value part of the TTV triple can be an term other than binary
- `emqx_ds:dirty_append` - same as `tx_write`
- `emqx_ds:next` - batch may consist of values of arbitrary type
- `emqx_ds:subscribe` - same as next

WARNING: this injection mechanism is _only_ meant for last-ditch optimization
of broadcasting and fan-out to DS subscriptions.
It's neither flexible nor easily extendable, and it won't help with anything else.

One should consider using it only use it in the situation where

1) The same payload is likely to be broadcast to multiple `emqx_ds:subscribe`ers.
2) It's provable that de-serialization of data creates a bottleneck.

In other words, it's just for `messages` DB.

## Use in builtin backends

Internally, injection is done in three places:

- `StorageLayout:next` - deserialization
- `StorageLayout:otx_prepare_tx` - serialization of writes
- `emqx_ds_beamsplitter:dispatch` - deserialization of beams' packs

This optimization _only_ makes the difference in the beamsplitter,
where it allows deserialization to happen before the fanout.
""".

%% API:
-export([
    default_schema/1,

    message_to_ttv/1,

    ser_fun/1,
    deser_fun/1,

    deser_batch/2
]).

-export_type([
    type/0,
    payload/0,
    schema/0,
    ser_fun/0,
    deser_fun/0
]).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include("emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-doc """
Data representation that is visible externally.
""".
-type type() :: ?ds_pt_ttv | ?ds_pt_mqtt.

-type payload() :: emqx_types:message().

-doc """
Schema of the internal data representation.
Values of this type can be stored permanently or traverse network.
""".
-type schema() :: ?ds_pt_ttv | {?ds_pt_mqtt, asn1}.

-type ser_fun() :: fun((payload()) -> binary()).
-type deser_fun() :: fun((emqx_ds:ttv()) -> payload()).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Return the default schema for a given payload type.
""".
-spec default_schema(type()) -> schema().
default_schema(?ds_pt_ttv) ->
    ?ds_pt_ttv;
default_schema(?ds_pt_mqtt) ->
    {?ds_pt_mqtt, asn1}.

-doc """
Helper function that transforms MQTT message to a TTV.
It should be called by the business layer before sending messages to `tx_write` and the like,
because the write side of DS APIs still works with data wrappind in a TTV triple.
""".
-spec message_to_ttv(emqx_types:message()) ->
    {emqx_ds:topic(), ?ds_tx_ts_monotonic, emqx_types:message()}.
message_to_ttv(Msg = #message{topic = TopicBin}) ->
    {emqx_ds:topic_words(TopicBin), ?ds_tx_ts_monotonic, Msg#message{topic = <<>>}}.

-spec ser_fun(schema()) -> ser_fun().
ser_fun(?ds_pt_ttv) ->
    fun id/1;
ser_fun({?ds_pt_mqtt, asn1}) ->
    fun emqx_ds_msg_serializer:serialize_asn1/1.

-spec deser_fun(schema()) -> deser_fun().
deser_fun(?ds_pt_ttv) ->
    fun id/1;
deser_fun({?ds_pt_mqtt, asn1}) ->
    fun({Topic, _, Binary}) ->
        Msg = emqx_ds_msg_serializer:deserialize_asn1(Binary),
        Msg#message{topic = emqx_topic:join(Topic)}
    end.

-spec deser_batch(schema(), [emqx_ds:ttv()]) -> [payload()].
deser_batch(?ds_pt_ttv, Batch) ->
    Batch;
deser_batch(Schema, Batch) ->
    lists:map(deser_fun(Schema), Batch).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

id(A) ->
    A.
