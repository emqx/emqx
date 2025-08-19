%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_payload_transform).
-moduledoc """
This module is used to inject payload (de)serialization callbacks directly into DS.

Enabling this changes the behavior of the DS API: some functions may return de-serialized data instead of TTV triples.

The following APIs are affected:

- `emqx_ds:tx_write`
- `emqx_ds:dirty_append`
- `emqx_ds:next`
- `emqx_ds:subscribe`

WARNING: this injection mechanism is _only_ meant for last-ditch optimization of broadcasting and fan-out.
It's neither flexible nor easily extendable.

Only use it in the situations where

1) The same payload is likely to be broadcast to multiple `emqx_ds:subscribe`ers.
2) It's provable that de-serialization of data is the bottleneck.

In other words, it's just for `messages` DB.

## Use in builtin backends

Internally, injection is done in three places:

- `StorageLayout:next` - deserialization
- `StorageLayout:otx_prepare_tx` - serialization of writes
- `emqx_ds_beamsplitter:dispatch` - deserialization of beams' packs
""".

%% API:
-export([
    message_to_ttv/1,

    serialize_payload/2,
    serialize_payload_fun/1,

    deserialize_payload/2,
    deserialize/2,
    deserialize_fun/1
]).

-export_type([
    t/0
]).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include("emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type t() :: ?ds_pt_identity | ?ds_pt_message_v1.

%%================================================================================
%% API functions
%%================================================================================

-spec message_to_ttv(emqx_types:message()) ->
    {emqx_ds:topic(), ?ds_tx_ts_monotonic, emqx_types:message()}.
message_to_ttv(Msg = #message{topic = TopicBin}) ->
    {emqx_ds:topic_words(TopicBin), ?ds_tx_ts_monotonic, Msg#message{topic = <<>>}}.

-spec serialize_payload(t(), tuple() | binary()) -> binary().
serialize_payload(?ds_pt_identity, Term) ->
    Term;
serialize_payload(?ds_pt_message_v1, Msg = #message{}) ->
    emqx_ds_msg_serializer:serialize(asn1, Msg).

-spec deserialize_payload(t(), binary()) -> binary() | emqx_types:message().
deserialize_payload(?ds_pt_identity, Bin) ->
    Bin;
deserialize_payload(?ds_pt_message_v1, MsgBin) ->
    emqx_ds_msg_serializer:deserialize(asn1, MsgBin).

-spec deserialize(t(), emqx_ds:ttv()) -> emqx_ds:ttv() | emqx_types:message().
deserialize(?ds_pt_identity, TTV) ->
    TTV;
deserialize(?ds_pt_message_v1, {Topic, _TS, MsgBin}) ->
    Msg = deserialize_payload(?ds_pt_message_v1, MsgBin),
    Msg#message{topic = emqx_topic:join(Topic)}.

-doc "Curried version of `deserialize'.".
deserialize_fun(Type) ->
    fun(In) ->
        deserialize(Type, In)
    end.

-doc "Curried version of `serialize_payload'.".
serialize_payload_fun(Type) ->
    fun(In) ->
        serialize_payload(Type, In)
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
