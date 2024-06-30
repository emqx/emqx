%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc This utility module provides a generic method for encoding
%% (and decoding) MQTT messages at rest.
%%
%% Note to developer: backward compatibility has to be maintained at
%% all times, for all releases.
-module(emqx_ds_msg_serializer).

%% API:
-export([serialize/2, deserialize/2, check_schema/1]).

%% internal exports:
-export([]).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("typerefl/include/types.hrl").
-include("../gen_src/DurableMessage.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-elvis([{elvis_style, atom_naming_convention, disable}]).
-dialyzer({nowarn_function, [serialize_asn1/1, deserialize_asn1/1]}).

%%================================================================================
%% Type declarations
%%================================================================================

%% FIXME: Properl reflection fails dialyzer check due wrong spec in
%% typerefl
-type schema() :: term().

-reflect_type([schema/0]).

%%================================================================================
%% API functions
%%================================================================================

-spec check_schema(schema()) -> ok | {error, _}.
check_schema(v1) ->
    ok;
check_schema(asn1) ->
    ok;
check_schema(_) ->
    {error, "Unknown schema type"}.

-spec serialize(schema(), emqx_types:message()) -> binary().
serialize(v1, Msg) ->
    serialize_v1(Msg);
serialize(asn1, Msg) ->
    serialize_asn1(Msg).

-spec deserialize(schema(), binary()) -> emqx_types:message().
deserialize(v1, Blob) ->
    deserialize_v1(Blob);
deserialize(asn1, Blob) ->
    deserialize_asn1(Blob).

%%================================================================================
%% Internal functions
%%================================================================================

%%--------------------------------------------------------------------------------
%% V1 (erlang:term_to_binary/binary_to_term). Simple not the most
%% space- and CPU-efficient encoding
%% --------------------------------------------------------------------------------

serialize_v1(Msg) ->
    term_to_binary(message_to_value_v1(Msg)).

message_to_value_v1(#message{
    id = Id,
    qos = Qos,
    from = From,
    flags = Flags,
    headers = Headers,
    topic = Topic,
    payload = Payload,
    timestamp = Timestamp,
    extra = Extra
}) ->
    {Id, Qos, From, Flags, Headers, Topic, Payload, Timestamp, Extra}.

deserialize_v1(Blob) ->
    value_v1_to_message(binary_to_term(Blob)).

value_v1_to_message({Id, Qos, From, Flags, Headers, Topic, Payload, Timestamp, Extra}) ->
    #message{
        id = Id,
        qos = Qos,
        from = From,
        flags = Flags,
        headers = Headers,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp,
        extra = Extra
    }.

%%--------------------------------------------------------------------------------
%% Encoding based on ASN1.
%%--------------------------------------------------------------------------------

serialize_asn1(#message{
    id = Id,
    qos = Qos,
    from = From0,
    flags = Flags,
    topic = Topic,
    payload = Payload,
    timestamp = Timestamp
}) ->
    From =
        case is_atom(From0) of
            true -> {atom, erlang:atom_to_binary(From0, utf8)};
            false -> {binary, From0}
        end,
    MiscFlags = [
        #'MiscFlag'{key = atom_to_binary(Key, utf8), value = Val}
     || {Key, Val} <- maps:to_list(Flags), Key =/= sys, Key =/= dup, Key =/= retain
    ],
    {ok, Bin} = 'DurableMessage':encode('DurableMessage', #'DurableMessage'{
        id =
            case Id of
                undefined -> <<>>;
                _ -> Id
            end,
        qos = Qos,
        from = From,
        topic = Topic,
        payload = iolist_to_binary(Payload),
        timestamp = Timestamp,
        flagSys = maps:get(sys, Flags, false),
        flagDup = maps:get(dup, Flags, false),
        flagRetain = maps:get(retain, Flags, false),
        miscFlags = MiscFlags
    }),
    Bin.

deserialize_asn1(Blob) ->
    {ok, #'DurableMessage'{
        id = Id,
        qos = Qos,
        from = From0,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp,
        flagSys = Sys,
        flagDup = Dup,
        flagRetain = Retain,
        miscFlags = MiscFlags
    }} = 'DurableMessage':decode('DurableMessage', Blob),
    From =
        case From0 of
            {atom, Bin} -> erlang:binary_to_atom(Bin, utf8);
            {binary, Bin} -> Bin
        end,
    %% Decode flags:
    Flags =
        lists:foldl(
            fun(#'MiscFlag'{key = K, value = Val}, Acc) ->
                maps:put(binary_to_atom(K, utf8), Val, Acc)
            end,
            #{sys => Sys, dup => Dup, retain => Retain},
            MiscFlags
        ),
    #message{
        id =
            case Id of
                <<>> -> undefined;
                _ -> Id
            end,
        qos = Qos,
        from = From,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp,
        flags = Flags
    }.

-ifdef(TEST).

v1_serialize_deserialize_test() ->
    Msg = #message{
        id = <<"message_id_val">>,
        qos = 2,
        from = <<"from_val">>,
        flags = #{sys => true, dup => true},
        headers = #{foo => bar},
        topic = <<"topic/value">>,
        payload = [<<"foo">>, <<"bar">>],
        timestamp = 42424242,
        extra = #{}
    },
    assert_transcode(v1, Msg).

asn1_serialize_deserialize_test() ->
    Msg = #message{
        id = <<"message_id_val">>,
        qos = 2,
        from = <<"from_val">>,
        flags = #{sys => true, dup => true, some_flag => true},
        topic = <<"topic/value">>,
        payload = <<"foo">>,
        timestamp = 42424242,
        extra = #{}
    },
    assert_transcode(asn1, Msg).

assert_transcode(Schema, Msg) ->
    assert_eq(
        Msg,
        deserialize(Schema, serialize(Schema, Msg))
    ).

assert_eq(Expect, Got) ->
    ?assertEqual(
        emqx_ds_test_helpers:message_canonical_form(Expect),
        emqx_ds_test_helpers:message_canonical_form(Got),
        {Expect, Got}
    ).

-endif.
