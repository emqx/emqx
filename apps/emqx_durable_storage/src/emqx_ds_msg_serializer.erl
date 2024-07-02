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
    timestamp = Timestamp,
    headers = Headers
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
    {HasPeerhost, PeerHost} =
        case Headers of
            #{peerhost := IP1} -> encode_ip_port(IP1, 0, 0);
            _ -> {false, <<>>}
        end,
    {HasPeername, PeerName} =
        case Headers of
            #{peername := {IP2, Port}} -> encode_ip_port(IP2, 16, Port);
            _ -> {false, <<>>}
        end,
    case Headers of
        #{username := Username} when is_binary(Username) ->
            HasUsername = true;
        _ ->
            Username = <<>>,
            HasUsername = false
    end,
    {StdProps, MiscProps} = serialize_asn1_properties(Headers),
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
        protoVer = asn1_encode_proto_ver(Headers),

        hasPeerhost = HasPeerhost,
        peerhost = PeerHost,

        hasPeername = HasPeername,
        peername = PeerName,

        hasUsername = HasUsername,
        username = Username,

        properties = StdProps,
        clientAttrs = [],
        misc =
            [{flag, I} || I <- MiscFlags] ++
                [{property, I} || I <- MiscProps]
    }),
    Bin.

asn1_encode_proto_ver(#{protocol := mqtt, proto_ver := V}) when is_integer(V), V >= 0, V =< 8 ->
    {mqtt, V};
asn1_encode_proto_ver(#{protocol := 'mqtt-sn', proto_ver := V}) when
    is_integer(V), V >= 0, V =< 255
->
    {'mqtt-sn', V};
asn1_encode_proto_ver(#{protocol := coap, proto_ver := V}) when is_integer(V), V >= 0, V =< 255 ->
    {coap, V};
asn1_encode_proto_ver(_) ->
    {undefined, 'NULL'}.

asn1_decode_proto_ver({undefined, 'NULL'}) ->
    {undefined, undefined};
asn1_decode_proto_ver(A) ->
    A.

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
        protoVer = {Protocol, ProtoVer},

        hasPeerhost = HasPeerhost,
        peerhost = PeerHost,

        hasPeername = HasPeername,
        peername = PeerName,

        hasUsername = HasUsername,
        username = Username,

        properties = Props,
        misc = Misc
    }} = 'DurableMessage':decode('DurableMessage', Blob),
    From =
        case From0 of
            {atom, Bin} -> erlang:binary_to_atom(Bin, utf8);
            {binary, Bin} -> Bin
        end,
    %% Decode flags:
    Flags = #{sys => Sys, dup => Dup, retain => Retain},
    Headers = #{
        username =>
            case HasUsername of
                true -> Username;
                false -> undefined
            end,
        peerhost =>
            case HasPeerhost of
                true -> element(1, decode_ip_port(0, PeerHost));
                false -> undefined
            end,
        peername =>
            case HasPeername of
                true -> decode_ip_port(16, PeerName);
                false -> undefined
            end,
        properties => deserialize_asn1_properties(Props),
        protocol => Protocol,
        proto_ver => ProtoVer
    },
    deserialize_asn1_misc(Misc, #message{
        id = Id,
        qos = Qos,
        from = From,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp,
        flags = Flags,
        headers = Headers
    }).

deserialize_asn1_misc([], Message) ->
    Message;
deserialize_asn1_misc(MiscData, Message0) ->
    lists:foldl(
        fun
            ({flag, #'MiscFlag'{key = Key, value = Val}}, Acc) ->
                Flags = maps:put(Key, Val, Acc#message.flags),
                Acc#message{flags = Flags};
            ({header, #'MiscProperty'{key = Key, value = Val}}, Acc) ->
                Headers = maps:put(binary_to_term(Key), binary_to_term(Val), Acc#message.headers),
                Acc#message{headers = Headers};
            ({property, #'MiscProperty'{key = Key, value = Val}}, Acc) ->
                #message{headers = Headers0} = Acc,
                Headers = maps:update_with(
                    properties,
                    fun(Props) ->
                        maps:put(binary_to_term(Key), binary_to_term(Val), Props)
                    end,
                    Headers0
                ),
                Acc#message{headers = Headers};
            ({extra, #'MiscProperty'{key = Key, value = Val}}, Acc) ->
                Extra = maps:put(binary_to_term(Key), binary_to_term(Val), Acc#message.extra),
                Acc#message{extra = Extra}
        end,
        Message0,
        MiscData
    ).

encode_ip_port({A0, A1, A2, A3}, PortSize, Port) ->
    {true, <<A0:8, A1:8, A2:8, A3:8, Port:PortSize>>};
encode_ip_port({A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, AA, AB, AC, AD, AE, AF}, PortSize, Port) ->
    {true,
        <<A0:8, A1:8, A2:8, A3:8, A4:8, A5:8, A6:8, A7:8, A8:8, A9:8, AA:8, AB:8, AC:8, AD:8, AE:8,
            AF:8, Port:PortSize>>};
encode_ip_port(_, _, _) ->
    {false, <<>>}.

decode_ip_port(PortSize, Bin) ->
    case Bin of
        <<A0:8, A1:8, A2:8, A3:8, Port:PortSize>> ->
            {{A0, A1, A2, A3}, Port};
        <<A0:8, A1:8, A2:8, A3:8, A4:8, A5:8, A6:8, A7:8, A8:8, A9:8, AA:8, AB:8, AC:8, AD:8, AE:8,
            AF:8, Port:PortSize>> ->
            {{A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, AA, AB, AC, AD, AE, AF}, Port}
    end.

serialize_asn1_properties(#{properties := Props}) ->
    maps:fold(fun prop_to_asn1/3, {[], []}, Props);
serialize_asn1_properties(_) ->
    {[], []}.

deserialize_asn1_properties([]) ->
    #{};
deserialize_asn1_properties(Props) ->
    lists:foldl(fun asn1_to_prop/2, #{}, Props).

prop_to_asn1('Payload-Format-Indicator', Val, {Std, Misc}) ->
    {
        [{payloadFormatIndicator, Val} | Std],
        Misc
    };
prop_to_asn1('Message-Expiry-Interval', Val, {Std, Misc}) ->
    {
        [{messageExpiryInterval, Val} | Std],
        Misc
    };
prop_to_asn1('Response-Topic', Val, {Std, Misc}) ->
    {
        [{responseTopic, Val} | Std],
        Misc
    };
prop_to_asn1('Correlation-Data', Val, {Std, Misc}) ->
    {
        [{correlationData, Val} | Std],
        Misc
    };
prop_to_asn1('Content-Type', Val, {Std, Misc}) ->
    {
        [{contentType, Val} | Std],
        Misc
    };
prop_to_asn1('User-Property', Props, {Std, Misc}) ->
    {
        [{userProperty, [#'UserProperty'{key = K, value = V} || {K, V} <- Props]} | Std],
        Misc
    };
prop_to_asn1(Key, Val, {Std, Misc}) ->
    {
        Std,
        [#'MiscProperty'{key = term_to_binary(Key), value = term_to_binary(Val)} | Misc]
    }.

asn1_to_prop({payloadFormatIndicator, Val}, Acc) ->
    maps:put('Payload-Format-Indicator', Val, Acc);
asn1_to_prop({messageExpiryInterval, Val}, Acc) ->
    maps:put('Message-Expiry-Interval', Val, Acc);
asn1_to_prop({responseTopic, Val}, Acc) ->
    maps:put('Response-Topic', Val, Acc);
asn1_to_prop({correlationData, Val}, Acc) ->
    maps:put('Correlation-Data', Val, Acc);
asn1_to_prop({contentType, Val}, Acc) ->
    maps:put('Content-Type', Val, Acc);
asn1_to_prop({userProperty, Props0}, Acc) ->
    Props = [{K, V} || #'UserProperty'{key = K, value = V} <- Props0],
    maps:put('User-Property', Props, Acc).

-ifdef(TEST).

test_messages() ->
    [
        #message{
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
        #message{
            id = <<0, 6, 28, 54, 12, 158, 221, 191, 244, 69, 0, 0, 13, 214, 0, 3>>,
            qos = 0,
            from = <<"MzE3MjU5NzA4NDY3MzcwNzg0NDYxNzI5NDg0NDk4NTM0NDA">>,
            flags = #{dup => true, retain => true},
            headers = #{
                peername => {{127, 0, 0, 1}, 34560},
                protocol => mqtt,
                username => undefined,
                proto_ver => 5,
                peerhost => {127, 0, 0, 1},
                properties =>
                    #{
                        'Content-Type' => <<"text/json">>,
                        'User-Property' => [{<<"foo">>, <<"bar">>}, {<<"baz">>, <<"quux">>}],
                        'Message-Expiry-Interval' => 10001,
                        'Payload-Format-Indicator' => 1,
                        42 => {foo, bar}
                    },
                client_attrs => #{}
            },
            topic = <<"foo/bar">>,
            payload = <<"foo">>,
            timestamp = 1719868325813,
            extra = #{}
        }
    ].

v1_serialize_deserialize_test_() ->
    [
        assert_transcode(v1, Msg)
     || Msg <- test_messages()
    ].

asn1_serialize_deserialize_test_() ->
    [
        assert_transcode(asn1, Msg)
     || Msg <- test_messages()
    ].

assert_transcode(Schema, Msg) ->
    fun() ->
        Blob = serialize(Schema, Msg),
        ?debugFmt("encoded size (~p) = ~p~n", [Schema, size(Blob)]),
        assert_eq(Msg, deserialize(Schema, Blob))
    end.

assert_eq(Expect, Got) ->
    ?assertEqual(
        emqx_ds_test_helpers:message_canonical_form(Expect),
        emqx_ds_test_helpers:message_canonical_form(Got),
        {Expect, Got}
    ).

-endif.
