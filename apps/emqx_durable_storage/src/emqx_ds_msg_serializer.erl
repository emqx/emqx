%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-elvis([{elvis_style, atom_naming_convention, disable}]).
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
    MiscFlags = maps:fold(
        fun
            (Key, Val, Acc) when Key =/= sys, Key =/= dup, Key =/= retain ->
                [asn1_encode_misc(flag, Key, Val) | Acc];
            (_, _, Acc) ->
                Acc
        end,
        [],
        Flags
    ),
    {StdHeaders, StdProps, MiscHeaders} = asn1_encode_headers(Headers),
    {ok, Bin} = 'DurableMessage':encode('DurableMessage', #'DurableMessage'{
        id = Id,
        from =
            case is_atom(From0) of
                true -> {atom, erlang:atom_to_binary(From0, utf8)};
                false -> {binary, From0}
            end,
        topic = Topic,
        payload = iolist_to_binary(Payload),
        timestamp = Timestamp,

        qos = Qos,
        sys = maps:get(sys, Flags, false),
        dup = maps:get(dup, Flags, false),
        retain = maps:get(retain, Flags, false),

        properties = StdProps,
        headers = StdHeaders,

        %% TODO: store client attrs?
        misc = MiscFlags ++ MiscHeaders
    }),
    Bin.

deserialize_asn1(Blob) ->
    {ok, #'DurableMessage'{
        id = Id,
        from = From0,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp,
        qos = Qos,

        sys = Sys,
        dup = Dup,
        retain = Retain,

        headers = StdHeaders,
        properties = StdProperties,

        misc = Misc
    }} = 'DurableMessage':decode('DurableMessage', Blob),
    From =
        case From0 of
            {atom, Bin} -> erlang:binary_to_atom(Bin, utf8);
            {binary, Bin} -> Bin
        end,
    %% Decode flags:
    Flags = #{sys => Sys, dup => Dup, retain => Retain},
    asn1_deserialize_misc(Misc, #message{
        id = Id,
        qos = Qos,
        from = From,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp,
        flags = Flags,
        headers = asn1_decode_headers(StdHeaders, StdProperties)
    }).

asn1_encode_headers(Headers) ->
    PeerName =
        case Headers of
            #{peername := {IP1, Port}} -> encode_ip_port(16, IP1, Port);
            _ -> asn1_NOVALUE
        end,
    PeerHost =
        case Headers of
            #{peerhost := IP2} -> encode_ip_port(0, IP2, 0);
            _ -> asn1_NOVALUE
        end,
    ProtoVer = asn1_encode_proto_ver(Headers),
    StdHeaders = #'StdHeaders'{
        protoVer = ProtoVer,
        peername = PeerName,
        peerhost = PeerHost,
        username =
            case Headers of
                #{username := U} when is_binary(U) -> U;
                _ -> asn1_NOVALUE
            end
    },
    {StdProps, MiscProps} = asn1_encode_properties(maps:get(properties, Headers, #{})),
    MiscHeaders = maps:fold(
        fun
            (Header, _V, Acc) when
                Header =:= properties; Header =:= username; Header =:= client_attrs
            ->
                Acc;
            (protocol, _V, Acc) when ProtoVer =/= asn1_NOVALUE ->
                Acc;
            (proto_ver, _V, Acc) when ProtoVer =/= asn1_NOVALUE ->
                Acc;
            (peername, _V, Acc) when PeerName =/= asn1_NOVALUE ->
                Acc;
            (peerhost, _V, Acc) when PeerHost =/= asn1_NOVALUE ->
                Acc;
            %% Add headers that could not be encoded using fixed schema:
            (Key, Val, Acc) ->
                [asn1_encode_misc(header, Key, Val) | Acc]
        end,
        [],
        Headers
    ),
    {StdHeaders, StdProps, MiscHeaders ++ MiscProps}.

asn1_encode_properties(Props) ->
    UserProps = maps:get('User-Property', Props, []),
    StdProperties = #'StdProperties'{
        payloadFormatIndicator = asn1_std_prop('Payload-Format-Indicator', Props),
        messageExpiryInterval = asn1_std_prop('Message-Expiry-Interval', Props),
        responseTopic = asn1_std_prop('Response-Topic', Props),
        correlationData = asn1_std_prop('Correlation-Data', Props),
        contentType = asn1_std_prop('Content-Type', Props),
        userProperty = [#'UserProperty'{key = K, value = V} || {K, V} <- UserProps]
    },
    MiscProperties = maps:fold(
        fun
            (K, V, Acc) when
                K =/= 'Payload-Format-Indicator',
                K =/= 'Message-Expiry-Interval',
                K =/= 'Response-Topic',
                K =/= 'Correlation-Data',
                K =/= 'Content-Type',
                K =/= 'User-Property'
            ->
                [asn1_encode_misc(property, K, V) | Acc];
            (_, _, Acc) ->
                Acc
        end,
        [],
        Props
    ),
    {StdProperties, MiscProperties}.

asn1_encode_misc(header, Key, Val) ->
    {header, #'MiscProperty'{
        key = term_to_binary(Key), value = term_to_binary(Val)
    }};
asn1_encode_misc(property, Key, Val) ->
    {property, #'MiscProperty'{
        key = term_to_binary(Key), value = term_to_binary(Val)
    }};
asn1_encode_misc(flag, Key, Val) ->
    {flag, #'MiscFlag'{
        key = atom_to_binary(Key, utf8), value = Val
    }}.

asn1_std_prop(Key, Map) ->
    case Map of
        #{Key := Val} -> Val;
        _ -> asn1_NOVALUE
    end.

asn1_decode_headers(
    #'StdHeaders'{
        protoVer = ProtoVer, peerhost = Peerhost, peername = Peername, username = Username
    },
    StdProperties
) ->
    M0 = asn1_decode_properties(StdProperties),
    M1 =
        case ProtoVer of
            asn1_NOVALUE -> M0;
            {Protocol, Ver} -> M0#{protocol => Protocol, proto_ver => Ver}
        end,
    M2 = asn1_add_optional(peername, decode_ip_port(16, Peername), M1),
    M3 =
        case decode_ip_port(0, Peerhost) of
            asn1_NOVALUE -> M2;
            {PeerIP, _} -> M2#{peerhost => PeerIP}
        end,
    asn1_add_optional(username, Username, M3).

asn1_decode_properties(#'StdProperties'{
    payloadFormatIndicator = PFI,
    userProperty = UP,
    messageExpiryInterval = MEI,
    responseTopic = RT,
    correlationData = CD,
    contentType = CT
}) ->
    M0 =
        case [{K, V} || #'UserProperty'{key = K, value = V} <- UP] of
            [] -> #{};
            UserProps -> #{'User-Property' => UserProps}
        end,
    M1 = asn1_add_optional('Payload-Format-Indicator', PFI, M0),
    M2 = asn1_add_optional('Message-Expiry-Interval', MEI, M1),
    M3 = asn1_add_optional('Response-Topic', RT, M2),
    M4 = asn1_add_optional('Correlation-Data', CD, M3),
    M5 = asn1_add_optional('Content-Type', CT, M4),
    case maps:size(M5) of
        0 -> #{};
        _ -> #{properties => M5}
    end.

asn1_add_optional(_Key, asn1_NOVALUE, Acc) -> Acc;
asn1_add_optional(Key, Val, Acc) -> maps:put(Key, Val, Acc).

-define(IS_VER(V), is_integer(V), V >= 0, V =< 255).

asn1_encode_proto_ver(#{protocol := mqtt, proto_ver := V}) when ?IS_VER(V) ->
    {mqtt, V};
asn1_encode_proto_ver(#{protocol := 'mqtt-sn', proto_ver := V}) when ?IS_VER(V) ->
    {'mqtt-sn', V};
asn1_encode_proto_ver(#{protocol := coap, proto_ver := V}) when ?IS_VER(V) ->
    {coap, V};
asn1_encode_proto_ver(_) ->
    asn1_NOVALUE.

-undef(IS_VER).

asn1_deserialize_misc(asn1_NOVALUE, Message) ->
    Message;
asn1_deserialize_misc(MiscData, Message0) ->
    lists:foldl(
        fun
            ({flag, #'MiscFlag'{key = Key, value = Val}}, Acc) ->
                Flags = maps:put(binary_to_atom(Key, utf8), Val, Acc#message.flags),
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
                    #{binary_to_term(Key) => binary_to_term(Val)},
                    Headers0
                ),
                Acc#message{headers = Headers};
            ({clientAttr, #'ClientAttr'{key = Key, value = Val}}, Acc) ->
                #message{headers = Headers0} = Acc,
                Headers = maps:update_with(
                    client_attrs,
                    fun(Props) ->
                        maps:put(Key, Val, Props)
                    end,
                    #{Key => Val},
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

encode_ip_port(PortSize, {A0, A1, A2, A3}, Port) ->
    <<A0:8, A1:8, A2:8, A3:8, Port:PortSize>>;
encode_ip_port(PortSize, {A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, AA, AB, AC, AD, AE, AF}, Port) ->
    <<A0:8, A1:8, A2:8, A3:8, A4:8, A5:8, A6:8, A7:8, A8:8, A9:8, AA:8, AB:8, AC:8, AD:8, AE:8,
        AF:8, Port:PortSize>>;
encode_ip_port(_, _, _) ->
    asn1_NOVALUE.

decode_ip_port(PortSize, Blob) ->
    case Blob of
        <<A0:8, A1:8, A2:8, A3:8, Port:PortSize>> ->
            {{A0, A1, A2, A3}, Port};
        <<A0:8, A1:8, A2:8, A3:8, A4:8, A5:8, A6:8, A7:8, A8:8, A9:8, AA:8, AB:8, AC:8, AD:8, AE:8,
            AF:8, Port:PortSize>> ->
            {{A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, AA, AB, AC, AD, AE, AF}, Port};
        _ ->
            asn1_NOVALUE
    end.

-ifdef(TEST).

test_messages() ->
    [
        #message{
            id = <<"message_id_val">>,
            qos = 2,
            from = <<"from_val">>,
            flags = #{sys => true, dup => true},
            topic = <<"topic/value">>,
            payload = [<<"foo">>, <<"bar">>],
            timestamp = 42424242,
            extra = #{}
        },
        #message{
            id = <<0, 6, 28, 54, 12, 158, 221, 191, 244, 69, 0, 0, 13, 214, 0, 3>>,
            qos = 0,
            from = <<"MzE3MjU5NzA4NDY3MzcwNzg0NDYxNzI5NDg0NDk4NTM0NDA">>,
            flags = #{dup => true, retain => true, sys => true},
            headers = #{
                peername => {{127, 0, 0, 1}, 34560},
                protocol => mqtt,
                username => <<"foobar">>,
                proto_ver => 5,
                peerhost => {1, 1, 1, 1},
                properties =>
                    #{
                        'Content-Type' => <<"text/json">>,
                        'User-Property' => [{<<"foo">>, <<"bar">>}, {<<"baz">>, <<"quux">>}],
                        'Message-Expiry-Interval' => 10001,
                        'Payload-Format-Indicator' => 1
                    }
            },
            topic = <<"foo/bar">>,
            payload = <<"foo">>,
            timestamp = 1719868325813,
            extra = #{}
        },
        #message{
            id = <<>>,
            from = undefined,
            flags = #{other_flag => true},
            headers = #{
                properties =>
                    #{
                        'Payload-Format-Indicator' => 1,
                        'Message-Expiry-Interval' => 1 bsl 32 - 1,
                        'Response-Topic' => <<"foo/bar/baz">>,
                        'Correlation-Data' => <<"correlation data">>,
                        'Content-Type' => <<"text/json">>,
                        'User-Property' => [{<<"foo">>, <<"bar">>}, {<<"baz">>, <<"quux">>}],
                        junk => garbage,
                        {34, 33, 2} => more_garbage
                    },
                junk => garbage
            },
            topic = <<"foo/bar">>,
            payload = <<"foo">>,
            timestamp = 171986,
            extra = #{}
        },
        #message{
            id = <<>>,
            from = undefined,
            headers = #{
                protocol => "some_protocol",
                proto_ver => 42,
                peername => "some.fancy.peername:222",
                peerhost => "some.fancy.peerhost"
            },
            topic = <<"foo/bar">>,
            payload = <<"foo">>,
            timestamp = 171986,
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
