%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_frame).

-behaviour(emqx_gateway_frame).

%% emqx_gateway_frame callbacks
-export([
    initial_parse_state/1,
    serialize_opts/0,
    serialize_pkt/2,
    parse/2,
    format/1,
    type/1,
    is_message/1
]).

-include("emqx_coap.hrl").
-include_lib("emqx/include/types.hrl").

-define(VERSION, 1).

-define(OPTION_IF_MATCH, 1).
-define(OPTION_URI_HOST, 3).
-define(OPTION_ETAG, 4).
-define(OPTION_IF_NONE_MATCH, 5).
% draft-ietf-core-observe-16
-define(OPTION_OBSERVE, 6).
-define(OPTION_URI_PORT, 7).
-define(OPTION_LOCATION_PATH, 8).
-define(OPTION_URI_PATH, 11).
-define(OPTION_CONTENT_FORMAT, 12).
-define(OPTION_MAX_AGE, 14).
-define(OPTION_URI_QUERY, 15).
-define(OPTION_ACCEPT, 17).
-define(OPTION_LOCATION_QUERY, 20).
% draft-ietf-core-block-17
-define(OPTION_BLOCK2, 23).
-define(OPTION_BLOCK1, 27).
-define(OPTION_PROXY_URI, 35).
-define(OPTION_PROXY_SCHEME, 39).
-define(OPTION_SIZE1, 60).

-elvis([{elvis_style, no_if_expression, disable}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec initial_parse_state(map()) -> emqx_gateway_frame:parse_state().
initial_parse_state(_) ->
    #{}.

-spec serialize_opts() -> emqx_gateway_frame:serialize_options().
serialize_opts() ->
    #{}.

%%--------------------------------------------------------------------
%% serialize_pkt
%%--------------------------------------------------------------------
%% empty message
serialize_pkt(#coap_message{type = Type, method = undefined, id = MsgId}, _Opts) ->
    <<?VERSION:2, (encode_type(Type)):2, 0:4, 0:3, 0:5, MsgId:16>>;
serialize_pkt(
    #coap_message{
        type = Type,
        method = Method,
        id = MsgId,
        token = Token,
        options = Options,
        payload = Payload
    },
    _Opts
) ->
    TKL = byte_size(Token),
    %% RFC 7252 Section 3: Token Length (0-8). 9-15 MUST NOT be sent.
    case TKL > 8 of
        true -> erlang:throw({bad_token, token_too_long});
        false -> ok
    end,
    {Class, Code} = method_to_class_code(Method),
    Head =
        <<?VERSION:2, (encode_type(Type)):2, TKL:4, Class:3, Code:5, MsgId:16, Token:TKL/binary>>,
    FlatOpts = flatten_options(Options),
    encode_option_list(FlatOpts, 0, Head, Payload).

-spec encode_type(message_type()) -> 0..3.
encode_type(con) -> 0;
encode_type(non) -> 1;
encode_type(ack) -> 2;
encode_type(reset) -> 3.

flatten_options(Opts) ->
    flatten_options(maps:to_list(Opts), []).

flatten_options([{_OptId, undefined} | T], Acc) ->
    flatten_options(T, Acc);
flatten_options([{OptId, OptVal} | T], Acc) ->
    flatten_options(
        T,
        case is_repeatable_option(OptId) of
            false ->
                [encode_option(OptId, OptVal) | Acc];
            _ ->
                try_encode_repeatable(OptId, OptVal) ++ Acc
        end
    );
flatten_options([], Acc) ->
    %% sort by option id for calculate the deltas
    lists:keysort(1, Acc).

encode_option_list([{OptNum, OptVal} | OptionList], LastNum, Acc, Payload) ->
    NumDiff = OptNum - LastNum,
    {Delta, ExtNum} =
        if
            NumDiff >= 269 ->
                {14, <<(NumDiff - 269):16>>};
            OptNum - LastNum >= 13 ->
                {13, <<(NumDiff - 13)>>};
            true ->
                {NumDiff, <<>>}
        end,
    Binaryize = byte_size(OptVal),
    {Len, ExtLen} =
        if
            Binaryize >= 269 ->
                {14, <<(Binaryize - 269):16>>};
            Binaryize >= 13 ->
                {13, <<(Binaryize - 13)>>};
            true ->
                {Binaryize, <<>>}
        end,
    Acc2 = <<Acc/binary, Delta:4, Len:4, ExtNum/binary, ExtLen/binary, OptVal/binary>>,
    encode_option_list(OptionList, OptNum, Acc2, Payload);
encode_option_list([], _LastNum, Acc, <<>>) ->
    Acc;
encode_option_list([], _, Acc, Payload) ->
    <<Acc/binary, 16#FF, Payload/binary>>.

try_encode_repeatable(uri_query, Val) when is_map(Val) ->
    maps:fold(
        fun(K, V, Acc) ->
            [encode_option(uri_query, <<K/binary, $=, V/binary>>) | Acc]
        end,
        [],
        Val
    );
try_encode_repeatable(K, Val) ->
    lists:foldr(
        fun
            (undefined, Acc) ->
                Acc;
            (E, Acc) ->
                [encode_option(K, E) | Acc]
        end,
        [],
        Val
    ).

%% RFC 7252
encode_option(if_match, OptVal) ->
    {?OPTION_IF_MATCH, OptVal};
encode_option(uri_host, OptVal) ->
    {?OPTION_URI_HOST, OptVal};
encode_option(etag, OptVal) ->
    {?OPTION_ETAG, OptVal};
encode_option(if_none_match, true) ->
    {?OPTION_IF_NONE_MATCH, <<>>};
encode_option(uri_port, OptVal) ->
    {?OPTION_URI_PORT, binary:encode_unsigned(OptVal)};
encode_option(location_path, OptVal) ->
    {?OPTION_LOCATION_PATH, OptVal};
encode_option(uri_path, OptVal) ->
    {?OPTION_URI_PATH, OptVal};
encode_option(content_format, OptVal) when is_integer(OptVal) ->
    {?OPTION_CONTENT_FORMAT, binary:encode_unsigned(OptVal)};
encode_option(content_format, OptVal) ->
    Num = content_format_to_code(OptVal),
    {?OPTION_CONTENT_FORMAT, binary:encode_unsigned(Num)};
encode_option(max_age, OptVal) ->
    {?OPTION_MAX_AGE, binary:encode_unsigned(OptVal)};
encode_option(uri_query, OptVal) ->
    {?OPTION_URI_QUERY, OptVal};
encode_option('accept', OptVal) ->
    {?OPTION_ACCEPT, binary:encode_unsigned(OptVal)};
encode_option(location_query, OptVal) ->
    {?OPTION_LOCATION_QUERY, OptVal};
encode_option(proxy_uri, OptVal) ->
    {?OPTION_PROXY_URI, OptVal};
encode_option(proxy_scheme, OptVal) ->
    {?OPTION_PROXY_SCHEME, OptVal};
encode_option(size1, OptVal) ->
    {?OPTION_SIZE1, binary:encode_unsigned(OptVal)};
encode_option(observe, OptVal) ->
    {?OPTION_OBSERVE, binary:encode_unsigned(OptVal)};
encode_option(block2, OptVal) ->
    {?OPTION_BLOCK2, encode_block(OptVal)};
encode_option(block1, OptVal) ->
    {?OPTION_BLOCK1, encode_block(OptVal)};
encode_option(OptNum, OptVal) when is_integer(OptNum), OptNum >= 0 ->
    {OptNum, OptVal};
encode_option(Option, Value) ->
    erlang:throw({bad_option, Option, Value}).

encode_block({Num, More, Size}) ->
    %% RFC 7959 Section 2: SZX MUST be 0..6 (block size 16..1024).
    case is_valid_block_size(Size) of
        true ->
            encode_block1(
                Num,
                (if
                    More -> 1;
                    true -> 0
                end),
                trunc(math:log2(Size)) - 4
            );
        false ->
            erlang:throw({bad_block, invalid_size})
    end.
encode_block1(Num, M, SizEx) when Num < 16 -> <<Num:4, M:1, SizEx:3>>;
encode_block1(Num, M, SizEx) when Num < 4096 -> <<Num:12, M:1, SizEx:3>>;
encode_block1(Num, M, SizEx) -> <<Num:28, M:1, SizEx:3>>.

is_valid_block_size(Size) when is_integer(Size) ->
    Size >= 16 andalso Size =< 1024 andalso (Size band (Size - 1)) =:= 0;
is_valid_block_size(_) ->
    false.
content_format_to_code(<<"text/plain">>) -> 0;
content_format_to_code(<<"application/link-format">>) -> 40;
content_format_to_code(<<"application/xml">>) -> 41;
content_format_to_code(<<"application/octet-stream">>) -> 42;
content_format_to_code(<<"application/exi">>) -> 47;
content_format_to_code(<<"application/json">>) -> 50;
content_format_to_code(<<"application/cbor">>) -> 60;
content_format_to_code(<<"application/vnd.oma.lwm2m+tlv">>) -> 11542;
content_format_to_code(<<"application/vnd.oma.lwm2m+json">>) -> 11543;
content_format_to_code(_) -> 42.
method_to_class_code(get) -> {0, 01};
method_to_class_code(post) -> {0, 02};
method_to_class_code(put) -> {0, 03};
method_to_class_code(delete) -> {0, 04};
method_to_class_code({ok, created}) -> {2, 01};
method_to_class_code({ok, deleted}) -> {2, 02};
method_to_class_code({ok, valid}) -> {2, 03};
method_to_class_code({ok, changed}) -> {2, 04};
method_to_class_code({ok, content}) -> {2, 05};
method_to_class_code({ok, nocontent}) -> {2, 07};
method_to_class_code({ok, continue}) -> {2, 31};
method_to_class_code({error, bad_request}) -> {4, 00};
method_to_class_code({error, unauthorized}) -> {4, 01};
method_to_class_code({error, bad_option}) -> {4, 02};
method_to_class_code({error, forbidden}) -> {4, 03};
method_to_class_code({error, not_found}) -> {4, 04};
method_to_class_code({error, method_not_allowed}) -> {4, 05};
method_to_class_code({error, not_acceptable}) -> {4, 06};
method_to_class_code({error, request_entity_incomplete}) -> {4, 08};
method_to_class_code({error, precondition_failed}) -> {4, 12};
method_to_class_code({error, request_entity_too_large}) -> {4, 13};
method_to_class_code({error, unsupported_content_format}) -> {4, 15};
method_to_class_code({error, internal_server_error}) -> {5, 00};
method_to_class_code({error, not_implemented}) -> {5, 01};
method_to_class_code({error, bad_gateway}) -> {5, 02};
method_to_class_code({error, service_unavailable}) -> {5, 03};
method_to_class_code({error, gateway_timeout}) -> {5, 04};
method_to_class_code({error, proxying_not_supported}) -> {5, 05};
method_to_class_code(Method) -> erlang:throw({bad_method, Method}).

%%--------------------------------------------------------------------
%% parse
%%--------------------------------------------------------------------

-spec parse(binary(), emqx_gateway_frame:parse_state()) ->
    emqx_gateway_frame:parse_result().
parse(<<Ver:2, TypeBits:2, TKL:4, Class:3, Code:5, MsgId:16, Rest/binary>>, ParseState) ->
    case Ver of
        ?VERSION ->
            parse_v1(TypeBits, TKL, Class, Code, MsgId, Rest, ParseState);
        _ ->
            %% RFC 7252 Section 3: unknown version MUST be silently ignored.
            {ok, {coap_ignore, {unknown_version, Ver}}, <<>>, ParseState}
    end;
parse(_, ParseState) ->
    {more, ParseState}.

parse_v1(TypeBits, TKL, Class, Code, MsgId, Rest, ParseState) ->
    Type = decode_type(TypeBits),
    case {Class, Code} of
        {0, 0} ->
            %% RFC 7252 Section 4.2: empty message must have no token/options/payload.
            case TKL =:= 0 andalso Rest =:= <<>> of
                true ->
                    {ok, #coap_message{type = Type, id = MsgId}, <<>>, ParseState};
                false ->
                    {ok, {coap_format_error, Type, MsgId, empty_message_with_data}, <<>>,
                        ParseState}
            end;
        _ ->
            parse_non_empty(Type, TKL, Class, Code, MsgId, Rest, ParseState)
    end.

parse_non_empty(Type, TKL, Class, Code, MsgId, Rest, ParseState) ->
    case is_supported_class(Class) of
        false ->
            %% RFC 7252 Section 4.2: reserved class is a message format error.
            {ok, {coap_format_error, Type, MsgId, reserved_class}, <<>>, ParseState};
        true ->
            case TKL > 8 of
                true ->
                    %% RFC 7252 Section 3: Token Length 9-15 is a format error.
                    {ok, {coap_format_error, Type, MsgId, invalid_tkl}, <<>>, ParseState};
                false ->
                    parse_token(Type, Class, Code, MsgId, Rest, TKL, ParseState)
            end
    end.

parse_token(Type, Class, Code, MsgId, Rest, TKL, ParseState) ->
    case Rest of
        <<Token:TKL/binary, Tail/binary>> ->
            parse_options(Type, Class, Code, MsgId, Token, Tail, ParseState);
        _ ->
            {ok, {coap_format_error, Type, MsgId, truncated_token}, <<>>, ParseState}
    end.

parse_options(Type, Class, Code, MsgId, Token, Tail, ParseState) ->
    case decode_option_list(Tail) of
        {ok, Options, Payload} ->
            parse_method(Type, Class, Code, MsgId, Token, Options, Payload, ParseState);
        {error, Reason} ->
            parse_option_error(Type, Class, MsgId, Token, Reason, ParseState)
    end.

parse_method(Type, Class, Code, MsgId, Token, Options, Payload, ParseState) ->
    case class_code_to_method_result(Class, Code) of
        {ok, Method} ->
            Options2 = maps:fold(
                fun(K, V, Acc) -> Acc#{K => get_option_val(K, V)} end, #{}, Options
            ),
            {ok,
                #coap_message{
                    type = Type,
                    method = Method,
                    id = MsgId,
                    token = Token,
                    options = Options2,
                    payload = Payload
                },
                <<>>, ParseState};
        {error, bad_method} ->
            %% RFC 7252 Section 5.8: unknown request method -> 4.05.
            Req = #coap_message{type = Type, id = MsgId, token = Token},
            {ok, {coap_request_error, Req, {error, method_not_allowed}}, <<>>, ParseState};
        {error, unknown_response} ->
            {ok, {coap_ignore, {unknown_response, Class, Code}}, <<>>, ParseState}
    end.

parse_option_error(Type, Class, MsgId, Token, Reason, ParseState) ->
    case classify_option_error(Reason, Class) of
        {request_error, ErrorCode} ->
            Req = #coap_message{type = Type, id = MsgId, token = Token},
            {ok, {coap_request_error, Req, {error, ErrorCode}}, <<>>, ParseState};
        format_error ->
            {ok, {coap_format_error, Type, MsgId, Reason}, <<>>, ParseState}
    end.

get_option_val(uri_query, V) ->
    KVList = lists:foldl(fun split_uri_query/2, [], V),
    maps:from_list(KVList);
get_option_val(K, V) ->
    case is_repeatable_option(K) of
        true -> lists:reverse(V);
        _ -> V
    end.
split_uri_query(Entry, Acc) ->
    case re:split(Entry, "=") of
        [Key, Val] -> [{Key, Val} | Acc];
        _ -> Acc
    end.

-spec decode_type(0..3) -> message_type().
decode_type(0) -> con;
decode_type(1) -> non;
decode_type(2) -> ack;
decode_type(3) -> reset.

-spec decode_option_list(binary()) ->
    {ok, message_options(), binary()} | {error, term()}.
decode_option_list(Bin) ->
    decode_option_list(Bin, 0, #{}).

decode_option_list(<<>>, _OptNum, OptMap) ->
    {ok, OptMap, <<>>};
decode_option_list(<<16#FF, Payload/binary>>, _OptNum, OptMap) ->
    %% RFC 7252 Section 3: payload marker with empty payload is a format error.
    case Payload of
        <<>> -> {error, payload_marker_empty};
        _ -> {ok, OptMap, Payload}
    end;
decode_option_list(<<Delta:4, Len:4, Bin/binary>>, OptNum, OptMap) ->
    case Delta of
        15 ->
            %% RFC 7252 Section 3.1: delta 15 is reserved.
            {error, option_delta_reserved};
        Any when Any < 13 -> decode_option_len(Bin, OptNum + Delta, Len, OptMap);
        13 ->
            case Bin of
                <<ExtOptNum, NewBin/binary>> ->
                    decode_option_len(NewBin, OptNum + ExtOptNum + 13, Len, OptMap);
                _ ->
                    {error, option_ext_delta_truncated}
            end;
        14 ->
            case Bin of
                <<ExtOptNum:16, NewBin/binary>> ->
                    decode_option_len(NewBin, OptNum + ExtOptNum + 269, Len, OptMap);
                _ ->
                    {error, option_ext_delta_truncated}
            end
    end.

decode_option_len(Bin, OptNum, Len, OptMap) when Len < 13 ->
    decode_option_value(Bin, OptNum, Len, OptMap);
decode_option_len(<<ExtOptLen, NewBin/binary>>, OptNum, 13, OptMap) ->
    decode_option_value(NewBin, OptNum, ExtOptLen + 13, OptMap);
decode_option_len(<<ExtOptLen:16, NewBin/binary>>, OptNum, 14, OptMap) ->
    decode_option_value(NewBin, OptNum, ExtOptLen + 269, OptMap);
decode_option_len(_, _OptNum, 15, _OptMap) ->
    %% RFC 7252 Section 3.1: length 15 is reserved.
    {error, option_length_reserved};
decode_option_len(_, _OptNum, _, _OptMap) ->
    {error, option_ext_len_truncated}.

decode_option_value(<<Bin/binary>>, OptNum, OptLen, OptMap) ->
    case Bin of
        <<OptVal:OptLen/binary, NewBin/binary>> ->
            case append_option(OptNum, OptVal, OptMap) of
                {ok, OptMap2} -> decode_option_list(NewBin, OptNum, OptMap2);
                {error, Reason} -> {error, Reason}
            end;
        _ ->
            {error, option_value_truncated}
    end.

append_option(OptNum, RawOptVal, OptMap) ->
    case decode_option(OptNum, RawOptVal) of
        {ok, OptId, OptVal} ->
            case is_repeatable_option(OptId) of
                false ->
                    case maps:is_key(OptId, OptMap) of
                        false ->
                            {ok, OptMap#{OptId => OptVal}};
                        true ->
                            case is_critical_option(OptNum) of
                                true -> {error, duplicate_critical_option};
                                false -> {ok, OptMap}
                            end
                    end;
                true ->
                    {ok, add_repeatable_option(OptId, OptVal, OptMap)}
            end;
        {ignore, _OptNum} ->
            {ok, OptMap};
        {error, Reason} ->
            {error, Reason}
    end.
add_repeatable_option(OptId, OptVal, OptMap) ->
    case maps:get(OptId, OptMap, undefined) of
        undefined -> OptMap#{OptId => [OptVal]};
        OptVals -> OptMap#{OptId => [OptVal | OptVals]}
    end.

decode_option(?OPTION_IF_MATCH, OptVal) ->
    {ok, if_match, OptVal};
decode_option(?OPTION_URI_HOST, OptVal) ->
    {ok, uri_host, OptVal};
decode_option(?OPTION_ETAG, OptVal) ->
    {ok, etag, OptVal};
decode_option(?OPTION_IF_NONE_MATCH, <<>>) ->
    {ok, if_none_match, true};
decode_option(?OPTION_IF_NONE_MATCH, _OptVal) ->
    %% RFC 7252 Section 5.10.8.2: If-None-Match MUST be empty.
    {error, invalid_if_none_match};
decode_option(?OPTION_URI_PORT, OptVal) ->
    {ok, uri_port, binary:decode_unsigned(OptVal)};
decode_option(?OPTION_LOCATION_PATH, OptVal) ->
    {ok, location_path, OptVal};
decode_option(?OPTION_URI_PATH, OptVal) ->
    {ok, uri_path, OptVal};
decode_option(?OPTION_CONTENT_FORMAT, OptVal) ->
    Num = binary:decode_unsigned(OptVal),
    {ok, content_format, content_code_to_format(Num)};
decode_option(?OPTION_MAX_AGE, OptVal) ->
    {ok, max_age, binary:decode_unsigned(OptVal)};
decode_option(?OPTION_URI_QUERY, OptVal) ->
    {ok, uri_query, OptVal};
decode_option(?OPTION_ACCEPT, OptVal) ->
    {ok, 'accept', binary:decode_unsigned(OptVal)};
decode_option(?OPTION_LOCATION_QUERY, OptVal) ->
    {ok, location_query, OptVal};
decode_option(?OPTION_PROXY_URI, OptVal) ->
    {ok, proxy_uri, OptVal};
decode_option(?OPTION_PROXY_SCHEME, OptVal) ->
    {ok, proxy_scheme, OptVal};
decode_option(?OPTION_SIZE1, OptVal) ->
    {ok, size1, binary:decode_unsigned(OptVal)};
decode_option(?OPTION_OBSERVE, OptVal) ->
    {ok, observe, binary:decode_unsigned(OptVal)};
decode_option(?OPTION_BLOCK2, OptVal) ->
    decode_block_option(block2, OptVal);
decode_option(?OPTION_BLOCK1, OptVal) ->
    decode_block_option(block1, OptVal);
decode_option(OptNum, _OptVal) when is_integer(OptNum) ->
    case is_critical_option(OptNum) of
        true ->
            %% RFC 7252 Section 5.4.1: unknown critical options -> 4.02.
            {error, unknown_critical_option};
        false ->
            %% RFC 7252 Section 5.4.1: unknown elective options MUST be ignored.
            {ignore, OptNum}
    end.

decode_block_option(Name, OptVal) ->
    case decode_block(OptVal) of
        {ok, Block} -> {ok, Name, Block};
        {error, Reason} -> {error, Reason}
    end.

decode_block(<<>>) ->
    decode_block1(0, 0, 0);
decode_block(<<Num:4, M:1, SizEx:3>>) ->
    decode_block1(Num, M, SizEx);
decode_block(<<Num:12, M:1, SizEx:3>>) ->
    decode_block1(Num, M, SizEx);
decode_block(<<Num:28, M:1, SizEx:3>>) ->
    decode_block1(Num, M, SizEx);
decode_block(_) ->
    {error, invalid_block_size}.
decode_block1(_Num, _M, 7) ->
    %% RFC 7959 Section 2: SZX=7 is reserved and must be rejected.
    {error, invalid_block_size};
decode_block1(Num, M, SizEx) ->
    {ok, {Num, M =/= 0, 1 bsl (SizEx + 4)}}.
content_code_to_format(0) -> <<"text/plain">>;
content_code_to_format(40) -> <<"application/link-format">>;
content_code_to_format(41) -> <<"application/xml">>;
content_code_to_format(42) -> <<"application/octet-stream">>;
content_code_to_format(47) -> <<"application/exi">>;
content_code_to_format(50) -> <<"application/json">>;
content_code_to_format(60) -> <<"application/cbor">>;
content_code_to_format(11542) -> <<"application/vnd.oma.lwm2m+tlv">>;
content_code_to_format(11543) -> <<"application/vnd.oma.lwm2m+json">>;
content_code_to_format(_) -> <<"application/octet-stream">>.
class_code_to_method({0, 01}) -> get;
class_code_to_method({0, 02}) -> post;
class_code_to_method({0, 03}) -> put;
class_code_to_method({0, 04}) -> delete;
class_code_to_method({2, 01}) -> {ok, created};
class_code_to_method({2, 02}) -> {ok, deleted};
class_code_to_method({2, 03}) -> {ok, valid};
class_code_to_method({2, 04}) -> {ok, changed};
class_code_to_method({2, 05}) -> {ok, content};
class_code_to_method({2, 07}) -> {ok, nocontent};
class_code_to_method({2, 31}) -> {ok, continue};
class_code_to_method({4, 00}) -> {error, bad_request};
class_code_to_method({4, 01}) -> {error, unauthorized};
class_code_to_method({4, 02}) -> {error, bad_option};
class_code_to_method({4, 03}) -> {error, forbidden};
class_code_to_method({4, 04}) -> {error, not_found};
class_code_to_method({4, 05}) -> {error, method_not_allowed};
class_code_to_method({4, 06}) -> {error, not_acceptable};
class_code_to_method({4, 08}) -> {error, request_entity_incomplete};
class_code_to_method({4, 12}) -> {error, precondition_failed};
class_code_to_method({4, 13}) -> {error, request_entity_too_large};
class_code_to_method({4, 15}) -> {error, unsupported_content_format};
class_code_to_method({5, 00}) -> {error, internal_server_error};
class_code_to_method({5, 01}) -> {error, not_implemented};
class_code_to_method({5, 02}) -> {error, bad_gateway};
class_code_to_method({5, 03}) -> {error, service_unavailable};
class_code_to_method({5, 04}) -> {error, gateway_timeout};
class_code_to_method({5, 05}) -> {error, proxying_not_supported};
class_code_to_method(_) -> undefined.

format(Msg) -> io_lib:format("~p", [Msg]).

type(#coap_message{}) -> coap;
type({coap_ignore, _}) -> undefined;
type({coap_format_error, _, _, _}) -> undefined;
type({coap_request_error, _, _}) -> undefined;
type(_) -> coap.

is_message(#coap_message{}) -> true;
is_message(_) -> false.
is_repeatable_option(if_match) -> true;
is_repeatable_option(etag) -> true;
is_repeatable_option(location_path) -> true;
is_repeatable_option(uri_path) -> true;
is_repeatable_option(uri_query) -> true;
is_repeatable_option(location_query) -> true;
is_repeatable_option(_) -> false.

%% RFC 7252 Section 3: only classes 0,2,4,5 are defined; others are reserved.
is_supported_class(0) -> true;
is_supported_class(2) -> true;
is_supported_class(4) -> true;
is_supported_class(5) -> true;
is_supported_class(_) -> false.

%% RFC 7252 Section 5.8: unknown request methods are treated as 4.05.
class_code_to_method_result(0, Code) ->
    case class_code_to_method({0, Code}) of
        undefined -> {error, bad_method};
        Method -> {ok, Method}
    end;
class_code_to_method_result(Class, Code) when Class =:= 2; Class =:= 4; Class =:= 5 ->
    case class_code_to_method({Class, Code}) of
        undefined -> {error, unknown_response};
        Method -> {ok, Method}
    end.

%% RFC 7252 Section 5.4.1: critical options are odd-numbered.
is_critical_option(OptNum) when is_integer(OptNum) ->
    (OptNum band 1) =:= 1.

%% RFC 7252 Section 5.4.1/5.8: map option errors to request or format errors.
classify_option_error(Reason, Class) when Class =:= 0 ->
    case Reason of
        unknown_critical_option -> {request_error, bad_option};
        duplicate_critical_option -> {request_error, bad_option};
        invalid_if_none_match -> {request_error, bad_option};
        invalid_block_size -> {request_error, bad_request};
        _ -> format_error
    end;
classify_option_error(_Reason, _Class) ->
    format_error.
