%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_stomp_frame_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_stomp.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

parse_frame(Bytes) ->
    parse_chunks([Bytes]).

parse_chunks(Chunks) ->
    parse_chunks(Chunks, emqx_stomp_frame:initial_parse_state(#{})).

parse_chunks([Chunk | Rest], Parser) ->
    case emqx_stomp_frame:parse(Chunk, Parser) of
        {more, NParser} when Rest =/= [] ->
            parse_chunks(Rest, NParser);
        Result ->
            Result
    end.

send_frame(Destination) ->
    <<"SEND\ndestination:", Destination/binary, "\n\nhi", 0>>.

connect_frame(Passcode) ->
    <<"CONNECT\nlogin:admin\npasscode:", Passcode/binary, "\n\n", 0>>.

destination(#stomp_frame{headers = Headers}) ->
    proplists:get_value(<<"destination">>, Headers).

passcode(#stomp_frame{headers = Headers}) ->
    proplists:get_value(<<"passcode">>, Headers).

%% split the binary right after its first occurrence of Byte
split_after(Byte, Bin) ->
    {Pos, 1} = binary:match(Bin, <<Byte>>),
    <<A:(Pos + 1)/binary, B/binary>> = Bin,
    [A, B].

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "Escaped colon in a header value decodes to a literal colon (#12917).".
t_unescape_colon(_) ->
    {ok, Frame, <<>>, _} = parse_frame(send_frame(<<"a\\cb">>)),
    ?assertEqual(<<"a:b">>, destination(Frame)).

-doc "Escaped backslash in a header value decodes to a literal backslash.".
t_unescape_backslash(_) ->
    {ok, Frame, <<>>, _} = parse_frame(send_frame(<<"a\\\\b">>)),
    ?assertEqual(<<"a\\b">>, destination(Frame)).

-doc "Escaped newline in a header value decodes to a literal line feed.".
t_unescape_newline(_) ->
    {ok, Frame, <<>>, _} = parse_frame(send_frame(<<"a\\nb">>)),
    ?assertEqual(<<"a\nb">>, destination(Frame)).

-doc "Escaped carriage return in a header value decodes to a literal CR.".
t_unescape_cr(_) ->
    {ok, Frame, <<>>, _} = parse_frame(send_frame(<<"a\\rb">>)),
    ?assertEqual(<<"a\rb">>, destination(Frame)).

-doc "A raw colon in a header value still passes through unchanged.".
t_raw_colon_in_value(_) ->
    {ok, Frame, <<>>, _} = parse_frame(send_frame(<<"a:b">>)),
    ?assertEqual(<<"a:b">>, destination(Frame)).

-doc "Escape sequences decode in header names as well as values.".
t_unescape_header_name(_) ->
    {ok, #stomp_frame{headers = Headers}, <<>>, _} =
        parse_frame(<<"SEND\na\\cb:v\n\nhi", 0>>),
    ?assertEqual([{<<"a:b">>, <<"v">>}], Headers).

-doc "An undefined escape sequence such as \\t is a fatal frame error (STOMP 1.2).".
t_invalid_escape_is_fatal(_) ->
    ?assertError({cannot_unescape, <<"\\t">>}, parse_frame(send_frame(<<"a\\tb">>))).

-doc """
Headers of a STOMP-command frame decode escape sequences: the command was added
in STOMP 1.1, so the CONNECT/CONNECTED escaping exemption does not apply to it.
""".
t_stomp_command_unescapes(_) ->
    {ok, #stomp_frame{headers = Headers}, <<>>, _} =
        parse_frame(<<"STOMP\naccept-version:1.2\nlogin:pa\\css\n\n", 0>>),
    ?assertEqual(<<"pa:ss">>, proplists:get_value(<<"login">>, Headers)).

-doc """
CONNECT headers are not unescaped: STOMP 1.2 exempts CONNECT and CONNECTED
frames from header escaping for backward compatibility with STOMP 1.0.
A backslash in a CONNECT passcode is a literal octet, and sequences that are
undefined escapes elsewhere are not an error here.
""".
t_connect_headers_not_unescaped(_) ->
    {ok, Frame1, <<>>, _} = parse_frame(connect_frame(<<"pa\\css">>)),
    ?assertEqual(<<"pa\\css">>, passcode(Frame1)),
    {ok, Frame2, <<>>, _} = parse_frame(connect_frame(<<"pa\\tss">>)),
    ?assertEqual(<<"pa\\tss">>, passcode(Frame2)).

-doc "CONNECTED headers are not unescaped, same as CONNECT (STOMP 1.0 compatibility).".
t_connected_headers_not_unescaped(_) ->
    {ok, #stomp_frame{headers = Headers}, <<>>, _} =
        parse_frame(<<"CONNECTED\nsession:s\\cid\n\n", 0>>),
    ?assertEqual(<<"s\\cid">>, proplists:get_value(<<"session">>, Headers)).

-doc "CRLF line endings are accepted everywhere LF is (STOMP 1.2 EOL = [CR] LF).".
t_crlf_line_endings(_) ->
    {ok, Frame, <<>>, _} =
        parse_frame(<<"CONNECT\r\nlogin:admin\r\npasscode:pa:ss\r\n\r\n", 0>>),
    ?assertEqual(<<"pa:ss">>, passcode(Frame)).

-doc "A standalone CRLF is a heartbeat frame, like a standalone LF.".
t_crlf_heartbeat(_) ->
    ?assertMatch(
        {ok, #stomp_frame{command = ?CMD_HEARTBEAT}, <<>>, _},
        parse_frame(<<"\r\n">>)
    ),
    ?assertMatch(
        {ok, #stomp_frame{command = ?CMD_HEARTBEAT}, <<>>, _},
        parse_chunks([<<"\r">>, <<"\n">>])
    ).

-doc "A CR not followed by LF is a fatal frame error.".
t_bare_cr_is_fatal(_) ->
    ?assertError(linefeed_expected, parse_frame(<<"CONNECT\rX">>)).

-doc "A chunk ending in a bare backslash continues the escape into the next chunk.".
t_split_at_bare_backslash(_) ->
    Chunks = split_after($\\, send_frame(<<"a\\cb">>)),
    ?assertMatch([<<"SEND\ndestination:a\\">>, <<"cb\n\nhi", 0>>], Chunks),
    {ok, Frame, <<>>, _} = parse_chunks(Chunks),
    ?assertEqual(<<"a:b">>, destination(Frame)).

-doc "A CONNECT chunk ending in a bare backslash continues as a literal octet.".
t_connect_split_at_bare_backslash(_) ->
    Chunks = split_after($\\, connect_frame(<<"pa\\css">>)),
    {ok, Frame, <<>>, _} = parse_chunks(Chunks),
    ?assertEqual(<<"pa\\css">>, passcode(Frame)).

-doc "A chunk ending in a bare CR continues correctly into the next chunk.".
t_split_at_bare_cr(_) ->
    Chunks = split_after($\r, <<"CONNECT\r\nlogin:admin\npasscode:pa:ss\n\n", 0>>),
    ?assertMatch([<<"CONNECT\r">>, <<"\nlogin:admin\npasscode:pa:ss\n\n", 0>>], Chunks),
    {ok, Frame, <<>>, _} = parse_chunks(Chunks),
    ?assertEqual(<<"pa:ss">>, passcode(Frame)).

-doc "A frame with CRLF line endings and escapes parses when fed one byte at a time.".
t_parse_byte_by_byte(_) ->
    Frame = <<"SEND\r\ndestination:a\\cb\r\n\r\nhi", 0>>,
    Chunks = [<<B>> || <<B>> <= Frame],
    {ok, Parsed, <<>>, _} = parse_chunks(Chunks),
    ?assertEqual(<<"a:b">>, destination(Parsed)).

-doc "serialize_pkt then parse returns the original headers and body unchanged.".
t_roundtrip(_) ->
    Headers = [
        {<<"destination">>, <<"a:b\\c\rd\ne">>},
        {<<"plain">>, <<"value">>}
    ],
    Body = <<"body">>,
    Frame = emqx_stomp_frame:make(<<"SEND">>, Headers, Body),
    Bin = iolist_to_binary(emqx_stomp_frame:serialize_pkt(Frame, #{})),
    {ok, #stomp_frame{command = Cmd, headers = ParsedHeaders, body = ParsedBody}, <<>>, _} =
        parse_frame(Bin),
    ?assertEqual(<<"SEND">>, Cmd),
    ?assertEqual(Body, ParsedBody),
    ?assertEqual(
        lists:sort(Headers),
        lists:sort(proplists:delete(<<"content-length">>, ParsedHeaders))
    ).

-doc "Escape sequences in the body stay literal; unescaping applies to headers only.".
t_body_not_unescaped(_) ->
    {ok, #stomp_frame{body = Body}, <<>>, _} =
        parse_frame(<<"SEND\ndestination:/queue/a\n\n\\c\\n\\\\", 0>>),
    ?assertEqual(<<"\\c\\n\\\\">>, Body).

-doc "A space after the header colon is part of the value, not a separator.".
t_space_after_colon_preserved(_) ->
    {ok, #stomp_frame{headers = Headers}, <<>>, _} =
        parse_frame(<<"CONNECT\nfoo: bar\n\n", 0>>),
    ?assertEqual([{<<"foo">>, <<" bar">>}], Headers).

-doc "Two frames in one chunk parse one after the other.".
t_pipelined_frames(_) ->
    Bytes = <<"CONNECT\na:1\n\n", 0, "SEND\nb:2\n\nhi", 0>>,
    {ok, Frame1, Rest, Parser} = parse_frame(Bytes),
    ?assertMatch(#stomp_frame{command = <<"CONNECT">>, headers = [{<<"a">>, <<"1">>}]}, Frame1),
    {ok, Frame2, <<>>, _} = emqx_stomp_frame:parse(Rest, Parser),
    ?assertMatch(
        #stomp_frame{command = <<"SEND">>, headers = [{<<"b">>, <<"2">>}], body = <<"hi">>},
        Frame2
    ).
