%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_trace_formatter_tests).

-include_lib("eunit/include/eunit.hrl").

format_no_tns_in_meta_test() ->
    Meta = #{
        clientid => <<"c">>,
        trace_tag => tag
    },
    Event = #{
        level => debug,
        meta => Meta,
        msg => <<"test_msg">>
    },
    Config = #{payload_fmt_opts => #{payload_encode => hidden}},
    Formatted = format(Event, Config),
    ?assertMatch(nomatch, re:run(Formatted, "tns:")),
    ok.

format_tns_in_meta_test() ->
    Meta = #{
        tns => <<"a">>,
        clientid => <<"c">>,
        trace_tag => tag
    },
    Event = #{
        level => debug,
        meta => Meta,
        msg => <<"test_msg">>
    },
    Config = #{payload_fmt_opts => #{payload_encode => hidden}},
    Formatted = format(Event, Config),
    ?assertMatch({match, _}, re:run(Formatted, "\stns:\sa\s")),
    ok.

format(Event, Config) ->
    unicode:characters_to_binary(emqx_trace_formatter:format(Event, Config)).

-doc "A `data` field in a report is redacted, not a formatter crash (issue 18764).".
textfmt_hidden_data_test() ->
    Data = <<32, 17, 0, 0, 14, 19, 0, 30, 33, 0, 32, 34, 255, 255, 39, 0, 16, 0, 0>>,
    Event = #{
        level => debug,
        msg => {report, #{msg => "recv_data", data => Data}},
        meta => #{time => 0, clientid => <<":routesync:d27dbf0d">>}
    },
    Config = #{payload_encode => hidden, single_line => true, template => [msg, "\n"]},
    Formatted = unicode:characters_to_binary(emqx_logger_textfmt:format(Event, Config)),
    ?assertMatch(nomatch, re:run(Formatted, "FORMATTER CRASH"), #{formatted => Formatted}),
    ?assertMatch(
        {match, _}, re:run(Formatted, "data: Encoded\\(hidden\\)=\\*{6}"), #{formatted => Formatted}
    ),
    ok.

-doc "A trace event with an `input` field shorter than the truncate limit is redacted.".
trace_hidden_input_test() ->
    Event = #{
        level => debug,
        meta => #{clientid => <<"c">>, trace_tag => tag, input => <<"secret">>},
        msg => <<"test_msg">>
    },
    Config = #{payload_fmt_opts => #{payload_encode => hidden}},
    Formatted = format(Event, Config),
    ?assertMatch({match, _}, re:run(Formatted, "input: Encoded\\(hidden\\)=\\*{6}"), #{
        formatted => Formatted
    }),
    ?assertMatch(nomatch, re:run(Formatted, "secret"), #{formatted => Formatted}),
    ok.

-doc "With `hidden`, a short term is replaced by a constant.".
format_term_hidden_short_test() ->
    ?assertEqual(<<"Encoded(hidden)=******">>, hidden_term(<<"short">>)).

-doc "With `hidden`, a term longer than the limit shows no size or truncation count.".
format_term_hidden_long_test() ->
    Term = binary:copy(<<"a">>, 2000),
    ?assertEqual(<<"Encoded(hidden)=******">>, hidden_term(Term)).

-doc "With `hidden`, non-binary terms are replaced by the same constant.".
format_term_hidden_non_binary_test_() ->
    [
        ?_assertEqual(<<"Encoded(hidden)=******">>, hidden_term(T))
     || T <- [atom, 42, 1.5, #{k => v}, [1, 2, 3], "str"]
    ].

-doc "`hex` and `text` keep their output, including truncation.".
format_term_hex_text_test_() ->
    Long = binary:copy(<<"a">>, 2000),
    [
        ?_assertEqual(<<"Encoded(text)=abc">>, fmt_term(<<"abc">>, text)),
        ?_assertEqual(<<"Encoded(hex)=616263">>, fmt_term(<<"abc">>, hex)),
        ?_assertEqual(
            <<"Encoded(text)=", (binary:copy(<<"a">>, 100))/binary, "...(1900 bytes)">>,
            fmt_term(Long, text)
        ),
        ?_assertEqual(
            <<"Encoded(hex)=", (binary:copy(<<"61">>, 100))/binary, "...(1900 bytes)">>,
            fmt_term(Long, hex)
        ),
        %% invalid utf8 falls back to hex
        ?_assertEqual(<<"Encoded(hex)=3C3C22FF223E3E">>, fmt_term(<<255>>, text))
    ].

hidden_term(Term) ->
    fmt_term(Term, hidden).

fmt_term(Term, Encode) ->
    {Formatted, #{payload_encode := _}} =
        emqx_trace_formatter:format_term(Term, #{payload_encode => Encode}),
    iolist_to_binary(Formatted).
