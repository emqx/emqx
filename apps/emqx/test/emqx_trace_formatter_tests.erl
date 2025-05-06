%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
