%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggreg_json_lines_tests).

-include_lib("eunit/include/eunit.hrl").

-export([decode/1]).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

new_opts() ->
    #{}.

fill_close(Records, JSONL) ->
    {Output, JSONLFinal} = emqx_connector_aggreg_json_lines:fill(Records, JSONL),
    Trailer = emqx_connector_aggreg_json_lines:close(JSONLFinal),
    iolist_to_binary([Output, Trailer]).

decode(Binary) ->
    do_decode(Binary, []).

do_decode(Binary, Acc) ->
    case jiffy:decode(Binary, [return_maps, return_trailer]) of
        {has_trailer, Term, Rest} ->
            do_decode(Rest, [Term | Acc]);
        Term ->
            lists:reverse([Term | Acc])
    end.

roundtrip(Records, JSONL) ->
    Binary = fill_close(Records, JSONL),
    decode(Binary).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

roundtrip_test() ->
    JSONL = emqx_connector_aggreg_json_lines:new(new_opts()),
    ?assertMatch(
        %% TODO: Make `jiffy` respect negative zeros.
        R when
            R ==
                [
                    #{<<"A">> => 1.2345, <<"B">> => "string", <<"Ç"/utf8>> => +0.0},
                    #{<<"A">> => 1 / 3, <<"B">> => "[]", <<"Ç"/utf8>> => -0.0},
                    #{<<"A">> => 111111, <<"B">> => "🫠", <<"Ç"/utf8>> => 0.0},
                    #{<<"A">> => 111.111, <<"B">> => "\"quoted\"", <<"Ç"/utf8>> => "line\r\nbreak"},
                    #{
                        <<"A">> => 111.111,
                        <<"B">> => "\"quoted\"",
                        <<"Ç"/utf8>> => <<"line\r\nbreak">>
                    },
                    #{<<"A">> => 222.222, <<"B">> => "", <<"Ç"/utf8>> => <<"undefined">>},
                    #{
                        <<"A">> => <<"atom">>,
                        <<"B">> => #{<<"nested">> => <<"struct">>},
                        <<"array">> => [<<"a">>, <<"b">>, $C, [], #{}],
                        <<"undefined"/utf8>> => <<"undefined">>,
                        <<"nil">> => <<"nil">>,
                        <<"null">> => null
                    }
                ],
        roundtrip(
            [
                #{<<"A">> => 1.2345, <<"B">> => "string", <<"Ç"/utf8>> => +0.0},
                #{<<"A">> => 1 / 3, <<"B">> => "[]", <<"Ç"/utf8>> => -0.0},
                #{<<"A">> => 111111, <<"B">> => "🫠", <<"Ç"/utf8>> => 0.0},
                #{<<"A">> => 111.111, <<"B">> => "\"quoted\"", <<"Ç"/utf8>> => "line\r\nbreak"},
                #{<<"A">> => 111.111, <<"B">> => "\"quoted\"", <<"Ç"/utf8>> => <<"line\r\nbreak">>},
                #{<<"A">> => 222.222, <<"B">> => "", <<"Ç"/utf8>> => undefined},
                #{
                    <<"A">> => atom,
                    <<"B">> => #{<<"nested">> => struct},
                    <<"array">> => [a, <<"b">>, $C, [], #{}],
                    <<"undefined"/utf8>> => undefined,
                    <<"nil">> => nil,
                    <<"null">> => null
                }
            ],
            JSONL
        )
    ).
