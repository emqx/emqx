%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggreg_csv_tests).

-include_lib("eunit/include/eunit.hrl").

encoding_test() ->
    CSV = emqx_connector_aggreg_csv:new(#{}),
    ?assertEqual(
        "A,B,Ç\n"
        "1.2345,string,0.0\n"
        "0.3333333333,\"[]\",-0.0\n"
        "111111,🫠,0.0\n"
        "111.111,\"\"\"quoted\"\"\",\"line\r\nbreak\"\n"
        "222.222,,\n",
        fill_close(CSV, [
            [
                #{<<"A">> => 1.2345, <<"B">> => "string", <<"Ç"/utf8>> => +0.0},
                #{<<"A">> => 1 / 3, <<"B">> => "[]", <<"Ç"/utf8>> => -0.0},
                #{<<"A">> => 111111, <<"B">> => "🫠", <<"Ç"/utf8>> => 0.0},
                #{<<"A">> => 111.111, <<"B">> => "\"quoted\"", <<"Ç"/utf8>> => "line\r\nbreak"},
                #{<<"A">> => 222.222, <<"B">> => "", <<"Ç"/utf8>> => undefined}
            ]
        ])
    ).

column_order_test() ->
    Order = [<<"ID">>, <<"TS">>],
    CSV = emqx_connector_aggreg_csv:new(#{column_order => Order}),
    ?assertEqual(
        "ID,TS,A,B,D\n"
        "1,2024-01-01,12.34,str,\"[]\"\n"
        "2,2024-01-02,23.45,ing,\n"
        "3,,45,,'\n"
        "4,2024-01-04,,,\n",
        fill_close(CSV, [
            [
                #{
                    <<"A">> => 12.34,
                    <<"B">> => "str",
                    <<"ID">> => 1,
                    <<"TS">> => "2024-01-01",
                    <<"D">> => <<"[]">>
                },
                #{
                    <<"TS">> => "2024-01-02",
                    <<"C">> => <<"null">>,
                    <<"ID">> => 2,
                    <<"A">> => 23.45,
                    <<"B">> => "ing"
                }
            ],
            [
                #{<<"A">> => 45, <<"D">> => <<"'">>, <<"ID">> => 3},
                #{<<"ID">> => 4, <<"TS">> => "2024-01-04"}
            ]
        ])
    ).

fill_close(CSV, LRecords) ->
    string(fill_close_(CSV, LRecords)).

fill_close_(CSV0, [Records | LRest]) ->
    {Writes, CSV} = emqx_connector_aggreg_csv:fill(Records, CSV0),
    [Writes | fill_close_(CSV, LRest)];
fill_close_(CSV, []) ->
    [emqx_connector_aggreg_csv:close(CSV)].

string(Writes) ->
    unicode:characters_to_list(Writes).
