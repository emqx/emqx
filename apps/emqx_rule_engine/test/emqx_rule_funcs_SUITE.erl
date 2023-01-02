%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_funcs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_rule_events, [eventmsg_publish/1]).

-define(PROPTEST(F), ?assert(proper:quickcheck(F()))).
%%-define(PROPTEST(F), ?assert(proper:quickcheck(F(), [{on_output, fun ct:print/2}]))).

%%------------------------------------------------------------------------------
%% Test cases for IoT Funcs
%%------------------------------------------------------------------------------

t_msgid(_) ->
    Msg = message(),
    ?assertEqual(undefined, apply_func(msgid, [], #{})),
    ?assertEqual(emqx_guid:to_hexstr(emqx_message:id(Msg)), apply_func(msgid, [], eventmsg_publish(Msg))).

t_qos(_) ->
    ?assertEqual(undefined, apply_func(qos, [], #{})),
    ?assertEqual(1, apply_func(qos, [], message())).

t_flags(_) ->
    ?assertEqual(#{dup => false}, apply_func(flags, [], message())).

t_flag(_) ->
    Msg = message(),
    Msg1 = emqx_message:set_flag(retain, Msg),
    ?assertNot(apply_func(flag, [dup], Msg)),
    ?assert(apply_func(flag, [retain], Msg1)).

t_topic(_) ->
    Msg = message(),
    ?assertEqual(<<"topic/#">>, apply_func(topic, [], Msg)),
    ?assertEqual(<<"topic">>, apply_func(topic, [1], Msg)).

t_clientid(_) ->
    Msg = message(),
    ?assertEqual(undefined, apply_func(clientid, [], #{})),
    ?assertEqual(<<"clientid">>, apply_func(clientid, [], Msg)).

t_clientip(_) ->
    Msg = emqx_message:set_header(peerhost, {127,0,0,1}, message()),
    ?assertEqual(undefined, apply_func(clientip, [], #{})),
    ?assertEqual(<<"127.0.0.1">>, apply_func(clientip, [], eventmsg_publish(Msg))).

t_peerhost(_) ->
    Msg = emqx_message:set_header(peerhost, {127,0,0,1}, message()),
    ?assertEqual(undefined, apply_func(peerhost, [], #{})),
    ?assertEqual(<<"127.0.0.1">>, apply_func(peerhost, [], eventmsg_publish(Msg))).

t_username(_) ->
    Msg = emqx_message:set_header(username, <<"feng">>, message()),
    ?assertEqual(<<"feng">>, apply_func(username, [], eventmsg_publish(Msg))).

t_payload(_) ->
    Input = emqx_message:to_map(message()),
    NestedMap = #{a => #{b => #{c => c}}},
    ?assertEqual(<<"hello">>, apply_func(payload, [], Input#{payload => <<"hello">>})),
    ?assertEqual(c, apply_func(payload, [<<"a.b.c">>], Input#{payload => NestedMap})).

%%------------------------------------------------------------------------------
%% Data Type Convertion Funcs
%%------------------------------------------------------------------------------
t_str(_) ->
    ?assertEqual(<<"abc">>, emqx_rule_funcs:str("abc")),
    ?assertEqual(<<"abc">>, emqx_rule_funcs:str(abc)),
    ?assertEqual(<<"{\"a\":1}">>, emqx_rule_funcs:str(#{a => 1})),
    ?assertEqual(<<"[{\"a\":1},{\"b\":1}]">>, emqx_rule_funcs:str([#{a => 1},#{b => 1}])),
    ?assertEqual(<<"1">>, emqx_rule_funcs:str(1)),
    ?assertEqual(<<"2.0">>, emqx_rule_funcs:str(2.0)),
    ?assertEqual(<<"true">>, emqx_rule_funcs:str(true)),
    ?assertError(_, emqx_rule_funcs:str({a, v})),

    ?assertEqual(<<"abc">>, emqx_rule_funcs:str_utf8("abc")),
    ?assertEqual(<<"abc 你好"/utf8>>, emqx_rule_funcs:str_utf8("abc 你好")),
    ?assertEqual(<<"abc 你好"/utf8>>, emqx_rule_funcs:str_utf8(<<"abc 你好"/utf8>>)),
    ?assertEqual(<<"abc">>, emqx_rule_funcs:str_utf8(abc)),
    ?assertEqual(<<"{\"a\":\"abc 你好\"}"/utf8>>, emqx_rule_funcs:str_utf8(#{a => <<"abc 你好"/utf8>>})),
    ?assertEqual(<<"1">>, emqx_rule_funcs:str_utf8(1)),
    ?assertEqual(<<"2.0">>, emqx_rule_funcs:str_utf8(2.0)),
    ?assertEqual(<<"true">>, emqx_rule_funcs:str_utf8(true)),
    ?assertError(_, emqx_rule_funcs:str_utf8({a, v})).

t_int(_) ->
    ?assertEqual(1, emqx_rule_funcs:int("1")),
    ?assertEqual(1, emqx_rule_funcs:int(<<"1.0">>)),
    ?assertEqual(1, emqx_rule_funcs:int(1)),
    ?assertEqual(1, emqx_rule_funcs:int(1.9)),
    ?assertEqual(1, emqx_rule_funcs:int(1.0001)),
    ?assertEqual(1, emqx_rule_funcs:int(true)),
    ?assertEqual(0, emqx_rule_funcs:int(false)),
    ?assertError({invalid_number, {a, v}}, emqx_rule_funcs:int({a, v})),
    ?assertError(_, emqx_rule_funcs:int("a")).

t_float(_) ->
    ?assertEqual(1.0, emqx_rule_funcs:float("1.0")),
    ?assertEqual(1.0, emqx_rule_funcs:float(<<"1.0">>)),
    ?assertEqual(1.0, emqx_rule_funcs:float(1)),
    ?assertEqual(1.0, emqx_rule_funcs:float(1.0)),
    ?assertEqual(1.9, emqx_rule_funcs:float(1.9)),
    ?assertEqual(1.0001, emqx_rule_funcs:float(1.0001)),
    ?assertEqual(1.0000000001, emqx_rule_funcs:float(1.0000000001)),
    ?assertError({invalid_number, {a, v}}, emqx_rule_funcs:float({a, v})),
    ?assertError(_, emqx_rule_funcs:float("a")).


t_float2str(_) ->
    ?assertEqual(<<"20.2">>, emqx_rule_funcs:float2str(20.2, 1)),
    ?assertEqual(<<"20.2">>, emqx_rule_funcs:float2str(20.2, 10)),
    ?assertEqual(<<"20.199999999999999">>, emqx_rule_funcs:float2str(20.2, 15)),
    ?assertEqual(<<"20.1999999999999993">>, emqx_rule_funcs:float2str(20.2, 16)).

t_map(_) ->
    ?assertEqual(#{ver => <<"1.0">>, name => "emqx"}, emqx_rule_funcs:map([{ver, <<"1.0">>}, {name, "emqx"}])),
    ?assertEqual(#{<<"a">> => 1}, emqx_rule_funcs:map(<<"{\"a\":1}">>)),
    ?assertError(_, emqx_rule_funcs:map(<<"a">>)),
    ?assertError(_, emqx_rule_funcs:map("a")),
    ?assertError(_, emqx_rule_funcs:map(1.0)).

t_bool(_) ->
    ?assertEqual(true, emqx_rule_funcs:bool(1)),
    ?assertEqual(true, emqx_rule_funcs:bool(1.0)),
    ?assertEqual(false, emqx_rule_funcs:bool(0)),
    ?assertEqual(false, emqx_rule_funcs:bool(0.0)),
    ?assertEqual(true, emqx_rule_funcs:bool(true)),
    ?assertEqual(true, emqx_rule_funcs:bool(<<"true">>)),
    ?assertEqual(false, emqx_rule_funcs:bool(false)),
    ?assertEqual(false, emqx_rule_funcs:bool(<<"false">>)),
    ?assertError({invalid_boolean, _}, emqx_rule_funcs:bool(3)).

t_proc_dict_put_get_del(_) ->
    ?assertEqual(undefined, emqx_rule_funcs:proc_dict_get(<<"abc">>)),
    emqx_rule_funcs:proc_dict_put(<<"abc">>, 1),
    ?assertEqual(1, emqx_rule_funcs:proc_dict_get(<<"abc">>)),
    emqx_rule_funcs:proc_dict_del(<<"abc">>),
    ?assertEqual(undefined, emqx_rule_funcs:proc_dict_get(<<"abc">>)).

t_term_encode(_) ->
    TestData = [<<"abc">>, #{a => 1}, #{<<"3">> => [1,2,4]}],
    lists:foreach(fun(Data) ->
            ?assertEqual(Data,
                emqx_rule_funcs:term_decode(
                    emqx_rule_funcs:term_encode(Data)))
        end, TestData).

t_hexstr2bin(_) ->
    ?assertEqual(<<6, 54, 79>>, emqx_rule_funcs:hexstr2bin(<<"6364f">>)),
    ?assertEqual(<<10>>, emqx_rule_funcs:hexstr2bin(<<"a">>)),
    ?assertEqual(<<15>>, emqx_rule_funcs:hexstr2bin(<<"f">>)),
    ?assertEqual(<<5>>, emqx_rule_funcs:hexstr2bin(<<"5">>)),
    ?assertEqual(<<1, 2>>, emqx_rule_funcs:hexstr2bin(<<"0102">>)),
    ?assertEqual(<<17, 33>>, emqx_rule_funcs:hexstr2bin(<<"1121">>)).

t_bin2hexstr(_) ->
    ?assertEqual(<<"0102">>, emqx_rule_funcs:bin2hexstr(<<1,2>>)),
    ?assertEqual(<<"1121">>, emqx_rule_funcs:bin2hexstr(<<17,33>>)).

t_hex_convert(_) ->
    ?PROPTEST(hex_convert).

hex_convert() ->
    ?FORALL(L, list(range(0, 255)),
            begin
                AbitraryBin = list_to_binary(L),
                AbitraryBin == emqx_rule_funcs:hexstr2bin(
                    emqx_rule_funcs:bin2hexstr(AbitraryBin))
            end).

t_is_null(_) ->
    ?assertEqual(true, emqx_rule_funcs:is_null(undefined)),
    ?assertEqual(false, emqx_rule_funcs:is_null(a)),
    ?assertEqual(false, emqx_rule_funcs:is_null(<<>>)),
    ?assertEqual(false, emqx_rule_funcs:is_null(<<"a">>)).

t_is_not_null(_) ->
    [?assertEqual(emqx_rule_funcs:is_not_null(T), not emqx_rule_funcs:is_null(T))
     || T <- [undefined, a, <<"a">>, <<>>]].

t_is_str(_) ->
    [?assertEqual(true, emqx_rule_funcs:is_str(T))
     || T <- [<<"a">>, <<>>, <<"abc">>]],
    [?assertEqual(false, emqx_rule_funcs:is_str(T))
     || T <- ["a", a, 1]].

t_is_bool(_) ->
    [?assertEqual(true, emqx_rule_funcs:is_bool(T))
     || T <- [true, false]],
    [?assertEqual(false, emqx_rule_funcs:is_bool(T))
     || T <- ["a", <<>>, a, 2]].

t_is_int(_) ->
    [?assertEqual(true, emqx_rule_funcs:is_int(T))
     || T <- [1, 2, -1]],
    [?assertEqual(false, emqx_rule_funcs:is_int(T))
     || T <- [1.1, "a", a]].

t_is_float(_) ->
    [?assertEqual(true, emqx_rule_funcs:is_float(T))
     || T <- [1.1, 2.0, -1.2]],
    [?assertEqual(false, emqx_rule_funcs:is_float(T))
     || T <- [1, "a", a, <<>>]].

t_is_num(_) ->
    [?assertEqual(true, emqx_rule_funcs:is_num(T))
     || T <- [1.1, 2.0, -1.2, 1]],
    [?assertEqual(false, emqx_rule_funcs:is_num(T))
     || T <- ["a", a, <<>>]].

t_is_map(_) ->
    [?assertEqual(true, emqx_rule_funcs:is_map(T))
     || T <- [#{}, #{a =>1}]],
    [?assertEqual(false, emqx_rule_funcs:is_map(T))
     || T <- ["a", a, <<>>]].

t_is_array(_) ->
    [?assertEqual(true, emqx_rule_funcs:is_array(T))
     || T <- [[], [1,2]]],
    [?assertEqual(false, emqx_rule_funcs:is_array(T))
     || T <- [<<>>, a]].

%%------------------------------------------------------------------------------
%% Test cases for arith op
%%------------------------------------------------------------------------------

t_arith_op(_) ->
    ?PROPTEST(prop_arith_op).

prop_arith_op() ->
    ?FORALL({X, Y}, {number(), number()},
            begin
                (X + Y) == apply_func('+', [X, Y]) andalso
                (X - Y) == apply_func('-', [X, Y]) andalso
                (X * Y) == apply_func('*', [X, Y]) andalso
                (if Y =/= 0 ->
                        (X / Y) == apply_func('/', [X, Y]);
                    true -> true
                 end) andalso
                (case is_integer(X)
                     andalso is_pos_integer(Y) of
                     true ->
                         (X rem Y) == apply_func('mod', [X, Y]);
                     false -> true
                end)
            end).

is_pos_integer(X) ->
    is_integer(X) andalso X > 0.

%%------------------------------------------------------------------------------
%% Test cases for math fun
%%------------------------------------------------------------------------------

t_math_fun(_) ->
    ?PROPTEST(prop_math_fun).

prop_math_fun() ->
    Excluded = [module_info, atanh, asin, acos],
    MathFuns = [{F, A} || {F, A} <- math:module_info(exports),
                          not lists:member(F, Excluded),
                          erlang:function_exported(emqx_rule_funcs, F, A)],
    ?FORALL({X, Y}, {pos_integer(), pos_integer()},
            begin
                lists:foldl(fun({F, 1}, True) ->
                                    True andalso comp_with_math(F, X);
                               ({F = fmod, 2}, True) ->
                                    True andalso (if Y =/= 0 ->
                                                         comp_with_math(F, X, Y);
                                                     true -> true
                                                  end);
                               ({F, 2}, True) ->
                                    True andalso comp_with_math(F, X, Y)
                            end, true, MathFuns)
            end).

comp_with_math(Fun, X)
        when Fun =:= exp;
             Fun =:= sinh;
             Fun =:= cosh ->
    if X < 710 -> math:Fun(X) == apply_func(Fun, [X]);
       true -> true
    end;
comp_with_math(F, X) ->
    math:F(X) == apply_func(F, [X]).

comp_with_math(F, X, Y) ->
    math:F(X, Y) == apply_func(F, [X, Y]).

%%------------------------------------------------------------------------------
%% Test cases for bits op
%%------------------------------------------------------------------------------

t_bits_op(_) ->
    ?PROPTEST(prop_bits_op).

prop_bits_op() ->
    ?FORALL({X, Y}, {integer(), integer()},
            begin
                (bnot X) == apply_func(bitnot, [X]) andalso
                (X band Y) == apply_func(bitand, [X, Y]) andalso
                (X bor Y) == apply_func(bitor, [X, Y]) andalso
                (X bxor Y) == apply_func(bitxor, [X, Y]) andalso
                (X bsl Y) == apply_func(bitsl, [X, Y]) andalso
                (X bsr Y) == apply_func(bitsr, [X, Y])
            end).

%%------------------------------------------------------------------------------
%% Test cases for string
%%------------------------------------------------------------------------------

t_lower_upper(_) ->
    ?assertEqual(<<"ABC4">>, apply_func(upper, [<<"abc4">>])),
    ?assertEqual(<<"0abc">>, apply_func(lower, [<<"0ABC">>])).

t_reverse(_) ->
    ?assertEqual(<<"dcba">>, apply_func(reverse, [<<"abcd">>])),
    ?assertEqual(<<"4321">>, apply_func(reverse, [<<"1234">>])).

t_strlen(_) ->
    ?assertEqual(4, apply_func(strlen, [<<"abcd">>])),
    ?assertEqual(2, apply_func(strlen, [<<"你好">>])).

t_substr(_) ->
    ?assertEqual(<<"">>, apply_func(substr, [<<"">>, 1])),
    ?assertEqual(<<"bc">>, apply_func(substr, [<<"abc">>, 1])),
    ?assertEqual(<<"bc">>, apply_func(substr, [<<"abcd">>, 1, 2])).

t_trim(_) ->
    ?assertEqual(<<>>, apply_func(trim, [<<>>])),
    ?assertEqual(<<>>, apply_func(ltrim, [<<>>])),
    ?assertEqual(<<>>, apply_func(rtrim, [<<>>])),
    ?assertEqual(<<"abc">>, apply_func(trim, [<<" abc ">>])),
    ?assertEqual(<<"abc ">>, apply_func(ltrim, [<<" abc ">>])),
    ?assertEqual(<<" abc">>, apply_func(rtrim, [<<" abc">>])).

t_split_all(_) ->
    ?assertEqual([], apply_func(split, [<<>>, <<"/">>])),
    ?assertEqual([], apply_func(split, [<<"/">>, <<"/">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c">>], apply_func(split, [<<"/a/b/c">>, <<"/">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c">>], apply_func(split, [<<"/a/b//c/">>, <<"/">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c">>], apply_func(split, [<<"a,b,c">>, <<",">>])),
    ?assertEqual([<<"a">>,<<" b ">>,<<"c">>], apply_func(split, [<<"a, b ,c">>, <<",">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c\n">>], apply_func(split, [<<"a,b,c\n">>, <<",">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c\r\n">>], apply_func(split, [<<"a,b,c\r\n">>, <<",">>])).

t_split_notrim_all(_) ->
    ?assertEqual([<<>>], apply_func(split, [<<>>, <<"/">>, <<"notrim">>])),
    ?assertEqual([<<>>,<<>>], apply_func(split, [<<"/">>, <<"/">>, <<"notrim">>])),
    ?assertEqual([<<>>, <<"a">>,<<"b">>,<<"c">>], apply_func(split, [<<"/a/b/c">>, <<"/">>, <<"notrim">>])),
    ?assertEqual([<<>>, <<"a">>,<<"b">>, <<>>, <<"c">>, <<>>], apply_func(split, [<<"/a/b//c/">>, <<"/">>, <<"notrim">>])),
    ?assertEqual([<<>>, <<"a">>,<<"b">>,<<"c\n">>], apply_func(split, [<<",a,b,c\n">>, <<",">>, <<"notrim">>])),
    ?assertEqual([<<"a">>,<<" b">>,<<"c\r\n">>], apply_func(split, [<<"a, b,c\r\n">>, <<",">>, <<"notrim">>])),
    ?assertEqual([<<"哈哈"/utf8>>,<<" 你好"/utf8>>,<<" 是的\r\n"/utf8>>], apply_func(split, [<<"哈哈, 你好, 是的\r\n"/utf8>>, <<",">>, <<"notrim">>])).

t_split_leading(_) ->
    ?assertEqual([], apply_func(split, [<<>>, <<"/">>, <<"leading">>])),
    ?assertEqual([], apply_func(split, [<<"/">>, <<"/">>, <<"leading">>])),
    ?assertEqual([<<"a/b/c">>], apply_func(split, [<<"/a/b/c">>, <<"/">>, <<"leading">>])),
    ?assertEqual([<<"a">>,<<"b//c/">>], apply_func(split, [<<"a/b//c/">>, <<"/">>, <<"leading">>])),
    ?assertEqual([<<"a">>,<<"b,c\n">>], apply_func(split, [<<"a,b,c\n">>, <<",">>, <<"leading">>])),
    ?assertEqual([<<"a b">>,<<"c\r\n">>], apply_func(split, [<<"a b,c\r\n">>, <<",">>, <<"leading">>])),
    ?assertEqual([<<"哈哈"/utf8>>,<<" 你好, 是的\r\n"/utf8>>], apply_func(split, [<<"哈哈, 你好, 是的\r\n"/utf8>>, <<",">>, <<"leading">>])).

t_split_leading_notrim(_) ->
    ?assertEqual([<<>>], apply_func(split, [<<>>, <<"/">>, <<"leading_notrim">>])),
    ?assertEqual([<<>>,<<>>], apply_func(split, [<<"/">>, <<"/">>, <<"leading_notrim">>])),
    ?assertEqual([<<>>, <<"a/b/c">>], apply_func(split, [<<"/a/b/c">>, <<"/">>, <<"leading_notrim">>])),
    ?assertEqual([<<"a">>,<<"b//c/">>], apply_func(split, [<<"a/b//c/">>, <<"/">>, <<"leading_notrim">>])),
    ?assertEqual([<<"a">>,<<"b,c\n">>], apply_func(split, [<<"a,b,c\n">>, <<",">>, <<"leading_notrim">>])),
    ?assertEqual([<<"a b">>,<<"c\r\n">>], apply_func(split, [<<"a b,c\r\n">>, <<",">>, <<"leading_notrim">>])),
    ?assertEqual([<<"哈哈"/utf8>>,<<" 你好, 是的\r\n"/utf8>>], apply_func(split, [<<"哈哈, 你好, 是的\r\n"/utf8>>, <<",">>, <<"leading_notrim">>])).

t_split_trailing(_) ->
    ?assertEqual([], apply_func(split, [<<>>, <<"/">>, <<"trailing">>])),
    ?assertEqual([], apply_func(split, [<<"/">>, <<"/">>, <<"trailing">>])),
    ?assertEqual([<<"/a/b">>, <<"c">>], apply_func(split, [<<"/a/b/c">>, <<"/">>, <<"trailing">>])),
    ?assertEqual([<<"a/b//c">>], apply_func(split, [<<"a/b//c/">>, <<"/">>, <<"trailing">>])),
    ?assertEqual([<<"a,b">>,<<"c\n">>], apply_func(split, [<<"a,b,c\n">>, <<",">>, <<"trailing">>])),
    ?assertEqual([<<"a b">>,<<"c\r\n">>], apply_func(split, [<<"a b,c\r\n">>, <<",">>, <<"trailing">>])),
    ?assertEqual([<<"哈哈, 你好"/utf8>>,<<" 是的\r\n"/utf8>>], apply_func(split, [<<"哈哈, 你好, 是的\r\n"/utf8>>, <<",">>, <<"trailing">>])).

t_split_trailing_notrim(_) ->
    ?assertEqual([<<>>], apply_func(split, [<<>>, <<"/">>, <<"trailing_notrim">>])),
    ?assertEqual([<<>>, <<>>], apply_func(split, [<<"/">>, <<"/">>, <<"trailing_notrim">>])),
    ?assertEqual([<<"/a/b">>, <<"c">>], apply_func(split, [<<"/a/b/c">>, <<"/">>, <<"trailing_notrim">>])),
    ?assertEqual([<<"a/b//c">>, <<>>], apply_func(split, [<<"a/b//c/">>, <<"/">>, <<"trailing_notrim">>])),
    ?assertEqual([<<"a,b">>,<<"c\n">>], apply_func(split, [<<"a,b,c\n">>, <<",">>, <<"trailing_notrim">>])),
    ?assertEqual([<<"a b">>,<<"c\r\n">>], apply_func(split, [<<"a b,c\r\n">>, <<",">>, <<"trailing_notrim">>])),
    ?assertEqual([<<"哈哈, 你好"/utf8>>,<<" 是的\r\n"/utf8>>], apply_func(split, [<<"哈哈, 你好, 是的\r\n"/utf8>>, <<",">>, <<"trailing_notrim">>])).

t_tokens(_) ->
    ?assertEqual([], apply_func(tokens, [<<>>, <<"/">>])),
    ?assertEqual([], apply_func(tokens, [<<"/">>, <<"/">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c">>], apply_func(tokens, [<<"/a/b/c">>, <<"/">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c">>], apply_func(tokens, [<<"/a/b//c/">>, <<"/">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c">>], apply_func(tokens, [<<" /a/ b /c">>, <<" /">>])),
    ?assertEqual([<<"a">>,<<"\nb">>,<<"c\n">>], apply_func(tokens, [<<"a ,\nb,c\n">>, <<", ">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c\r\n">>], apply_func(tokens, [<<"a ,b,c\r\n">>, <<", ">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c">>], apply_func(tokens, [<<"a,b, c\n">>, <<", ">>, <<"nocrlf">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c">>], apply_func(tokens, [<<"a,b,c\r\n">>, <<",">>, <<"nocrlf">>])),
    ?assertEqual([<<"a">>,<<"b">>,<<"c">>], apply_func(tokens, [<<"a,b\r\n,c\n">>, <<",">>, <<"nocrlf">>])),
    ?assertEqual([], apply_func(tokens, [<<"\r\n">>, <<",">>, <<"nocrlf">>])),
    ?assertEqual([], apply_func(tokens, [<<"\r\n">>, <<",">>, <<"nocrlf">>])),
    ?assertEqual([<<"哈哈"/utf8>>,<<"你好"/utf8>>,<<"是的"/utf8>>], apply_func(tokens, [<<"哈哈, 你好, 是的\r\n"/utf8>>, <<", ">>, <<"nocrlf">>])).

t_concat(_) ->
    ?assertEqual(<<"ab">>, apply_func(concat, [<<"a">>, <<"b">>])),
    ?assertEqual(<<"ab">>, apply_func('+', [<<"a">>, <<"b">>])),
    ?assertEqual(<<"哈哈你好"/utf8>>, apply_func(concat, [<<"哈哈"/utf8>>,<<"你好"/utf8>>])),
    ?assertEqual(<<"abc">>, apply_func(concat, [apply_func(concat, [<<"a">>, <<"b">>]), <<"c">>])),
    ?assertEqual(<<"a">>, apply_func(concat, [<<"">>, <<"a">>])),
    ?assertEqual(<<"a">>, apply_func(concat, [<<"a">>, <<"">>])),
    ?assertEqual(<<>>, apply_func(concat, [<<"">>, <<"">>])).

t_sprintf(_) ->
    ?assertEqual(<<"Hello Shawn!">>, apply_func(sprintf, [<<"Hello ~s!">>, <<"Shawn">>])),
    ?assertEqual(<<"Name: ABC, Count: 2">>, apply_func(sprintf, [<<"Name: ~s, Count: ~p">>, <<"ABC">>, 2])),
    ?assertEqual(<<"Name: ABC, Count: 2, Status: {ok,running}">>, apply_func(sprintf, [<<"Name: ~s, Count: ~p, Status: ~p">>, <<"ABC">>, 2, {ok, running}])).

t_pad(_) ->
    ?assertEqual(<<"abc  ">>, apply_func(pad, [<<"abc">>, 5])),
    ?assertEqual(<<"abc">>, apply_func(pad, [<<"abc">>, 0])),
    ?assertEqual(<<"abc  ">>, apply_func(pad, [<<"abc">>, 5, <<"trailing">>])),
    ?assertEqual(<<"abc">>, apply_func(pad, [<<"abc">>, 0, <<"trailing">>])),
    ?assertEqual(<<" abc ">>, apply_func(pad, [<<"abc">>, 5, <<"both">>])),
    ?assertEqual(<<"abc">>, apply_func(pad, [<<"abc">>, 0, <<"both">>])),
    ?assertEqual(<<"  abc">>, apply_func(pad, [<<"abc">>, 5, <<"leading">>])),
    ?assertEqual(<<"abc">>, apply_func(pad, [<<"abc">>, 0, <<"leading">>])).

t_pad_char(_) ->
    ?assertEqual(<<"abcee">>, apply_func(pad, [<<"abc">>, 5, <<"trailing">>, <<"e">>])),
    ?assertEqual(<<"abcexex">>, apply_func(pad, [<<"abc">>, 5, <<"trailing">>, <<"ex">>])),
    ?assertEqual(<<"eabce">>, apply_func(pad, [<<"abc">>, 5, <<"both">>, <<"e">>])),
    ?assertEqual(<<"exabcex">>, apply_func(pad, [<<"abc">>, 5, <<"both">>, <<"ex">>])),
    ?assertEqual(<<"eeabc">>, apply_func(pad, [<<"abc">>, 5, <<"leading">>, <<"e">>])),
    ?assertEqual(<<"exexabc">>, apply_func(pad, [<<"abc">>, 5, <<"leading">>, <<"ex">>])).

t_replace(_) ->
    ?assertEqual(<<"ab-c--">>, apply_func(replace, [<<"ab c  ">>, <<" ">>, <<"-">>])),
    ?assertEqual(<<"ab::c::::">>, apply_func(replace, [<<"ab c  ">>, <<" ">>, <<"::">>])),
    ?assertEqual(<<"ab-c--">>, apply_func(replace, [<<"ab c  ">>, <<" ">>, <<"-">>, <<"all">>])),
    ?assertEqual(<<"ab-c  ">>, apply_func(replace, [<<"ab c  ">>, <<" ">>, <<"-">>, <<"leading">>])),
    ?assertEqual(<<"ab c -">>, apply_func(replace, [<<"ab c  ">>, <<" ">>, <<"-">>, <<"trailing">>])).

t_ascii(_) ->
    ?assertEqual(97, apply_func(ascii, [<<"a">>])),
    ?assertEqual(97, apply_func(ascii, [<<"ab">>])).

t_find(_) ->
    ?assertEqual(<<"cbcd">>, apply_func(find, [<<"acbcd">>, <<"c">>])),
    ?assertEqual(<<"cbcd">>, apply_func(find, [<<"acbcd">>, <<"c">>, <<"leading">>])),
    ?assertEqual(<<"">>, apply_func(find, [<<"acbcd">>, <<"e">>])),
    ?assertEqual(<<"">>, apply_func(find, [<<"">>, <<"c">>])),
    ?assertEqual(<<"">>, apply_func(find, [<<"">>, <<"">>])).

t_find_trailing(_) ->
    ?assertEqual(<<"cd">>, apply_func(find, [<<"acbcd">>, <<"c">>, <<"trailing">>])),
    ?assertEqual(<<"">>, apply_func(find, [<<"acbcd">>, <<"e">>, <<"trailing">>])),
    ?assertEqual(<<"">>, apply_func(find, [<<"">>, <<"c">>, <<"trailing">>])),
    ?assertEqual(<<"">>, apply_func(find, [<<"">>, <<"">>, <<"trailing">>])).

t_regex_match(_) ->
    ?assertEqual(true, apply_func(regex_match, [<<"acbcd">>, <<"c">>])),
    ?assertEqual(true, apply_func(regex_match, [<<"acbcd">>, <<"(ac)+">>])),
    ?assertEqual(false, apply_func(regex_match, [<<"bcd">>, <<"(ac)+">>])),
    ?assertEqual(true, apply_func(regex_match, [<<>>, <<".*">>])),
    ?assertEqual(false, apply_func(regex_match, [<<>>, <<"[a-z]+">>])),
    ?assertEqual(true, apply_func(regex_match, [<<"exebd">>, <<"^[a-z]+$">>])),
    ?assertEqual(false, apply_func(regex_match, [<<"e2xebd">>, <<"^[a-z]+$">>])).

t_regex_replace(_) ->
    ?assertEqual(<<>>, apply_func(regex_replace, [<<>>, <<"c.*">>, <<"e">>])),
    ?assertEqual(<<"aebed">>, apply_func(regex_replace, [<<"accbcd">>, <<"c+">>, <<"e">>])),
    ?assertEqual(<<"a[cc]b[c]d">>, apply_func(regex_replace, [<<"accbcd">>, <<"c+">>, <<"[&]">>])).

ascii_string() -> list(range(0,127)).

bin(S) -> iolist_to_binary(S).

%%------------------------------------------------------------------------------
%% Test cases for array funcs
%%------------------------------------------------------------------------------

t_nth(_) ->
    ?assertEqual(2, apply_func(nth, [2, [1,2,3,4]])),
    ?assertEqual(4, apply_func(nth, [4, [1,2,3,4]])).

t_length(_) ->
    ?assertEqual(4, apply_func(length, [[1,2,3,4]])),
    ?assertEqual(0, apply_func(length, [[]])).

t_slice(_) ->
    ?assertEqual([1,2,3,4], apply_func(sublist, [4, [1,2,3,4]])),
    ?assertEqual([1,2], apply_func(sublist, [2, [1,2,3,4]])),
    ?assertEqual([4], apply_func(sublist, [4, 1, [1,2,3,4]])),
    ?assertEqual([4], apply_func(sublist, [4, 2, [1,2,3,4]])),
    ?assertEqual([], apply_func(sublist, [5, 2, [1,2,3,4]])),
    ?assertEqual([2,3], apply_func(sublist, [2, 2, [1,2,3,4]])),
    ?assertEqual([1], apply_func(sublist, [1, 1, [1,2,3,4]])).

t_first_last(_) ->
    ?assertEqual(1, apply_func(first, [[1,2,3,4]])),
    ?assertEqual(4, apply_func(last, [[1,2,3,4]])).

t_contains(_) ->
    ?assertEqual(true, apply_func(contains, [1, [1,2,3,4]])),
    ?assertEqual(true, apply_func(contains, [3, [1,2,3,4]])),
    ?assertEqual(true, apply_func(contains, [<<"a">>, [<<>>,<<"ab">>,3,<<"a">>]])),
    ?assertEqual(true, apply_func(contains, [#{a=>b}, [#{a=>1}, #{a=>b}]])),
    ?assertEqual(false, apply_func(contains, [#{a=>b}, [#{a=>1}]])),
    ?assertEqual(false, apply_func(contains, [3, [1, 2]])),
    ?assertEqual(false, apply_func(contains, [<<"c">>, [<<>>,<<"ab">>,3,<<"a">>]])).

t_map_get(_) ->
    ?assertEqual(1, apply_func(map_get, [<<"a">>, #{a => 1}])),
    ?assertEqual(undefined, apply_func(map_get, [<<"a">>, #{}])),
    ?assertEqual(1, apply_func(map_get, [<<"a.b">>, #{a => #{b => 1}}])),
    ?assertEqual(undefined, apply_func(map_get, [<<"a.c">>, #{a => #{b => 1}}])),
    ?assertEqual(undefined, apply_func(map_get, [<<"a">>, #{}])).

t_map_put(_) ->
    ?assertEqual(#{<<"a">> => 1}, apply_func(map_put, [<<"a">>, 1, #{}])),
    ?assertEqual(#{a => 2}, apply_func(map_put, [<<"a">>, 2, #{a => 1}])),
    ?assertEqual(#{<<"a">> => #{<<"b">> => 1}}, apply_func(map_put, [<<"a.b">>, 1, #{}])),
    ?assertEqual(#{a => #{b => 1, <<"c">> => 1}}, apply_func(map_put, [<<"a.c">>, 1, #{a => #{b => 1}}])),
    ?assertEqual(#{a => 2}, apply_func(map_put, [<<"a">>, 2, #{a => 1}])).

t_mget(_) ->
    ?assertEqual(1, apply_func(map_get, [<<"a">>, #{a => 1}])),
    ?assertEqual(1, apply_func(map_get, [<<"a">>, #{<<"a">> => 1}])),
    ?assertEqual(undefined, apply_func(map_get, [<<"a">>, #{}])).

t_mput(_) ->
    ?assertEqual(#{<<"a">> => 1}, apply_func(map_put, [<<"a">>, 1, #{}])),
    ?assertEqual(#{<<"a">> => 2}, apply_func(map_put, [<<"a">>, 2, #{<<"a">> => 1}])),
    ?assertEqual(#{a => 2}, apply_func(map_put, [<<"a">>, 2, #{a => 1}])).

t_bitsize(_) ->
    ?assertEqual(8, apply_func(bitsize, [<<"a">>])),
    ?assertEqual(4, apply_func(bitsize, [<<15:4>>])).

t_subbits(_) ->
    ?assertEqual(1, apply_func(subbits, [<<255:8>>, 1])),
    ?assertEqual(3, apply_func(subbits, [<<255:8>>, 2])),
    ?assertEqual(7, apply_func(subbits, [<<255:8>>, 3])),
    ?assertEqual(15, apply_func(subbits, [<<255:8>>, 4])),
    ?assertEqual(31, apply_func(subbits, [<<255:8>>, 5])),
    ?assertEqual(63, apply_func(subbits, [<<255:8>>, 6])),
    ?assertEqual(127, apply_func(subbits, [<<255:8>>, 7])),
    ?assertEqual(255, apply_func(subbits, [<<255:8>>, 8])).

t_subbits2(_) ->
    ?assertEqual(1, apply_func(subbits, [<<255:8>>, 1, 1])),
    ?assertEqual(3, apply_func(subbits, [<<255:8>>, 1, 2])),
    ?assertEqual(7, apply_func(subbits, [<<255:8>>, 1, 3])),
    ?assertEqual(15, apply_func(subbits, [<<255:8>>, 1, 4])),
    ?assertEqual(31, apply_func(subbits, [<<255:8>>, 1, 5])),
    ?assertEqual(63, apply_func(subbits, [<<255:8>>, 1, 6])),
    ?assertEqual(127, apply_func(subbits, [<<255:8>>, 1, 7])),
    ?assertEqual(255, apply_func(subbits, [<<255:8>>, 1, 8])).

t_subbits2_1(_) ->
    ?assertEqual(1, apply_func(subbits, [<<255:8>>, 2, 1])),
    ?assertEqual(3, apply_func(subbits, [<<255:8>>, 2, 2])),
    ?assertEqual(7, apply_func(subbits, [<<255:8>>, 2, 3])),
    ?assertEqual(15, apply_func(subbits, [<<255:8>>, 2, 4])),
    ?assertEqual(31, apply_func(subbits, [<<255:8>>, 2, 5])),
    ?assertEqual(63, apply_func(subbits, [<<255:8>>, 2, 6])),
    ?assertEqual(127, apply_func(subbits, [<<255:8>>, 2, 7])),
    ?assertEqual(127, apply_func(subbits, [<<255:8>>, 2, 8])).
t_subbits2_integer(_) ->
    ?assertEqual(456, apply_func(subbits, [<<456:32/integer>>, 1, 32, <<"integer">>, <<"signed">>, <<"big">>])),
    ?assertEqual(-456, apply_func(subbits, [<<-456:32/integer>>, 1, 32, <<"integer">>, <<"signed">>, <<"big">>])).

t_subbits2_float(_) ->
    R = apply_func(subbits, [<<5.3:64/float>>, 1, 64, <<"float">>, <<"unsigned">>, <<"big">>]),
    RL = (5.3 - R),
    ct:pal(";;;;~p", [R]),
    ?assert( (RL >= 0 andalso RL < 0.0001) orelse (RL =< 0 andalso RL > -0.0001)),

    R2 = apply_func(subbits, [<<-5.3:64/float>>, 1, 64, <<"float">>, <<"signed">>, <<"big">>]),

    RL2 = (5.3 + R2),
    ct:pal(";;;;~p", [R2]),
    ?assert( (RL2 >= 0 andalso RL2 < 0.0001) orelse (RL2 =< 0 andalso RL2 > -0.0001)).

%%------------------------------------------------------------------------------
%% Test cases for Hash funcs
%%------------------------------------------------------------------------------

t_hash_funcs(_) ->
    ?PROPTEST(prop_hash_fun).

prop_hash_fun() ->
    ?FORALL(S, binary(),
            begin
                (32 == byte_size(apply_func(md5, [S]))) andalso
                (40 == byte_size(apply_func(sha, [S]))) andalso
                (64 == byte_size(apply_func(sha256, [S])))
            end).


%%------------------------------------------------------------------------------
%% Test cases for gzip funcs
%%------------------------------------------------------------------------------

t_gzip_funcs(_) ->
    ?PROPTEST(prop_gzip_fun).

prop_gzip_fun() ->
    ?FORALL(S, binary(),
            S == apply_func(gunzip, [apply_func(gzip, [S])])).

%%------------------------------------------------------------------------------
%% Test cases for zip funcs
%%------------------------------------------------------------------------------

t_zip_funcs(_) ->
    ?PROPTEST(prop_zip_fun).

prop_zip_fun() ->
    ?FORALL(S, binary(),
            S == apply_func(unzip, [apply_func(zip, [S])])).

%%------------------------------------------------------------------------------
%% Test cases for zip funcs
%%------------------------------------------------------------------------------

t_zip_compress_funcs(_) ->
    ?PROPTEST(prop_zip_compress_fun).

prop_zip_compress_fun() ->
    ?FORALL(S, binary(),
            S == apply_func(zip_uncompress, [apply_func(zip_compress, [S])])).

%%------------------------------------------------------------------------------
%% Test cases for base64
%%------------------------------------------------------------------------------

t_base64_encode(_) ->
    ?PROPTEST(prop_base64_encode).

prop_base64_encode() ->
    ?FORALL(S, list(range(0, 255)),
            begin
                Bin = iolist_to_binary(S),
                Bin == base64:decode(apply_func(base64_encode, [Bin]))
            end).

%%--------------------------------------------------------------------
%% Date functions
%%--------------------------------------------------------------------

t_now_rfc3339(_) ->
    ?assert(is_integer(
        calendar:rfc3339_to_system_time(
            binary_to_list(apply_func(now_rfc3339, []))))).

t_now_rfc3339_1(_) ->
    [?assert(is_integer(
        calendar:rfc3339_to_system_time(
            binary_to_list(apply_func(now_rfc3339, [atom_to_binary(Unit, utf8)])),
            [{unit, Unit}])))
     || Unit <- [second,millisecond,microsecond,nanosecond]].

t_now_timestamp(_) ->
    ?assert(is_integer(apply_func(now_timestamp, []))).

t_now_timestamp_1(_) ->
    [?assert(is_integer(
            apply_func(now_timestamp, [atom_to_binary(Unit, utf8)])))
     || Unit <- [second,millisecond,microsecond,nanosecond]].

t_unix_ts_to_rfc3339(_) ->
    [begin
        BUnit = atom_to_binary(Unit, utf8),
        Epoch = apply_func(now_timestamp, [BUnit]),
        DateTime = apply_func(unix_ts_to_rfc3339, [Epoch, BUnit]),
        ?assertEqual(Epoch,
            calendar:rfc3339_to_system_time(binary_to_list(DateTime), [{unit, Unit}]))
     end || Unit <- [second,millisecond,microsecond,nanosecond]].

t_rfc3339_to_unix_ts(_) ->
    [begin
        BUnit = atom_to_binary(Unit, utf8),
        Epoch = apply_func(now_timestamp, [BUnit]),
        DateTime = apply_func(unix_ts_to_rfc3339, [Epoch, BUnit]),
        ?assertEqual(Epoch, emqx_rule_funcs:rfc3339_to_unix_ts(DateTime, BUnit))
     end || Unit <- [second,millisecond,microsecond,nanosecond]].

t_format_date_funcs(_) ->
    ?PROPTEST(prop_format_date_fun).

prop_format_date_fun() ->
    Args1 = [<<"second">>, <<"+07:00">>, <<"%m--%d--%Y---%H:%M:%S%z">>],
    ?FORALL(S, erlang:system_time(second),
            S == apply_func(date_to_unix_ts,
                            Args1 ++ [apply_func(format_date,
                                                 Args1 ++ [S])])),
    Args2 = [<<"millisecond">>, <<"+04:00">>, <<"--%m--%d--%Y---%H:%M:%S:%3N%z">>],
    Args2DTUS = [<<"millisecond">>, <<"--%m--%d--%Y---%H:%M:%S:%3N%z">>],
    ?FORALL(S, erlang:system_time(millisecond),
            S == apply_func(date_to_unix_ts,
                            Args2DTUS ++ [apply_func(format_date,
                                                 Args2 ++ [S])])),
    Args = [<<"second">>, <<"+08:00">>, <<"%Y-%m-%d-%H:%M:%S%z">>],
    ArgsDTUS = [<<"second">>, <<"%Y-%m-%d-%H:%M:%S%z">>],
    ?FORALL(S, erlang:system_time(second),
            S == apply_func(date_to_unix_ts,
                            ArgsDTUS ++ [apply_func(format_date,
                                                Args ++ [S])])),
    % no offset in format string. force add offset
    Second = erlang:system_time(second),
    Args3 = [<<"second">>, <<"+04:00">>, <<"--%m--%d--%Y---%H:%M:%S">>, Second],
    Formatters3 = apply_func(format_date, Args3),
    Args3DTUS = [<<"second">>, <<"+04:00">>, <<"--%m--%d--%Y---%H:%M:%S">>, Formatters3],
    Second == apply_func(date_to_unix_ts, Args3DTUS).

apply_func(Name, Args) when is_atom(Name) ->
    erlang:apply(emqx_rule_funcs, Name, Args);
apply_func(Fun, Args) when is_function(Fun) ->
    erlang:apply(Fun, Args).

apply_func(Name, Args, Input) when is_map(Input) ->
    apply_func(apply_func(Name, Args), [Input]);
apply_func(Name, Args, Msg) ->
    apply_func(Name, Args, emqx_message:to_map(Msg)).

message() ->
    emqx_message:set_flags(#{dup => false},
        emqx_message:make(<<"clientid">>, 1, <<"topic/#">>, <<"payload">>)).

% t_contains_topic(_) ->
%     error('TODO').

% t_contains_topic_match(_) ->
%     error('TODO').

% t_div(_) ->
%     error('TODO').

% t_mod(_) ->
%     error('TODO').

% t_abs(_) ->
%     error('TODO').

% t_acos(_) ->
%     error('TODO').

% t_acosh(_) ->
%     error('TODO').

% t_asin(_) ->
%     error('TODO').

% t_asinh(_) ->
%     error('TODO').

% t_atan(_) ->
%     error('TODO').

% t_atanh(_) ->
%     error('TODO').

% t_ceil(_) ->
%     error('TODO').

% t_cos(_) ->
%     error('TODO').

% t_cosh(_) ->
%     error('TODO').

% t_exp(_) ->
%     error('TODO').

% t_floor(_) ->
%     error('TODO').

% t_fmod(_) ->
%     error('TODO').

% t_log(_) ->
%     error('TODO').

% t_log10(_) ->
%     error('TODO').

% t_log2(_) ->
%     error('TODO').

% t_power(_) ->
%     error('TODO').

% t_round(_) ->
%     error('TODO').

% t_sin(_) ->
%     error('TODO').

% t_sinh(_) ->
%     error('TODO').

% t_sqrt(_) ->
%     error('TODO').

% t_tan(_) ->
%     error('TODO').

% t_tanh(_) ->
%     error('TODO').

% t_bitnot(_) ->
%     error('TODO').

% t_bitand(_) ->
%     error('TODO').

% t_bitor(_) ->
%     error('TODO').

% t_bitxor(_) ->
%     error('TODO').

% t_bitsl(_) ->
%     error('TODO').

% t_bitsr(_) ->
%     error('TODO').

% t_lower(_) ->
%     error('TODO').

% t_ltrim(_) ->
%     error('TODO').

% t_rtrim(_) ->
%     error('TODO').

% t_upper(_) ->
%     error('TODO').

% t_split(_) ->
%     error('TODO').

% t_md5(_) ->
%     error('TODO').

% t_sha(_) ->
%     error('TODO').

% t_sha256(_) ->
%     error('TODO').

% t_json_encode(_) ->
%     error('TODO').

% t_json_decode(_) ->
%     error('TODO').


%%------------------------------------------------------------------------------
%% CT functions
%%------------------------------------------------------------------------------

all() ->
    IsTestCase = fun("t_" ++ _) -> true; (_) -> false end,
    [F || {F, _A} <- module_info(exports), IsTestCase(atom_to_list(F))].

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 30}}].
