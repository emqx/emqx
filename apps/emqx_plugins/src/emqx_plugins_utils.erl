%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_utils).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    bin/1,
    parse_name_vsn/1,
    plugin_name/1,
    normalize_state_item/1,
    latest_name_vsn/2,
    compare_vsn/2,
    make_name_vsn_binary/2,
    make_name_vsn_string/2
]).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.

parse_name_vsn(NameVsn) when is_binary(NameVsn) ->
    parse_name_vsn(binary_to_list(NameVsn));
parse_name_vsn(NameVsn) when is_list(NameVsn) ->
    case lists:splitwith(fun(X) -> X =/= $- end, NameVsn) of
        {AppName, [$- | Vsn]} -> {list_to_atom(AppName), Vsn};
        _ -> error(bad_name_vsn)
    end.

compare_vsn(Vsn1, Vsn2) when is_binary(Vsn1) ->
    compare_vsn(binary_to_list(Vsn1), Vsn2);
compare_vsn(Vsn1, Vsn2) when is_binary(Vsn2) ->
    compare_vsn(Vsn1, binary_to_list(Vsn2));
compare_vsn(Vsn1, Vsn2) ->
    try emqx_release:vsn_compare(Vsn1, Vsn2) of
        Result ->
            Result
    catch
        error:{invalid_version_string, _} ->
            compare_vsn_fallback(Vsn1, Vsn2)
    end.

make_name_vsn_binary(Name, Vsn) ->
    iolist_to_binary([Name, "-", Vsn]).

make_name_vsn_string(Name, Vsn) ->
    binary_to_list(make_name_vsn_binary(Name, Vsn)).

plugin_name(NameVsn) ->
    {Name, _Vsn} = parse_name_vsn(NameVsn),
    bin(Name).

normalize_state_item(#{name_vsn := _NameVsn, enable := _Enable} = Item) ->
    Item;
normalize_state_item(#{<<"name_vsn">> := NameVsn, <<"enable">> := Enable}) ->
    #{
        name_vsn => NameVsn,
        enable => Enable
    }.

%% compare_vsn/2 returns `newer` when the second argument is newer than the first.
%% When versions are equal, keep the accumulator (`NameVsn1`) stable.
latest_name_vsn(NameVsn1, NameVsn2) ->
    {_Name1, Vsn1} = parse_name_vsn(NameVsn1),
    {_Name2, Vsn2} = parse_name_vsn(NameVsn2),
    case compare_vsn(Vsn1, Vsn2) of
        newer -> NameVsn2;
        _ -> NameVsn1
    end.

compare_vsn_fallback(Vsn1, Vsn2) ->
    compare_segments(split_vsn(Vsn1), split_vsn(Vsn2)).

split_vsn(Vsn) ->
    re:split(Vsn, "[.-]", [{return, list}, trim]).

compare_segments([], []) ->
    same;
compare_segments([], [_ | _]) ->
    older;
compare_segments([_ | _], []) ->
    newer;
compare_segments([S1 | Rest1], [S2 | Rest2]) ->
    case compare_segment(segment_type(S1), segment_type(S2)) of
        same -> compare_segments(Rest1, Rest2);
        Result -> Result
    end.

segment_type(Segment) ->
    case lists:all(fun(Char) -> Char >= $0 andalso Char =< $9 end, Segment) of
        true -> {int, list_to_integer(Segment)};
        false -> {string, string:lowercase(Segment)}
    end.

compare_segment({int, I1}, {int, I2}) when I1 =:= I2 ->
    same;
compare_segment({int, I1}, {int, I2}) when I1 < I2 ->
    older;
compare_segment({int, _I1}, {int, _I2}) ->
    newer;
compare_segment({string, S1}, {string, S2}) when S1 =:= S2 ->
    same;
compare_segment({string, S1}, {string, S2}) when S1 < S2 ->
    older;
compare_segment({string, _S1}, {string, _S2}) ->
    newer;
compare_segment({int, _}, {string, _}) ->
    newer;
compare_segment({string, _}, {int, _}) ->
    older.

-ifdef(TEST).

parse_name_vsn_test_() ->
    [
        ?_assertError(bad_name_vsn, parse_name_vsn("foo")),
        ?_assertEqual({foo, "1.0.0"}, parse_name_vsn("foo-1.0.0")),
        ?_assertEqual({bar_plugin, "5.9.0-beta.1"}, parse_name_vsn(<<"bar_plugin-5.9.0-beta.1">>))
    ].

make_name_vsn_string_test_() ->
    [
        ?_assertEqual("foo-1.0.0", make_name_vsn_string(<<"foo">>, "1.0.0"))
    ].

compare_vsn_fallback_test_() ->
    [
        ?_assertEqual(older, compare_vsn_fallback("1.0.0-alpha", "1.0.0-beta")),
        ?_assertEqual(newer, compare_vsn_fallback("2.0.0", "1.9.9")),
        ?_assertEqual(same, compare_vsn_fallback("1.0.0", "1.0.0")),
        ?_assertEqual(older, compare_segments([], ["1"])),
        ?_assertEqual(newer, compare_segments(["1"], [])),
        ?_assertEqual(["1", "0", "0", "beta"], split_vsn("1.0.0-beta")),
        ?_assertEqual({int, 10}, segment_type("10")),
        ?_assertEqual({string, "beta"}, segment_type("Beta")),
        ?_assertEqual(same, compare_segment({int, 1}, {int, 1})),
        ?_assertEqual(older, compare_segment({string, "alpha"}, {string, "beta"})),
        ?_assertEqual(newer, compare_segment({int, 1}, {string, "alpha"})),
        ?_assertEqual(older, compare_segment({string, "alpha"}, {int, 1}))
    ].

compare_vsn_test_() ->
    {setup,
        fun() ->
            meck:new(emqx_release, [passthrough]),
            ok
        end,
        fun(_) ->
            meck:unload(emqx_release)
        end,
        [
            fun compare_vsn_binary_args_case/0,
            fun compare_vsn_invalid_string_fallback_case/0
        ]}.

compare_vsn_binary_args_case() ->
    meck:expect(emqx_release, vsn_compare, fun("2.0.0", "1.0.0") -> newer end),
    ?assertEqual(newer, compare_vsn(<<"2.0.0">>, <<"1.0.0">>)).

compare_vsn_invalid_string_fallback_case() ->
    meck:expect(
        emqx_release,
        vsn_compare,
        fun("1.0.0-alpha", "1.0.0-beta") ->
            erlang:error({invalid_version_string, invalid})
        end
    ),
    ?assertEqual(older, compare_vsn("1.0.0-alpha", "1.0.0-beta")).

-endif.
