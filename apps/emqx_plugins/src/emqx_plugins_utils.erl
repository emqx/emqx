%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_utils).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    parse_name_vsn/1,
    make_name_vsn_string/2
]).

parse_name_vsn(NameVsn) when is_binary(NameVsn) ->
    parse_name_vsn(binary_to_list(NameVsn));
parse_name_vsn(NameVsn) when is_list(NameVsn) ->
    case lists:splitwith(fun(X) -> X =/= $- end, NameVsn) of
        {AppName, [$- | Vsn]} -> {list_to_atom(AppName), Vsn};
        _ -> error(bad_name_vsn)
    end.

make_name_vsn_string(Name, Vsn) ->
    binary_to_list(iolist_to_binary([Name, "-", Vsn])).

-ifdef(TEST).

parse_name_vsn_test_() ->
    [
        ?_assertError(bad_name_vsn, parse_name_vsn("foo")),
        ?_assertEqual({foo, "1.0.0"}, parse_name_vsn("foo-1.0.0"))
    ].

make_name_vsn_string_test_() ->
    [
        ?_assertEqual("foo-1.0.0", make_name_vsn_string(<<"foo">>, "1.0.0"))
    ].

-endif.
