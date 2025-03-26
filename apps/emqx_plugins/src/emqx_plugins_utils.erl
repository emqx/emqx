%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
