%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd_opts_tests.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_opts_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(SOCKOPTS, [
	binary,
	{packet,    raw},
	{reuseaddr, true},
	{backlog,   512},
	{nodelay,   true}
]).

merge_test() ->
    Opts = emqttd_opts:merge(?SOCKOPTS, [raw,
                                        {backlog, 1024},
                                        {nodelay, false},
                                        {max_clients, 1024},
                                        {acceptors, 16}]),
    ?assertEqual(1024, proplists:get_value(backlog, Opts)),
    ?assertEqual(1024, proplists:get_value(max_clients, Opts)),
    ?assertEqual(lists:sort(Opts), [binary, raw,
                                    {acceptors, 16},
                                    {backlog, 1024},
                                    {max_clients, 1024},
                                    {nodelay, false},
                                    {packet, raw},
                                    {reuseaddr, true}]).


-endif.
