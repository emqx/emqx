%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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
