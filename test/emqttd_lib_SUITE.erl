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

-module(emqttd_lib_SUITE).

-compile(export_all).

-define(SOCKOPTS, [
	binary,
	{packet,    raw},
	{reuseaddr, true},
	{backlog,   512},
	{nodelay,   true}
]).

-define(PQ, priority_queue).

-define(BASE62, emqttd_base62).

all() -> [{group, guid}, {group, opts},
          {group, ?PQ}, {group, time},
          {group, node}, {group, base62}].

groups() ->
    [{guid, [], [guid_gen]},
     {opts, [], [opts_merge]},
     {?PQ,  [], [priority_queue_plen,
                 priority_queue_out2]},
     {time, [], [time_now_to_]},
     {node, [], [node_is_aliving, node_parse_name]},
     {base62, [], [base62_encode]}].

%%--------------------------------------------------------------------
%% emqttd_guid
%%--------------------------------------------------------------------

guid_gen(_) ->
    Guid1 = emqttd_guid:gen(),
    Guid2 = emqttd_guid:gen(),
    <<_:128>> = Guid1,
    true = (Guid2 >= Guid1),
    {Ts1, _, 0} = emqttd_guid:new(),
    Ts2 = emqttd_guid:timestamp(emqttd_guid:gen()),
    true = Ts2 > Ts1.

%%--------------------------------------------------------------------
%% emqttd_opts
%%--------------------------------------------------------------------

opts_merge(_) ->
    Opts = emqttd_opts:merge(?SOCKOPTS, [raw,
                                         binary,
                                         {backlog, 1024},
                                         {nodelay, false},
                                         {max_clients, 1024},
                                         {acceptors, 16}]),
    1024 = proplists:get_value(backlog, Opts),
    1024 = proplists:get_value(max_clients, Opts),
    [binary, raw,
     {acceptors, 16},
     {backlog, 1024},
     {max_clients, 1024},
     {nodelay, false},
     {packet, raw},
     {reuseaddr, true}] = lists:sort(Opts).

%%--------------------------------------------------------------------
%% priority_queue
%%--------------------------------------------------------------------

priority_queue_plen(_) ->
    Q = ?PQ:new(),
    0 = ?PQ:plen(0, Q),
    Q0 = ?PQ:in(z, Q),
    1 = ?PQ:plen(0, Q0),
    Q1 = ?PQ:in(x, 1, Q0),
    1 = ?PQ:plen(1, Q1),
    Q2 = ?PQ:in(y, 2, Q1),
    1 = ?PQ:plen(2, Q2),
    Q3 = ?PQ:in(z, 2, Q2),
    2 = ?PQ:plen(2, Q3),
    {_, Q4} = ?PQ:out(1, Q3),
    0 = ?PQ:plen(1, Q4),
    {_, Q5} = ?PQ:out(Q4),
    1 = ?PQ:plen(2, Q5),
    {_, Q6} = ?PQ:out(Q5),
    0 = ?PQ:plen(2, Q6),
    1 = ?PQ:len(Q6),
    {_, Q7} = ?PQ:out(Q6),
    0 = ?PQ:len(Q7).

priority_queue_out2(_) ->
    Els = [a, {b, 1}, {c, 1}, {d, 2}, {e, 2}, {f, 2}],
    Q  = ?PQ:new(),
    Q0 = lists:foldl(
            fun({El, P}, Acc) ->
                    ?PQ:in(El, P, Acc);
                (El, Acc) ->
                    ?PQ:in(El, Acc)
            end, Q, Els),
    {Val, Q1} = ?PQ:out(Q0),
    {value, d} = Val,
    {Val1, Q2} = ?PQ:out(2, Q1),
    {value, e} = Val1,
    {Val2, Q3} = ?PQ:out(1, Q2),
    {value, b} = Val2,
    {Val3, Q4} = ?PQ:out(Q3),
    {value, f} = Val3,
    {Val4, Q5} = ?PQ:out(Q4),
    {value, c} = Val4,
    {Val5, Q6} = ?PQ:out(Q5),
    {value, a} = Val5,
    {empty, _Q7} = ?PQ:out(Q6).

%%--------------------------------------------------------------------
%% emqttd_time
%%--------------------------------------------------------------------

time_now_to_(_) ->
    emqttd_time:seed(),
    emqttd_time:now_to_secs(),
    emqttd_time:now_to_ms().

%%--------------------------------------------------------------------
%% emqttd_node
%%--------------------------------------------------------------------

node_is_aliving(_) ->
    io:format("Node: ~p~n", [node()]),
    true = emqttd_node:is_aliving(node()),
    false = emqttd_node:is_aliving('x@127.0.0.1').

node_parse_name(_) ->
    'a@127.0.0.1' = emqttd_node:parse_name("a@127.0.0.1"),
    'b@127.0.0.1' = emqttd_node:parse_name("b").

%%--------------------------------------------------------------------
%% base62 encode decode
%%--------------------------------------------------------------------

base62_encode(_) ->
    10 = ?BASE62:decode(?BASE62:encode(10)),
    100 = ?BASE62:decode(?BASE62:encode(100)),
    9999 = ?BASE62:decode(?BASE62:encode(9999)),
    65535 = ?BASE62:decode(?BASE62:encode(65535)),
    <<X:128/unsigned-big-integer>> = emqttd_guid:gen(),
    <<Y:128/unsigned-big-integer>> = emqttd_guid:gen(),
    X = ?BASE62:decode(?BASE62:encode(X)),
    Y = ?BASE62:decode(?BASE62:encode(Y)).

