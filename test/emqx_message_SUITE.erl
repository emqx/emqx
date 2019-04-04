%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_message_SUITE).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([ t_make/1
        , t_flag/1
        , t_header/1
        , t_format/1
        , t_expired/1
        , t_to_map/1
        ]).

-export([ all/0
        , suite/0
        ]).

t_make(_) ->
    Msg = emqx_message:make(<<"topic">>, <<"payload">>),
    ?assertEqual(0, emqx_message:qos(Msg)),
    ?assertEqual(undefined, emqx_message:from(Msg)),
    ?assertEqual(<<"payload">>, emqx_message:payload(Msg)),
    Msg1 = emqx_message:make(<<"clientid">>, <<"topic">>, <<"payload">>),
    ?assertEqual(0, emqx_message:qos(Msg1)),
    ?assertEqual(<<"topic">>, emqx_message:topic(Msg1)),
    Msg2 = emqx_message:make(<<"clientid">>, ?QOS_2, <<"topic">>, <<"payload">>),
    ?assert(is_binary(emqx_message:id(Msg2))),
    ?assertEqual(2, emqx_message:qos(Msg2)),
    ?assertEqual(<<"clientid">>, emqx_message:from(Msg2)),
    ?assertEqual(<<"topic">>, emqx_message:topic(Msg2)),
    ?assertEqual(<<"payload">>, emqx_message:payload(Msg2)).

t_flag(_) ->
    Msg = emqx_message:make(<<"clientid">>, <<"topic">>, <<"payload">>),
    Msg2 = emqx_message:set_flag(retain, false, Msg),
    Msg3 = emqx_message:set_flag(dup, Msg2),
    ?assert(emqx_message:get_flag(dup, Msg3)),
    ?assertNot(emqx_message:get_flag(retain, Msg3)),
    Msg4 = emqx_message:unset_flag(dup, Msg3),
    Msg5 = emqx_message:unset_flag(retain, Msg4),
    ?assertEqual(undefined, emqx_message:get_flag(dup, Msg5, undefined)),
    ?assertEqual(undefined, emqx_message:get_flag(retain, Msg5, undefined)),
    Msg6 = emqx_message:set_flags(#{dup => true, retain => true}, Msg5),
    ?assert(emqx_message:get_flag(dup, Msg6)),
    ?assert(emqx_message:get_flag(retain, Msg6)).

t_header(_) ->
    Msg = emqx_message:make(<<"clientid">>, <<"topic">>, <<"payload">>),
    Msg1 = emqx_message:set_headers(#{a => 1, b => 2}, Msg),
    Msg2 = emqx_message:set_header(c, 3, Msg1),
    ?assertEqual(1, emqx_message:get_header(a, Msg2)),
    ?assertEqual(4, emqx_message:get_header(d, Msg2, 4)),
    Msg3 = emqx_message:remove_header(a, Msg2),
    ?assertEqual(#{b => 2, c => 3}, emqx_message:get_headers(Msg3)).

t_format(_) ->
    io:format("~s", [emqx_message:format(emqx_message:make(<<"clientid">>, <<"topic">>, <<"payload">>))]).

t_expired(_) ->
    Msg = emqx_message:make(<<"clientid">>, <<"topic">>, <<"payload">>),
    Msg1 = emqx_message:set_headers(#{'Message-Expiry-Interval' => 1}, Msg),
    timer:sleep(500),
    ?assertNot(emqx_message:is_expired(Msg1)),
    timer:sleep(600),
    ?assert(emqx_message:is_expired(Msg1)),
    timer:sleep(1000),
    Msg2 = emqx_message:update_expiry(Msg1),
    ?assertEqual(1, emqx_message:get_header('Message-Expiry-Interval', Msg2)).

t_to_map(_) ->
    Msg = emqx_message:make(<<"clientid">>, ?QOS_1, <<"topic">>, <<"payload">>),
    List = [{id, Msg#message.id},
            {qos, ?QOS_1},
            {from, <<"clientid">>},
            {flags, #{dup => false}},
            {headers, #{}},
            {topic, <<"topic">>},
            {payload, <<"payload">>},
            {timestamp, Msg#message.timestamp}],
    ?assertEqual(List, emqx_message:to_list(Msg)),
    ?assertEqual(maps:from_list(List), emqx_message:to_map(Msg)).

all() ->
    IsTestCase = fun("t_" ++ _) -> true; (_) -> false end,
    [F || {F, _A} <- module_info(exports), IsTestCase(atom_to_list(F))].

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 30}}].

