%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_messages_SUITE).

-include_lib("stdlib/include/assert.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(NOW,
    (calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}]))
).

-define(HERE(FMT, ARGS),
    io:format(
        user,
        "*** " ?MODULE_STRING ":~p/~p ~s @ ~p *** " ++ FMT ++ "~n",
        [?FUNCTION_NAME, ?FUNCTION_ARITY, ?NOW, node() | ARGS]
    )
).

all() ->
    [t_messages_persisted].

init_per_suite(Config) ->
    dbg:tracer(),
    dbg:p(all, c),
    dbg:tpl({emqx_persistent_session_ds, persist_session, '_'}, x),
    dbg:tpl({emqx_persistent_session_ds, discard_session, '_'}, x),
    dbg:tpl({emqx_persistent_session_ds, store_message, '_'}, x),
    dbg:tpl({emqx_persistent_session_ds, register_subscription, '_'}, x),
    dbg:tpl({emqx_persistent_session_ds, unregister_subscription, '_'}, x),
    {ok, _} = application:ensure_all_started(emqx_durable_storage),
    ok = emqx_common_test_helpers:start_apps([], fun
        (emqx) ->
            emqx_common_test_helpers:boot_modules(all),
            emqx_config:init_load(emqx_schema, <<"persistent_session_store.enabled = true">>),
            emqx_app:set_init_config_load_done();
        (_) ->
            ok
    end),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:ensure_mnesia_stopped(),
    ok.

t_messages_persisted(_Config) ->
    C1 = connect(<<?MODULE_STRING "1">>, true, 30),
    C2 = connect(<<?MODULE_STRING "2">>, false, 60),
    C3 = connect(<<?MODULE_STRING "3">>, false, undefined),
    C4 = connect(<<?MODULE_STRING "4">>, false, 0),

    CP = connect(<<?MODULE_STRING "-pub">>, true, undefined),

    {ok, _, [1]} = emqtt:subscribe(C1, <<"client/+/topic">>, qos1),
    {ok, _, [0]} = emqtt:subscribe(C2, <<"client/+/topic">>, qos0),
    {ok, _, [1]} = emqtt:subscribe(C2, <<"random/+">>, qos1),
    {ok, _, [2]} = emqtt:subscribe(C3, <<"client/#">>, qos2),
    {ok, _, [0]} = emqtt:subscribe(C4, <<"random/#">>, qos0),

    Messages = [
        M1 = {<<"client/1/topic">>, <<"1">>},
        M2 = {<<"client/2/topic">>, <<"2">>},
        M3 = {<<"client/3/topic/sub">>, <<"3">>},
        M4 = {<<"client/4">>, <<"4">>},
        M5 = {<<"random/5">>, <<"5">>},
        M6 = {<<"random/6/topic">>, <<"6">>},
        M7 = {<<"client/7/topic">>, <<"7">>},
        M8 = {<<"client/8/topic/sub">>, <<"8">>},
        M9 = {<<"random/9">>, <<"9">>},
        M10 = {<<"random/10">>, <<"10">>}
    ],

    Results = [emqtt:publish(CP, Topic, Payload, 1) || {Topic, Payload} <- Messages],

    ?HERE("Results = ~p", [Results]),

    Persisted = consume(<<"local">>, {['#'], 0}),

    ?HERE("Persisted = ~p", [Persisted]),

    ?assertEqual(
        [M1, M2, M5, M7, M9, M10],
        [{emqx_message:topic(M), emqx_message:payload(M)} || M <- Persisted]
    ),

    ok.

%%

connect(ClientId, CleanStart, EI) ->
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {clean_start, CleanStart},
        {properties,
            maps:from_list(
                [{'Session-Expiry-Interval', EI} || is_integer(EI)]
            )}
    ]),
    {ok, _} = emqtt:connect(Client),
    Client.

consume(Shard, Replay) ->
    {ok, It} = emqx_ds_storage_layer:make_iterator(Shard, Replay),
    consume(It).

consume(It) ->
    case emqx_ds_storage_layer:next(It) of
        {value, Val, NIt} ->
            [Val | consume(NIt)];
        none ->
            []
    end.
