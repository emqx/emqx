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

-module(emqx_mod_delayed_SUITE).

-import(emqx_mod_delayed, [on_message_publish/1]).

-compile(export_all).
-compile(nowarn_export_all).

-record(delayed_message, {key, msg}).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_modules], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_ct_helpers:stop_apps([emqx_modules]).

set_special_configs(emqx) ->
    AclFilePath = filename:join(["test", "emqx_SUITE_data", "acl.conf"]),
    application:set_env(emqx, modules, [{emqx_mod_delayed, []},
                                        {emqx_mod_acl_internal, [{acl_file, AclFilePath}]}]),
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false);
set_special_configs(_App) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_load_case(_) ->
    UnHooks = emqx_hooks:lookup('message.publish'),
    ?assertEqual([], UnHooks),
    _ = emqx_mod_delayed:load([]),
    Hooks = emqx_hooks:lookup('message.publish'),
    ?assertEqual(1, length(Hooks)),
    ok.

t_delayed_message(_) ->
    _ = emqx_mod_delayed:load([]),
    DelayedMsg = emqx_message:make(?MODULE, 1, <<"$delayed/1/publish">>, <<"delayed_m">>),
    ?assertEqual({stop, DelayedMsg#message{topic = <<"publish">>, headers = #{allow_publish => false}}}, on_message_publish(DelayedMsg)),

    Msg = emqx_message:make(?MODULE, 1, <<"no_delayed_msg">>, <<"no_delayed">>),
    ?assertEqual({ok, Msg}, on_message_publish(Msg)),

    [Key] = mnesia:dirty_all_keys(emqx_mod_delayed),
    [#delayed_message{msg = #message{payload = Payload}}] = mnesia:dirty_read({emqx_mod_delayed, Key}),
    ?assertEqual(<<"delayed_m">>, Payload),
    timer:sleep(5000),

    EmptyKey = mnesia:dirty_all_keys(emqx_mod_delayed),
    ?assertEqual([], EmptyKey),
    ok = emqx_mod_delayed:unload([]).

t_banned_delayed(_) ->
    _ = emqx_mod_delayed:load([]),
    ClientId1 = <<"bc1">>,
    ClientId2 = <<"bc2">>,

    Now = erlang:system_time(second),
    Who = {clientid, ClientId2},
    emqx_banned:create(#{
        who => Who,
        by => <<"test">>,
        reason => <<"test">>,
        at => Now,
        until => Now + 120
    }),

    snabbkaffe:start_trace(),

    {ok, SubRef} =
        snabbkaffe_collector:subscribe(?match_event(#{?snk_kind := ignore_delayed_message_publish}),
                                       _NEvents    = 2,
                                       _Timeout    = 10000,
                                      0),

    lists:foreach(
      fun(ClientId) ->
              Msg = emqx_message:make(ClientId, <<"$delayed/1/bc">>, <<"payload">>),
              emqx_mod_delayed:on_message_publish(Msg)
      end,
      [ClientId1, ClientId1, ClientId1, ClientId2, ClientId2]
     ),

    ?assertMatch({ok, [#{?snk_kind := ignore_delayed_message_publish},
                       #{?snk_kind := ignore_delayed_message_publish}
                      ]},
                 snabbkaffe_collector:receive_events(SubRef)),

    snabbkaffe:stop(),

    emqx_banned:delete(Who),
    timer:sleep(500),
    EmptyKey = mnesia:dirty_all_keys(emqx_mod_delayed),
    ?assertEqual([], EmptyKey),
    ok = emqx_mod_delayed:unload([]).
