%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authn_initializer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("emqx_authn.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_authn], fun set_special_configs/1),
    Config.

set_special_configs(emqx_authn) ->
    Config = #{
               <<"mechanism">> => <<"password-based">>,
               <<"backend">> => <<"built-in-database">>,
               <<"user_id_type">> => <<"username">>,
               <<"password_hash_algorithm">> => #{
                                                  <<"name">> => <<"sha256">>
                                                 },
               <<"enable">> => <<"true">>},
    emqx_config:put_raw([<<"authentication">>], Config),
    emqx_config:put_raw([<<"listeners">>, <<"ssl">>, <<"default">>, <<"authentication">>], Config),
    ok;

set_special_configs(_App) ->
    ok.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_authn]),
    ok.

t_config_reinitialization(_Config) ->
    {ok, Chains0} = ?AUTHN:list_chain_names(),
    ?assertEqual([?GLOBAL, 'ssl:default'], lists:sort(Chains0)),

    erlang:exit(whereis(?AUTHN), kill),

    timer:sleep(10),
    {ok, Chains1} = ?AUTHN:list_chain_names(),
    ?assertEqual([?GLOBAL, 'ssl:default'], lists:sort(Chains1)).

t_idempotency(_Config) ->
    ok = emqx_authn_initializer:initialize(),

    [Pid0] = [P || {emqx_authn_initializer, P, _, _} <- supervisor:which_children(emqx_authn_sup)],
    exit(Pid0, kill),
    timer:sleep(20),

    [Pid1] = [P || {emqx_authn_initializer, P, _, _} <- supervisor:which_children(emqx_authn_sup)],
    {current_function,{erlang, hibernate, 3}} = erlang:process_info(Pid1, current_function).

t_register_and_die(_Config) ->
    InitFun = fun(_Pid) ->
                      timer:sleep(10)
              end,

    {ok, _} = emqx_authn_initializer:start_link(
                #{
                  interval => 5,
                  init_fun => InitFun,
                  name => test_process
                 }),

    ?check_trace(
       begin
           ?wait_async_action(
              spawn(fun() ->
                            register(test_process, self()),
                            timer:sleep(20)
                    end),
              #{?snk_kind := emqx_authn_initializer_process_died, name := test_process},
              1000)
       end,
       fun(_, Trace) ->
               ?assert(
                  ?strict_causality(
                     #{?snk_kind := emqx_authn_initializer_process_registered,
                       name := test_process},
                     #{?snk_kind := emqx_authn_initializer_process_initialized,
                       name := test_process},
                     Trace)),

               ?assert(
                  ?strict_causality(
                     #{?snk_kind := emqx_authn_initializer_process_initialized,
                       name := test_process},
                     #{?snk_kind := emqx_authn_initializer_process_died,
                       name := test_process},
                     Trace))
       end).
