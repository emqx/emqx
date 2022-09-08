%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%%=================================================================================
%% CT boilerplate
%%=================================================================================

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

%%=================================================================================
%% Helpers fns
%%=================================================================================

%%=================================================================================
%% Testcases
%%=================================================================================

t_will_message_connection_denied({init, Config}) ->
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_authn]),
    mria:clear_table(emqx_authn_mnesia),
    AuthnConfig = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"clientid">>
    },
    Chain = 'mqtt:global',
    emqx:update_config(
        [authentication],
        {create_authenticator, Chain, AuthnConfig}
    ),
    User = #{user_id => <<"subscriber">>, password => <<"p">>},
    AuthenticatorID = <<"password_based:built_in_database">>,
    {ok, _} = emqx_authentication:add_user(
        Chain,
        AuthenticatorID,
        User
    ),
    Config;
t_will_message_connection_denied({'end', _Config}) ->
    emqx:update_config(
        [authentication],
        {delete_authenticator, 'mqtt:global', <<"password_based:built_in_database">>}
    ),
    emqx_common_test_helpers:stop_apps([emqx_authn, emqx_conf]),
    mria:clear_table(emqx_authn_mnesia),
    ok;
t_will_message_connection_denied(Config) when is_list(Config) ->
    {ok, Subscriber} = emqtt:start_link([
        {clientid, <<"subscriber">>},
        {password, <<"p">>}
    ]),
    {ok, _} = emqtt:connect(Subscriber),
    {ok, _, [?RC_SUCCESS]} = emqtt:subscribe(Subscriber, <<"lwt">>),

    process_flag(trap_exit, true),

    {ok, Publisher} = emqtt:start_link([
        {clientid, <<"publisher">>},
        {will_topic, <<"lwt">>},
        {will_payload, <<"should not be published">>}
    ]),
    snabbkaffe:start_trace(),
    ?wait_async_action(
        {error, _} = emqtt:connect(Publisher),
        #{?snk_kind := channel_terminated}
    ),
    snabbkaffe:stop(),

    receive
        {publish, #{
            topic := <<"lwt">>,
            payload := <<"should not be published">>
        }} ->
            ct:fail("should not publish will message")
    after 0 ->
        ok
    end,

    ok.
