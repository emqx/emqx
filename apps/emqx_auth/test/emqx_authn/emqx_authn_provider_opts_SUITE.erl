%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Verifies that emqx_authn dispatch consults the
%% `support_user_operations' flag declared at registration time, instead
%% of probing `erlang:function_exported/3' on every dispatch.
%%
%% This SUITE doubles as the provider fixture: it implements the
%% mandatory `emqx_authn_provider' callbacks plus one optional callback
%% (`add_user/2'). Each test case registers it differently (with or
%% without `support_user_operations') and checks that dispatch matches
%% the declared capability — not whether `add_user/2' happens to be
%% exported (it always is).

-module(emqx_authn_provider_opts_SUITE).

-behaviour(emqx_authn_provider).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_authn_chains.hrl").

-define(CHAIN, 'mqtt:global').
-define(BACKEND, built_in_database).
-define(AUTHN_TYPE, {password_based, ?BACKEND}).
-define(AUTHENTICATOR_ID, <<"password_based:built_in_database">>).

%%------------------------------------------------------------------------------
%% emqx_authn_provider callbacks
%%------------------------------------------------------------------------------

create(_AuthenticatorID, _Config) ->
    {ok, #{mark => provider_state}}.

update(_Config, _State) ->
    {ok, #{mark => provider_state}}.

authenticate(#{username := <<"good">>}, _State) ->
    {ok, #{is_superuser => false}};
authenticate(_, _State) ->
    {error, bad_username_or_password}.

destroy(_State) ->
    ok.

%% Optional callback — always exported. Whether dispatch reaches it is
%% governed solely by the support_user_operations flag.
add_user(_UserInfo, _State) ->
    {ok, #{user_id => <<"u">>, is_superuser => false}}.

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_auth],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(Case, Config) ->
    Opts = testcase_provider_opts(Case),
    ok = emqx_authn_chains:register_providers([{?AUTHN_TYPE, ?MODULE, Opts}]),
    {ok, _} = emqx:update_config(
        [authentication],
        {create_authenticator, ?CHAIN, #{
            <<"mechanism">> => <<"password_based">>,
            <<"backend">> => <<"built_in_database">>,
            <<"user_id_type">> => <<"username">>
        }}
    ),
    Config.

end_per_testcase(_Case, _Config) ->
    _ = emqx:update_config(
        [authentication],
        {delete_authenticator, ?CHAIN, ?AUTHENTICATOR_ID}
    ),
    _ = emqx_authn_chains:deregister_providers([?AUTHN_TYPE]),
    ok.

testcase_provider_opts(t_user_ops_enabled_dispatches) ->
    #{support_user_operations => true};
testcase_provider_opts(_Case) ->
    #{}.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

%% Provider registered without support_user_operations must short-circuit
%% with unsupported_operation, even though add_user/2 IS exported. This
%% proves dispatch consults the flag, not erlang:function_exported/3.
t_user_ops_disabled_short_circuits(_Config) ->
    ?assertEqual(
        {error, unsupported_operation},
        emqx_authn_chains:add_user(?CHAIN, ?AUTHENTICATOR_ID, #{
            user_id => <<"u">>, password => <<"p">>
        })
    ).

%% Provider registered with support_user_operations => true dispatches
%% normally.
t_user_ops_enabled_dispatches(_Config) ->
    ?assertMatch(
        {ok, _},
        emqx_authn_chains:add_user(?CHAIN, ?AUTHENTICATOR_ID, #{
            user_id => <<"u">>, password => <<"p">>
        })
    ).

%% Regression: authenticate/2 is mandatory and is dispatched directly,
%% so it must work regardless of the support_user_operations flag.
t_authenticate_dispatches_to_provider(_Config) ->
    Credential = #{
        listener => 'tcp:default',
        protocol => mqtt,
        username => <<"good">>,
        password => <<"p">>
    },
    ?assertMatch({ok, _}, emqx_access_control:authenticate(Credential)).
