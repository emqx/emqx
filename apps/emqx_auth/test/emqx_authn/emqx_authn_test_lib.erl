%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_test_lib).

-include("emqx_authn.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_common_test_helpers, [on_exit/1]).

authenticator_example(Id) ->
    #{Id := #{value := Example}} = emqx_authn_api:authenticator_examples(),
    Example.

http_example() ->
    authenticator_example('password_based:http').

built_in_database_example() ->
    authenticator_example('password_based:built_in_database').

jwt_example() ->
    authenticator_example(jwt).

delete_authenticators(Path, Chain) ->
    case emqx_authn_chains:list_authenticators(Chain) of
        {error, _} ->
            ok;
        {ok, Authenticators} ->
            lists:foreach(
                fun(#{id := ID}) ->
                    emqx:update_config(
                        Path,
                        {delete_authenticator, Chain, ID},
                        #{rawconf_with_defaults => true}
                    )
                end,
                Authenticators
            )
    end.

delete_config(ID) ->
    {ok, _} =
        emqx:update_config(
            [authentication],
            {delete_authenticator, ?GLOBAL, ID},
            #{rawconf_with_defaults => false}
        ).

client_ssl_cert_opts() ->
    Dir = code:lib_dir(emqx_auth),
    #{
        <<"keyfile">> => filename:join([Dir, <<"test/data/certs">>, <<"client.key">>]),
        <<"certfile">> => filename:join([Dir, <<"test/data/certs">>, <<"client.crt">>]),
        <<"cacertfile">> => filename:join([Dir, <<"test/data/certs">>, <<"ca.crt">>])
    }.

register_fake_providers(ProviderTypes) ->
    Providers = [
        {ProviderType, emqx_authn_fake_provider}
     || ProviderType <- ProviderTypes
    ],
    emqx_authn_chains:register_providers(Providers).

deregister_providers() ->
    ProviderTypes = maps:keys(emqx_authn_chains:get_providers()),
    emqx_authn_chains:deregister_providers(ProviderTypes).

enable_node_cache(Enable) ->
    {ok, _} = emqx:update_config(
        [authentication_settings, node_cache],
        #{<<"enable">> => Enable}
    ),
    ok.

-doc """
Checks that, if an authentication backend returns the `clientid_override` attribute, it's
used to override.
""".
t_clientid_override(TCConfig, Opts) when is_list(TCConfig) ->
    #{
        mk_config_fn := MkConfigFn,
        overridden_clientid := OverriddenClientId
    } = Opts,
    PostConfigFn = maps:get(post_config_fn, Opts, fun() -> ok end),
    ClientOpts = maps:get(client_opts, Opts, #{}),
    Config = MkConfigFn(),
    on_exit(fun() -> _ = emqx_authn_test_lib:delete_authenticators([?CONF_NS_ATOM], ?GLOBAL) end),
    {ok, _} = emqx:update_config(
        [?CONF_NS_ATOM],
        {create_authenticator, ?GLOBAL, Config}
    ),
    PostConfigFn(),
    OriginalClientId = <<"original_clientid">>,
    {ok, C} = emqtt:start_link(ClientOpts#{clientid => OriginalClientId}),
    {ok, _} = emqtt:connect(C),
    %% We use the clientid override internally.
    ?assertMatch([OverriddenClientId], emqx_cm:all_client_ids()),
    %% We don't return `'Assigned-Client-Identifier'` in `CONNACK` properties because the
    %% client did not specify an empty clientid.
    ?assertMatch(OriginalClientId, proplists:get_value(clientid, emqtt:info(C))),
    ok = emqtt:stop(C),
    ok.
