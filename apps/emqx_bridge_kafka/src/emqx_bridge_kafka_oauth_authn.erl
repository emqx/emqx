%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_oauth_authn).

%% API
-export([
    mk_token_callback/1,
    register/2,
    unregister/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

mk_token_callback(OAuth2ResourceId) ->
    fun(_Context) ->
        case emqx_connector_oauth2:get_token(OAuth2ResourceId) of
            {ok, Token} ->
                {ok, #{token => Token}};
            {error, Reason} ->
                {error, Reason}
        end
    end.

register(
    ClientId,
    #{
        mechanism := oauth,
        grant_type := client_credentials,
        endpoint_uri := Endpoint,
        client_id := OAuthClientId,
        client_secret := ClientSecret
    } = Opts
) ->
    Config = oauth2_config(Opts, Endpoint, OAuthClientId, ClientSecret),
    emqx_connector_oauth2:register(ClientId, Config);
register(_ClientId, _Auth) ->
    ok.

unregister(ClientId) ->
    emqx_connector_oauth2:unregister(ClientId).

oauth2_config(Opts, Endpoint, OAuthClientId, ClientSecret) ->
    Timeout = maps:get(timeout, Opts, 5_000),
    #{
        token_endpoint => Endpoint,
        client_id => OAuthClientId,
        client_secret => ClientSecret,
        scope => maps:get(scope, Opts, undefined),
        timeout => Timeout,
        connect_timeout => maps:get(connect_timeout, Opts, Timeout)
    }.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

oauth2_config_test() ->
    Secret = emqx_secret:wrap(<<"secret">>),
    Config = oauth2_config(
        #{
            scope => <<"scope">>,
            timeout => 10_000,
            connect_timeout => 2_000,
            extensions => #{<<"not">> => <<"forwarded">>}
        },
        <<"https://auth.example/token">>,
        <<"client">>,
        Secret
    ),
    ?assertEqual(
        #{
            token_endpoint => <<"https://auth.example/token">>,
            client_id => <<"client">>,
            client_secret => Secret,
            scope => <<"scope">>,
            timeout => 10_000,
            connect_timeout => 2_000
        },
        Config
    ).

-endif.
