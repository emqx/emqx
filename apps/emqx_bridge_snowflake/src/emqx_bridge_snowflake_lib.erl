%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_snowflake_lib).

%% API
-export([
    account_id_validator/1,
    common_ehttpc_pool_opts/1,
    jwt_config/2,
    http_pool_workers_healthy/2
]).

-include_lib("public_key/include/public_key.hrl").
-include_lib("emqx/include/logger.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

account_id_validator(AccountId) ->
    case binary:split(AccountId, <<"-">>) of
        [_, _] ->
            ok;
        _ ->
            {error, <<"Account identifier must be of form ORGID-ACCOUNTNAME">>}
    end.

common_ehttpc_pool_opts(Params) ->
    #{
        connect_timeout := ConnectTimeout,
        pipelining := Pipelining,
        max_inactive := MaxInactive,
        proxy := ProxyConfig0
    } = Params,
    TransportOpts = emqx_tls_lib:to_client_opts(#{enable => true, verify => verify_none}),
    ProxyConfig =
        case ProxyConfig0 of
            none ->
                [];
            #{host := ProxyHost, port := ProxyPort} ->
                [
                    {proxy, #{
                        host => str(ProxyHost),
                        port => ProxyPort
                    }}
                ]
        end,
    ProxyConfig ++
        [
            {connect_timeout, ConnectTimeout},
            {keepalive, 30_000},
            {transport, tls},
            {transport_opts, TransportOpts},
            {max_inactive, MaxInactive},
            {enable_pipelining, Pipelining}
        ].

jwt_config(ResId, Params) ->
    #{
        account := Account,
        private_key := PrivateKeyPEM,
        private_key_password := PrivateKeyPassword,
        pipe_user := PipeUser
    } = Params,
    PrivateJWK =
        case PrivateKeyPassword /= undefined of
            true ->
                jose_jwk:from_pem(
                    emqx_secret:unwrap(PrivateKeyPassword),
                    emqx_secret:unwrap(PrivateKeyPEM)
                );
            false ->
                jose_jwk:from_pem(emqx_secret:unwrap(PrivateKeyPEM))
        end,
    %% N.B.
    %% The account_identifier and user values must use all uppercase characters
    %% https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#using-key-pair-authentication
    AccountUp = string:uppercase(Account),
    PipeUserUp = string:uppercase(PipeUser),
    Fingerprint = fingerprint(PrivateJWK),
    Sub = iolist_to_binary([AccountUp, <<".">>, PipeUserUp]),
    Iss = iolist_to_binary([Sub, <<".">>, Fingerprint]),
    #{
        expiration => 360_000,
        resource_id => ResId,
        jwk => emqx_secret:wrap(PrivateJWK),
        iss => Iss,
        sub => Sub,
        aud => <<"unused">>,
        kid => <<"unused">>,
        alg => <<"RS256">>
    }.

http_pool_workers_healthy(HTTPPool, Timeout) ->
    Workers = [Worker || {_WorkerName, Worker} <- ehttpc:workers(HTTPPool)],
    DoPerWorker =
        fun(Worker) ->
            case ehttpc:health_check(Worker, Timeout) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "snowflake_ehttpc_health_check_failed",
                        pool => HTTPPool,
                        reason => Reason,
                        worker => Worker,
                        wait_time => Timeout
                    }),
                    {error, Reason}
            end
        end,
    try emqx_utils:pmap(DoPerWorker, Workers, Timeout) of
        [_ | _] = Status0 ->
            Errors = lists:filter(fun(St) -> St =/= ok end, Status0),
            case Errors of
                [] ->
                    ok;
                [Error | _] ->
                    {error, Error}
            end;
        [] ->
            {error, {<<"http_pool_initializing">>, HTTPPool}}
    catch
        exit:timeout ->
            {error, timeout}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

str(X) -> emqx_utils_conv:str(X).

fingerprint(PrivateJWK) ->
    {_, PublicRSAKey} = jose_jwk:to_public_key(PrivateJWK),
    #'SubjectPublicKeyInfo'{algorithm = DEREncoded} =
        public_key:pem_entry_encode('SubjectPublicKeyInfo', PublicRSAKey),
    Hash = crypto:hash(sha256, DEREncoded),
    Hash64 = base64:encode(Hash),
    <<"SHA256:", Hash64/binary>>.
