%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_mfa_totp).

-behaviour(emqx_dashboard_mfa).

-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([
    namespace/0,
    fields/1,
    desc/1
]).

-export([
    config_ref/0,
    api_ref/1,
    create/1,
    update/2,
    destroy/1,
    initiate/2,
    verify/3,
    setup_user_mfa/1,
    generate_secret/0
]).

-define(MIN_SECRET_LEN, 20).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() ->
    "mfa.totp".

config_ref() ->
    hoconsc:ref(?MODULE, config).

api_ref(Field) ->
    hoconsc:ref(?MODULE, Field).

fields(config) ->
    [
        {enable,
            hoconsc:mk(boolean(), #{
                require => true, default => true, desc => ?DESC("enable")
            })},
        {interval_length,
            hoconsc:mk(integer(), #{
                require => true, default => 30, desc => ?DESC("interval_length")
            })},
        {token_length,
            hoconsc:mk(integer(), #{
                require => true, default => 6, desc => ?DESC("token_length")
            })}
    ];
fields(api_config) ->
    [
        emqx_dashboard_mfa_schema:method_schema(totp)
        | fields(config)
    ];
fields(second_login) ->
    [
        emqx_dashboard_mfa_schema:method_schema(totp),
        emqx_dashboard_mfa_schema:token_schema(),
        {code,
            hoconsc:mk(binary(), #{
                required => true,
                desc => ?DESC("code")
            })}
    ];
fields(first_login_resp) ->
    [
        emqx_dashboard_mfa_schema:method_schema(totp),
        emqx_dashboard_mfa_schema:token_schema(),
        {secret,
            hoconsc:mk(binary(), #{
                required => false,
                desc => ?DESC("secret")
            })}
    ];
fields(setup) ->
    [
        emqx_dashboard_mfa_schema:method_schema(totp),
        emqx_dashboard_api:field(password)
    ];
fields(setup_resp) ->
    [
        emqx_dashboard_mfa_schema:method_schema(totp),
        {secret,
            emqx_schema_secret:mk(#{
                required => true,
                desc => ?DESC("secret")
            })}
    ].

desc(config) ->
    "Configuration";
desc(login) ->
    "Login Parameters";
desc(lgoin_resp) ->
    "Login Response";
desc(setup) ->
    "User MFA Setup Parameters";
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_Config) ->
    {ok, #{}}.

update(_Config, _State) ->
    {ok, #{}}.

destroy(_State) ->
    ok.

initiate(_Username, _Settings) ->
    {ok, #{}}.

verify(#{<<"code">> := Code}, #{secret := Secret}, _Context) ->
    Secret2 = emqx_secret:unwrap(Secret),
    %% TODO
    %% if we introduce a stateful MFA method in the future,
    %% we should get this `options` from the MFA state/context
    Options = emqx:get_config([dashboard, mfa, totp]),
    try
        case pot:valid_totp(Code, Secret2, maps:to_list(Options)) of
            true ->
                ok;
            false ->
                error
        end
    catch
        _:Reason ->
            ?SLOG(debug, #{
                msg => "totp_verification_error ",
                reason => Reason
            }),
            error
    end.

setup_user_mfa(_) ->
    Secret = generate_secret(),
    Settings = #{method => totp, secret => emqx_secret:wrap(Secret)},
    {ok, Settings, Settings#{secret := Secret}}.

generate_secret() ->
    pot_base32:encode(emqx_utils:gen_id(?MIN_SECRET_LEN)).
