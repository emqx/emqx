%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_api_settings).

-behaviour(minirest_api).

-include_lib("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([settings/2]).

-define(BAD_REQUEST, 'BAD_REQUEST').

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/authorization/settings"].

%%--------------------------------------------------------------------
%% Schema for each URI
%%--------------------------------------------------------------------

schema("/authorization/settings") ->
    #{
        'operationId' => settings,
        get =>
            #{
                description => ?DESC(authorization_settings_get),
                responses =>
                    #{200 => ref_authz_schema()}
            },
        put =>
            #{
                description => ?DESC(authorization_settings_put),
                'requestBody' => ref_authz_schema(),
                responses =>
                    #{
                        200 => ref_authz_schema(),
                        400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
                    }
            }
    }.

ref_authz_schema() ->
    emqx_schema:authz_fields().

settings(get, _Params) ->
    {200, authorization_settings()};
settings(put, #{body := Body}) ->
    #{
        <<"no_match">> := NoMatch,
        <<"deny_action">> := DenyAction,
        <<"cache">> := Cache
        %% We do not pass the body to emqx_conf:update_config/3 which
        %% fills the defaults. So we need to fill the defaults here
    } = emqx_schema:fill_defaults(ref_authz_schema(), Body),

    %% TODO
    %% This should be fixed, updating individual keys bypassing
    %% emqx_conf:update_config/3 is error-prone.
    {ok, _} = emqx_authz_utils:update_config([authorization, no_match], NoMatch),
    {ok, _} = emqx_authz_utils:update_config(
        [authorization, deny_action], DenyAction
    ),
    {ok, _} = emqx_authz_utils:update_config([authorization, cache], Cache),

    {200, authorization_settings()}.

authorization_settings() ->
    emqx_schema:fill_defaults(ref_authz_schema(), emqx_config:get_raw([authorization], #{})).
