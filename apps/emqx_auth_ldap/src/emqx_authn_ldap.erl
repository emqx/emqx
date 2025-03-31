%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_ldap).

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include("emqx_auth_ldap.hrl").

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    do_create(Config).

do_create(Config0) ->
    ResourceId = emqx_authn_utils:make_resource_id(?AUTHN_BACKEND_BIN),
    Config = filter_placeholders(Config0),
    State = parse_config(Config),
    {ok, _Data} = emqx_authn_utils:create_resource(
        ResourceId, emqx_ldap, Config, ?AUTHN_MECHANISM_BIN, ?AUTHN_BACKEND_BIN
    ),
    {ok, State#{resource_id => ResourceId}}.

update(Config, #{resource_id := ResourceId} = _State) ->
    NState = parse_config(Config),
    case
        emqx_authn_utils:update_resource(
            emqx_ldap, Config, ResourceId, ?AUTHN_MECHANISM_BIN, ?AUTHN_BACKEND_BIN
        )
    of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, _} ->
            {ok, NState#{resource_id => ResourceId}}
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := undefined}, _) ->
    {error, bad_username_or_password};
authenticate(Credential, #{method := #{type := Type}} = State) ->
    case Type of
        hash ->
            emqx_authn_ldap_hash:authenticate(Credential, State);
        bind ->
            emqx_authn_ldap_bind:authenticate(Credential, State)
    end.

parse_config(
    #{base_dn := BaseDN, filter := Filter, query_timeout := QueryTimeout, method := Method}
) ->
    PasswordVars =
        case Method of
            #{bind_password := Password} -> emqx_auth_template:placeholder_vars_from_str(Password);
            _ -> []
        end,
    BaseDNVars = emqx_auth_template:placeholder_vars_from_str(BaseDN),
    FilterVars = emqx_auth_template:placeholder_vars_from_str(Filter),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(
        BaseDNVars ++ FilterVars ++ PasswordVars
    ),
    #{
        query_timeout => QueryTimeout,
        method => Method,
        cache_key_template => CacheKeyTemplate
    }.

filter_placeholders(#{base_dn := BaseDN0, filter := Filter0} = Config0) ->
    BaseDN = emqx_auth_template:escape_disallowed_placeholders_str(
        BaseDN0, ?AUTHN_DEFAULT_ALLOWED_VARS
    ),
    Filter = emqx_auth_template:escape_disallowed_placeholders_str(
        Filter0, ?AUTHN_DEFAULT_ALLOWED_VARS
    ),
    Config1 = filter_bind_password_placeholders(Config0),
    Config1#{base_dn => BaseDN, filter => Filter}.

filter_bind_password_placeholders(#{method := #{bind_password := Password0} = Method} = Config0) ->
    Password = emqx_auth_template:escape_disallowed_placeholders_str(
        Password0, ?AUTHN_DEFAULT_ALLOWED_VARS
    ),
    Config0#{method => Method#{bind_password => Password}};
filter_bind_password_placeholders(Config) ->
    Config.
