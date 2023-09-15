%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ldap_authn_bind).

-include_lib("emqx_authn/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("eldap/include/eldap.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([
    namespace/0,
    tags/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    refs/0,
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn".

tags() ->
    [<<"Authentication">>].

%% used for config check when the schema module is resolved
roots() ->
    [{?CONF_NS, hoconsc:mk(hoconsc:ref(?MODULE, ldap_bind))}].

fields(ldap_bind) ->
    [
        {mechanism, emqx_authn_schema:mechanism(password_based)},
        {backend, emqx_authn_schema:backend(ldap_bind)},
        {query_timeout, fun query_timeout/1}
    ] ++
        emqx_authn_schema:common_fields() ++
        emqx_ldap:fields(config) ++ emqx_ldap:fields(bind_opts).

desc(ldap_bind) ->
    ?DESC(ldap_bind);
desc(_) ->
    undefined.

query_timeout(type) -> emqx_schema:timeout_duration_ms();
query_timeout(desc) -> ?DESC(?FUNCTION_NAME);
query_timeout(default) -> <<"5s">>;
query_timeout(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [hoconsc:ref(?MODULE, ldap_bind)].

create(_AuthenticatorID, Config) ->
    emqx_ldap_authn:do_create(?MODULE, Config).

update(Config, State) ->
    emqx_ldap_authn:update(Config, State).

destroy(State) ->
    emqx_ldap_authn:destroy(State).

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := undefined}, _) ->
    {error, bad_username_or_password};
authenticate(
    #{password := _Password} = Credential,
    #{
        query_timeout := Timeout,
        resource_id := ResourceId
    } = _State
) ->
    case
        emqx_resource:simple_sync_query(
            ResourceId,
            {query, Credential, [], Timeout}
        )
    of
        {ok, []} ->
            ignore;
        {ok, [_Entry | _]} ->
            case
                emqx_resource:simple_sync_query(
                    ResourceId,
                    {bind, Credential}
                )
            of
                ok ->
                    {ok, #{is_superuser => false}};
                {error, Reason} ->
                    ?TRACE_AUTHN_PROVIDER(error, "ldap_bind_failed", #{
                        resource => ResourceId,
                        reason => Reason
                    }),
                    {error, bad_username_or_password}
            end;
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "ldap_query_failed", #{
                resource => ResourceId,
                timeout => Timeout,
                reason => Reason
            }),
            ignore
    end.
