%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_ldap).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("eldap/include/eldap.hrl").

-behaviour(emqx_dashboard_sso).

-export([
    namespace/0,
    fields/1,
    desc/1
]).

-export([
    hocon_ref/0,
    login_ref/0,
    login/2,
    create/1,
    update/2,
    destroy/1,
    convert_certs/2
]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() ->
    "sso".

hocon_ref() ->
    hoconsc:ref(?MODULE, ldap).

login_ref() ->
    hoconsc:ref(?MODULE, login).

fields(ldap) ->
    emqx_dashboard_sso_schema:common_backend_schema([ldap]) ++
        [
            {query_timeout, fun query_timeout/1}
        ] ++
        adjust_ldap_fields(emqx_ldap:fields(config));
fields(login) ->
    [
        emqx_dashboard_sso_schema:backend_schema([ldap])
        | emqx_dashboard_sso_schema:username_password_schema()
    ].

query_timeout(type) -> emqx_schema:timeout_duration_ms();
query_timeout(desc) -> ?DESC(?FUNCTION_NAME);
query_timeout(default) -> <<"5s">>;
query_timeout(_) -> undefined.

desc(ldap) ->
    "LDAP";
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(Config0) ->
    ResourceId = emqx_dashboard_sso_manager:make_resource_id(ldap),
    {Config, State} = parse_config(Config0),
    case emqx_dashboard_sso_manager:create_resource(ResourceId, emqx_ldap, Config) of
        {ok, _} ->
            {ok, State#{resource_id => ResourceId}};
        {error, _} = Error ->
            Error
    end.

update(Config0, #{resource_id := ResourceId} = _State) ->
    {Config, NState} = parse_config(Config0),
    case emqx_dashboard_sso_manager:update_resource(ResourceId, emqx_ldap, Config) of
        {ok, _} ->
            {ok, NState#{resource_id => ResourceId}};
        {error, _} = Error ->
            Error
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

parse_config(Config0) ->
    Config = ensure_bind_password(Config0),
    {Config, maps:with([query_timeout], Config0)}.

%% In this feature, the `bind_password` is fixed, so it should conceal from the swagger,
%% but the connector still needs it, hence we should add it back here
ensure_bind_password(Config) ->
    Config#{method => #{type => bind, bind_password => <<"${password}">>}}.

adjust_ldap_fields(Fields) ->
    lists:map(fun adjust_ldap_field/1, Fields).

adjust_ldap_field({base_dn, Meta}) ->
    {base_dn, maps:remove(example, Meta)};
adjust_ldap_field({filter, Meta}) ->
    Default = <<"(& (objectClass=person) (uid=${username}))">>,
    {filter, Meta#{
        desc => ?DESC(filter),
        default => Default,
        example => Default
    }};
adjust_ldap_field(Any) ->
    Any.

login(
    #{body := #{<<"username">> := Username} = Sign} = _Req,
    #{
        query_timeout := Timeout,
        resource_id := ResourceId
    } = _State
) ->
    case
        emqx_resource:simple_sync_query(
            ResourceId,
            {query, Sign, [], Timeout}
        )
    of
        {ok, []} ->
            {error, user_not_found};
        {ok, [Entry]} ->
            case
                emqx_resource:simple_sync_query(
                    ResourceId,
                    {bind, Entry#eldap_entry.object_name, Sign}
                )
            of
                {ok, #{result := ok}} ->
                    ensure_user_exists(Username);
                {ok, #{result := 'invalidCredentials'} = Reason} ->
                    {error, Reason};
                {error, _Reason} ->
                    %% All error reasons are logged in resource buffer worker
                    {error, ldap_bind_query_failed}
            end;
        {error, _Reason} ->
            %% All error reasons are logged in resource buffer worker
            {error, ldap_query_failed}
    end.

ensure_user_exists(Username) ->
    case emqx_dashboard_admin:lookup_user(ldap, Username) of
        [User] ->
            emqx_dashboard_token:sign(User, <<>>);
        [] ->
            case emqx_dashboard_admin:add_sso_user(ldap, Username, ?ROLE_VIEWER, <<>>) of
                {ok, _} ->
                    ensure_user_exists(Username);
                Error ->
                    Error
            end
    end.

convert_certs(Dir, Conf) ->
    case
        emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
            Dir, maps:get(<<"ssl">>, Conf, undefined)
        )
    of
        {ok, SSL} ->
            new_ssl_source(Conf, SSL);
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config"}),
            throw({bad_ssl_config, Reason})
    end.

new_ssl_source(Source, undefined) ->
    Source;
new_ssl_source(Source, SSL) ->
    Source#{<<"ssl">> => SSL}.
