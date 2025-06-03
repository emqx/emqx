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
        emqx_ldap:fields(config) ++
        adjust_ldap_fields(emqx_ldap:fields(search_options));
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
    ?DESC("LDAP");
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(Config) ->
    ResourceId = emqx_dashboard_sso_manager:make_resource_id(ldap),
    maybe
        {ok, State} ?= parse_config(Config),
        {ok, _} ?=
            emqx_dashboard_sso_manager:create_resource(ResourceId, emqx_ldap_connector, Config),
        {ok, State#{resource_id => ResourceId}}
    end.

update(Config, #{resource_id := ResourceId} = _State) ->
    maybe
        {ok, NState} ?= parse_config(Config),
        {ok, _} ?=
            emqx_dashboard_sso_manager:update_resource(ResourceId, emqx_ldap_connector, Config),
        {ok, NState#{resource_id => ResourceId}}
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

parse_config(#{base_dn := BaseDN, filter := Filter, query_timeout := QueryTimeout}) ->
    maybe
        {ok, BaseDNTemplate} ?= parse_dn(BaseDN),
        {ok, FilterTemplate} ?= parse_filter(Filter),
        State = #{
            base_dn_template => BaseDNTemplate,
            filter_template => FilterTemplate,
            query_timeout => QueryTimeout
        },
        {ok, State}
    end.

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
    #{body := #{<<"username">> := Username, <<"password">> := Password} = Sign} = _Req,
    #{
        query_timeout := Timeout,
        resource_id := ResourceId,
        base_dn_template := BaseDNTemplate,
        filter_template := FilterTemplate
    } = _State
) ->
    BaseDN = render_base_dn(BaseDNTemplate, Sign),
    Filter = render_filter(FilterTemplate, Sign),
    case
        emqx_resource:simple_sync_query(
            ResourceId,
            {query, BaseDN, Filter, [{timeout, Timeout}]}
        )
    of
        {ok, []} ->
            {error, user_not_found};
        {ok, [#eldap_entry{object_name = ObjectName}]} ->
            case
                emqx_resource:simple_sync_query(
                    ResourceId,
                    {bind, ObjectName, Password}
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

render_base_dn(BaseDNTemplate, Sign) ->
    emqx_ldap_dn:map_values(
        fun(Value) -> render(Value, Sign) end,
        BaseDNTemplate
    ).

render_filter(FilterTemplate, Sign) ->
    emqx_ldap_filter:map_values(
        fun(Value) -> render(Value, Sign) end,
        FilterTemplate
    ).

render(Value, Sign) ->
    {Rendered, []} = emqx_template:render(Value, Sign, #{var_trans => fun strings_to_unicode/2}),
    iodata_to_str(Rendered).

parse_filter(Filter) ->
    maybe
        {ok, ParsedFilter} ?= emqx_ldap_filter:parse(Filter),
        {ok, emqx_ldap_filter:map_values(fun emqx_template:parse/1, ParsedFilter)}
    end.

parse_dn(DN) ->
    maybe
        {ok, ParsedDN} ?= emqx_ldap_dn:parse(DN),
        {ok, emqx_ldap_dn:map_values(fun emqx_template:parse/1, ParsedDN)}
    end.

iodata_to_str(Iodata) ->
    binary_to_list(iolist_to_binary(Iodata)).

strings_to_unicode(_Name, Value) when is_binary(Value) ->
    Value;
strings_to_unicode(Name, Value) when is_list(Value) ->
    to_unicode_binary(Name, Value).

to_unicode_binary(Name, Value) when is_list(Value) ->
    try unicode:characters_to_binary(Value) of
        Encoded when is_binary(Encoded) ->
            Encoded;
        _ ->
            error({encode_error, {non_unicode_data, Name}})
    catch
        error:badarg ->
            error({encode_error, {non_unicode_data, Name}})
    end.
