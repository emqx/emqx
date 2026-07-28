%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_http).

-behaviour(emqx_authz_source).

%% AuthZ Callbacks
-export([
    create/1,
    update/2,
    destroy/1,
    authorize/4,
    format_for_api/1
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_auth_http.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(VAR_ACCESS, "access").
-define(LEGACY_SUBSCRIBE_ACTION, 1).
-define(LEGACY_PUBLISH_ACTION, 2).

-define(ALLOWED_VARS, [
    ?VAR_USERNAME,
    ?VAR_CLIENTID,
    ?VAR_PEERHOST,
    ?VAR_PEERPORT,
    ?VAR_PROTONAME,
    ?VAR_MOUNTPOINT,
    ?VAR_TOPIC,
    ?VAR_ACTION,
    ?VAR_CERT_SUBJECT,
    ?VAR_CERT_CN_NAME,
    ?VAR_CERT_PEM,
    ?VAR_ACCESS,
    ?VAR_NS_CLIENT_ATTRS,
    ?VAR_ZONE,
    ?VAR_LISTENER,
    ?VAR_QOS,
    ?VAR_RETAIN
]).

create(Source) ->
    ResourceId = emqx_authz_utils:make_resource_id(?AUTHZ_TYPE),
    State = new_state(ResourceId, Source),
    ok = create_resource(State),
    State.

update(#{resource_id := ResourceId} = _State, Source) ->
    State = new_state(ResourceId, Source),
    ok = update_resource(ResourceId, State),
    State;
update(_State, Source) ->
    %% The previous state had no connector resource (templated host URL).
    create(Source).

destroy(#{resource_id := ResourceId}) ->
    emqx_authz_utils:remove_resource(ResourceId);
destroy(_State) ->
    ok.

create_resource(#{resource_id := _} = State) ->
    emqx_authz_utils:create_resource(emqx_bridge_http_connector, State);
create_resource(_State) ->
    ok.

update_resource(_OldResourceId, #{resource_id := _} = State) ->
    emqx_authz_utils:update_resource(emqx_bridge_http_connector, State);
update_resource(OldResourceId, _State) ->
    %% Updated to a templated host URL: the old connector pool is no longer needed.
    emqx_authz_utils:remove_resource(OldResourceId).

authorize(Client, Action, Topic, #{type := http} = State) ->
    Values = client_vars(Client, Action, Topic),
    case emqx_auth_http_utils:generate_request(State, Values) of
        {ok, Request} ->
            handle_response(query(Values, Request, State), State);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "http_request_generation_failed",
                reason => Reason
            }),
            emqx_authz_utils:backend_failure_result()
    end.

query(
    Values,
    Request,
    #{
        resource_id := ResourceId,
        cache_key_template := CacheKeyTemplate,
        method := Method,
        request_timeout := RequestTimeout
    }
) ->
    CacheKey = emqx_auth_template:cache_key(Values, CacheKeyTemplate),
    emqx_authz_utils:cached_simple_sync_query(
        CacheKey,
        ResourceId,
        {Method, Request, RequestTimeout}
    );
query(
    Values,
    Request,
    #{
        one_off_base := OneOffBase,
        cache_key_template := CacheKeyTemplate,
        method := Method,
        request_timeout := RequestTimeout
    }
) ->
    CacheKey = emqx_auth_template:cache_key(Values, CacheKeyTemplate),
    emqx_authz_utils:cached_apply(CacheKey, fun() ->
        emqx_auth_http_utils:one_off_request(OneOffBase, Method, RequestTimeout, Values, Request)
    end).

handle_response(Response, State) ->
    case Response of
        {ok, 204, _Headers} ->
            {matched, allow};
        {ok, 200, Headers, Body} ->
            ContentType = emqx_authz_utils:content_type(Headers),
            case emqx_authz_utils:parse_http_resp_body(ContentType, Body) of
                error ->
                    ?SLOG(error, #{
                        msg => authz_http_response_incorrect,
                        content_type => ContentType,
                        body => <<"******">>
                    }),
                    emqx_authz_utils:backend_failure_result();
                {error, Reason} ->
                    ?tp(error, bad_authz_http_response, #{reason => Reason}),
                    emqx_authz_utils:backend_failure_result();
                Result ->
                    {matched, Result}
            end;
        {ok, Status, Headers} ->
            log_nomtach_msg(Status, Headers, undefined),
            nomatch;
        {ok, Status, Headers, Body} ->
            log_nomtach_msg(Status, Headers, Body),
            nomatch;
        {error, Reason} ->
            ?tp(authz_http_request_failure, #{error => Reason}),
            ?SLOG(error, #{
                msg => "http_server_query_failed",
                resource => maps:get(resource_id, State, undefined),
                reason => Reason
            }),
            emqx_authz_utils:backend_failure_result()
    end.

format_for_api(#{<<"headers">> := Headers} = Source) ->
    NewHeaders =
        case Source of
            #{<<"method">> := <<"get">>} ->
                emqx_auth_http_utils:convert_headers_no_content_type(Headers);
            #{<<"method">> := <<"post">>} ->
                emqx_auth_http_utils:convert_headers(Headers);
            _ ->
                Headers
        end,
    Source#{<<"headers">> => NewHeaders};
format_for_api(Source) ->
    Source.

log_nomtach_msg(Status, Headers, _Body) ->
    ?SLOG(
        debug,
        #{
            msg => unexpected_authz_http_response,
            status => Status,
            content_type => emqx_authz_utils:content_type(Headers),
            body => <<"******">>
        }
    ).

check_oauth2_headers_conflict(Headers, Oauth2) ->
    case emqx_connector_oauth2_schema:validate(Headers, Oauth2) of
        ok ->
            ok;
        {error, #{message := Msg}} ->
            throw(#{kind => validation_error, reason => Msg})
    end.

new_state(ResourceId, #{url := RawUrl, headers := Headers0} = Source) ->
    ok = check_oauth2_headers_conflict(Headers0, maps:get(oauth2, Source, undefined)),
    Resolution = maps:get(hostname_resolution, Source, static),
    case emqx_auth_http_utils:parse_url_template(RawUrl, Resolution) of
        {pooled, {RequestBase, Path, Query}} ->
            ok = check_pool_size_for_pooled(Source),
            {Vars, StateBase} = parse_templates(Path, Query, Source),
            ResourceConfig = emqx_authz_utils:cleanup_resource_config(
                [url, method, request_timeout, body, allowed_hosts, hostname_resolution],
                Source#{
                    request_base => RequestBase,
                    pool_type => random
                }
            ),
            emqx_authz_utils:init_state(Source, StateBase#{
                resource_config => ResourceConfig,
                resource_id => ResourceId,
                cache_key_template => emqx_auth_template:cache_key_template(Vars)
            });
        {static_host, #{
            scheme := Scheme, host := Host, port := Port, path := Path, query := Query
        }} ->
            ok = check_no_oauth2(Source),
            {Vars, StateBase} = parse_templates(Path, Query, Source),
            OneOffBase = (one_off_base(Scheme, Port, Source))#{static_host => Host},
            emqx_authz_utils:init_state(Source, StateBase#{
                one_off_base => OneOffBase,
                cache_key_template => emqx_auth_template:cache_key_template(Vars)
            });
        {templated_host, #{
            scheme := Scheme,
            host_template := HostTemplateStr,
            port := Port,
            path := Path,
            query := Query
        }} ->
            ok = check_dynamic_resolution(Resolution),
            ok = check_no_oauth2(Source),
            AllowedHosts = allowed_hosts(Source),
            {HostVars, HostTemplate} = emqx_auth_template:parse_str(
                HostTemplateStr, ?ALLOWED_VARS
            ),
            {Vars, StateBase} = parse_templates(Path, Query, Source),
            OneOffBase = (one_off_base(Scheme, Port, Source))#{
                host_template => HostTemplate,
                allowed_hosts => AllowedHosts
            },
            emqx_authz_utils:init_state(Source, StateBase#{
                one_off_base => OneOffBase,
                cache_key_template => emqx_auth_template:cache_key_template(HostVars ++ Vars)
            })
    end.

one_off_base(Scheme, Port, Source) ->
    #{
        scheme => Scheme,
        port => Port,
        connect_timeout => maps:get(connect_timeout, Source, 15000),
        ssl_opts => one_off_ssl_opts(Scheme, Source),
        pool => emqx_auth_http_utils:ensure_pool(authz, pool_size(Source))
    }.

pool_size(Source) ->
    maps:get(pool_size, Source, 8).

check_pool_size_for_pooled(Source) ->
    case pool_size(Source) of
        0 ->
            throw(#{
                kind => validation_error,
                reason =>
                    <<"'pool_size' must be at least 1 when 'hostname_resolution' is 'static'">>
            });
        _ ->
            ok
    end.

check_dynamic_resolution(dynamic) ->
    ok;
check_dynamic_resolution(static) ->
    throw(#{
        kind => validation_error,
        reason =>
            <<"'hostname_resolution' must be set to 'dynamic' when the URL host contains template placeholders">>
    }).

parse_templates(
    Path,
    Query,
    #{
        method := Method,
        headers := Headers0,
        request_timeout := ReqTimeout
    } = Source
) ->
    {BasePathVars, BasePathTemplate} = emqx_auth_template:parse_str(Path, ?ALLOWED_VARS),
    {BaseQueryVars, BaseQueryTemplate} = emqx_auth_template:parse_deep(
        cow_qs:parse_qs(Query),
        ?ALLOWED_VARS
    ),
    {BodyVars, BodyTemplate} =
        emqx_auth_template:parse_deep(
            emqx_utils_maps:binary_key_map(maps:get(body, Source, #{})),
            ?ALLOWED_VARS
        ),
    Headers = maps:to_list(emqx_auth_http_utils:transform_header_name(Headers0)),
    {HeadersVars, HeadersTemplate} = emqx_authn_utils:parse_deep(Headers),
    Vars = BasePathVars ++ BaseQueryVars ++ BodyVars ++ HeadersVars,
    StateBase = #{
        method => Method,
        headers_template => HeadersTemplate,
        base_path_template => BasePathTemplate,
        base_query_template => BaseQueryTemplate,
        body_template => BodyTemplate,
        request_timeout => ReqTimeout
    },
    {Vars, StateBase}.

check_no_oauth2(#{oauth2 := #{enable := true}}) ->
    throw(#{
        kind => validation_error,
        reason => <<"OAuth2 authentication is not supported with a templated host URL">>
    });
check_no_oauth2(_Source) ->
    ok.

allowed_hosts(Source) ->
    case emqx_auth_http_utils:parse_allowed_hosts(maps:get(allowed_hosts, Source, [])) of
        [] ->
            throw(#{
                kind => validation_error,
                reason =>
                    <<"'allowed_hosts' must be configured when the URL host contains template placeholders">>
            });
        AllowedHosts ->
            AllowedHosts
    end.

one_off_ssl_opts(https, Source) ->
    SSLConf = maps:get(ssl, Source, #{}),
    emqx_tls_lib:to_client_opts(SSLConf#{enable => true});
one_off_ssl_opts(http, _Source) ->
    [].

client_vars(Client, Action, Topic) ->
    Vars = emqx_authz_utils:vars_for_rule_query(Client, Action),
    add_legacy_access_var(Vars#{topic => Topic}).

add_legacy_access_var(#{action := subscribe} = Vars) ->
    Vars#{access => ?LEGACY_SUBSCRIBE_ACTION};
add_legacy_access_var(#{action := publish} = Vars) ->
    Vars#{access => ?LEGACY_PUBLISH_ACTION};
add_legacy_access_var(Vars) ->
    Vars.
