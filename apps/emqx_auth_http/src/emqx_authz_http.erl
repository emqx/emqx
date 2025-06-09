%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    ok = emqx_authz_utils:create_resource(emqx_bridge_http_connector, State),
    State.

update(#{resource_id := ResourceId} = _State, Source) ->
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:update_resource(emqx_bridge_http_connector, State),
    State.

destroy(#{resource_id := ResourceId}) ->
    emqx_authz_utils:remove_resource(ResourceId).

authorize(
    Client,
    Action,
    Topic,
    #{
        type := http,
        resource_id := ResourceId,
        cache_key_template := CacheKeyTemplate,
        method := Method,
        request_timeout := RequestTimeout
    } = State
) ->
    Values = client_vars(Client, Action, Topic),
    case emqx_auth_http_utils:generate_request(State, Values) of
        {ok, Request} ->
            CacheKey = emqx_auth_template:cache_key(Values, CacheKeyTemplate),
            Response = emqx_authz_utils:cached_simple_sync_query(
                CacheKey,
                ResourceId,
                {Method, Request, RequestTimeout}
            ),
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
                                body => Body
                            }),
                            nomatch;
                        {error, Reason} ->
                            ?tp(error, bad_authz_http_response, #{reason => Reason}),
                            nomatch;
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
                        resource => ResourceId,
                        reason => Reason
                    }),
                    ignore
            end;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "http_request_generation_failed",
                reason => Reason
            }),
            ignore
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

log_nomtach_msg(Status, Headers, Body) ->
    ?SLOG(
        debug,
        #{
            msg => unexpected_authz_http_response,
            status => Status,
            content_type => emqx_authz_utils:content_type(Headers),
            body => Body
        }
    ).

new_state(
    ResourceId,
    #{
        url := RawUrl,
        method := Method,
        headers := Headers0,
        request_timeout := ReqTimeout
    } = Source
) ->
    {RequestBase, Path, Query} = emqx_auth_http_utils:parse_url(RawUrl),
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
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    ResourceConfig = emqx_authz_utils:resource_config(
        [url, method, request_timeout, body],
        Source#{
            request_base => RequestBase,
            pool_type => random
        }
    ),
    emqx_authz_utils:init_state(Source, #{
        resource_config => ResourceConfig,
        resource_id => ResourceId,
        method => Method,
        headers_template => HeadersTemplate,
        base_path_template => BasePathTemplate,
        base_query_template => BaseQueryTemplate,
        body_template => BodyTemplate,
        request_timeout => ReqTimeout,
        cache_key_template => CacheKeyTemplate
    }).

client_vars(Client, Action, Topic) ->
    Vars = emqx_authz_utils:vars_for_rule_query(Client, Action),
    add_legacy_access_var(Vars#{topic => Topic}).

add_legacy_access_var(#{action := subscribe} = Vars) ->
    Vars#{access => ?LEGACY_SUBSCRIBE_ACTION};
add_legacy_access_var(#{action := publish} = Vars) ->
    Vars#{access => ?LEGACY_PUBLISH_ACTION};
add_legacy_access_var(Vars) ->
    Vars.
