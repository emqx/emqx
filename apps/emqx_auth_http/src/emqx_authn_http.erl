%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_http).

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-export([
    create_state/2,
    generate_request/2,
    request_for_log/2,
    response_for_log/1,
    extract_auth_data/2,
    safely_parse_body/2
]).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_auth_http.hrl").

-define(DEFAULT_CONTENT_TYPE, <<"application/json">>).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    create(Config).

create(Config) ->
    maybe
        ResourceId = emqx_authn_utils:make_resource_id(?AUTHN_BACKEND_BIN),
        {ok, ResourceConfig, State} ?= create_state(ResourceId, Config),
        ok ?=
            emqx_authn_utils:create_resource(
                emqx_bridge_http_connector,
                ResourceConfig,
                State,
                ?AUTHN_MECHANISM_BIN,
                ?AUTHN_BACKEND_BIN
            ),
        {ok, State}
    end.

update(Config0, #{resource_id := ResourceId} = _State) ->
    maybe
        {ok, ResourceConfig, State} ?= create_state(ResourceId, Config0),
        ok ?=
            emqx_authn_utils:update_resource(
                emqx_bridge_http_connector,
                ResourceConfig,
                State,
                ?AUTHN_MECHANISM_BIN,
                ?AUTHN_BACKEND_BIN
            ),
        {ok, State}
    end.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(
    Credential,
    #{
        resource_id := ResourceId,
        method := Method,
        request_timeout := RequestTimeout,
        cache_key_template := CacheKeyTemplate
    } = State
) ->
    case generate_request(Credential, State) of
        {ok, Request} ->
            CacheKey = emqx_auth_template:cache_key(Credential, CacheKeyTemplate),
            Response = emqx_authn_utils:cached_simple_sync_query(
                CacheKey,
                ResourceId,
                {Method, Request, RequestTimeout}
            ),
            ?TRACE_AUTHN_PROVIDER("http_response", #{
                request => request_for_log(Credential, State),
                response => response_for_log(Response),
                resource => ResourceId
            }),
            case Response of
                {ok, 204, _Headers} ->
                    {ok, #{is_superuser => false}};
                {ok, 200, Headers, Body} ->
                    handle_response(Headers, Body);
                {ok, _StatusCode, _Headers} = Response ->
                    ignore;
                {ok, _StatusCode, _Headers, _Body} = Response ->
                    ignore;
                {error, _Reason} ->
                    ignore
            end;
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(
                error,
                "generate_http_request_failed",
                #{reason => Reason, credential => emqx_authn_utils:without_password(Credential)}
            ),
            ignore
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

create_state(ResourceId, Config) ->
    Pipeline = [
        fun check_ssl_opts/1,
        fun normalize_headers/1,
        fun check_method_headers/1,
        fun parse_config/1
    ],
    maybe
        {ok, ResourceConfig, State} ?= emqx_utils:pipeline(Pipeline, Config, undefined),
        {ok, ResourceConfig, State#{resource_id => ResourceId}}
    end.

check_ssl_opts(#{url := <<"https://", _/binary>>, ssl := #{enable := false}}) ->
    {error,
        {invalid_ssl_opts,
            <<"it's required to enable the TLS option to establish a https connection">>}};
check_ssl_opts(_) ->
    ok.

normalize_headers(#{headers := Headers} = Config) ->
    {ok, Config#{headers => ensure_binary_names(Headers)}, undefined}.

check_method_headers(#{headers := Headers, method := get}) ->
    case maps:is_key(<<"content-type">>, Headers) of
        false ->
            ok;
        true ->
            {error, {invalid_headers, <<"HTTP GET requests cannot include content-type header.">>}}
    end;
check_method_headers(#{headers := Headers, method := post} = Config) ->
    {ok,
        Config#{
            headers =>
                maps:merge(#{<<"content-type">> => ?DEFAULT_CONTENT_TYPE}, Headers)
        },
        undefined}.

parse_config(
    #{
        method := Method,
        url := RawUrl,
        headers := Headers,
        request_timeout := RequestTimeout
    } = Config
) ->
    {RequestBase, Path, Query} = emqx_auth_http_utils:parse_url(RawUrl),
    {BasePathVars, BasePathTemplate} = emqx_authn_utils:parse_str(Path),
    {BaseQueryVars, BaseQueryTemplate} = emqx_authn_utils:parse_deep(
        cow_qs:parse_qs(Query)
    ),
    {BodyVars, BodyTemplate} = emqx_authn_utils:parse_deep(
        emqx_utils_maps:binary_key_map(maps:get(body, Config, #{}))
    ),
    {HeadersVars, HeadersTemplate} = emqx_authn_utils:parse_deep(maps:to_list(Headers)),
    Vars = BasePathVars ++ BaseQueryVars ++ BodyVars ++ HeadersVars,
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    State = emqx_authn_utils:init_state(Config, #{
        method => Method,
        path => Path,
        headers_template => HeadersTemplate,
        base_path_template => BasePathTemplate,
        base_query_template => BaseQueryTemplate,
        body_template => BodyTemplate,
        cache_key_template => CacheKeyTemplate,
        request_timeout => RequestTimeout,
        url => RawUrl
    }),
    ResourceConfig0 = emqx_authn_utils:cleanup_resource_config(
        [method, url, headers, request_timeout], Config
    ),
    ResourceConfig = ResourceConfig0#{
        request_base => RequestBase,
        pool_type => random
    },
    {ok, ResourceConfig, State}.

generate_request(Credential, State) ->
    emqx_auth_http_utils:generate_request(State, Credential).

handle_response(Headers, Body) ->
    ContentType = proplists:get_value(<<"content-type">>, Headers),
    case safely_parse_body(ContentType, Body) of
        {ok, NBody} ->
            body_to_auth_data(NBody);
        {error, _Reason} ->
            ignore
    end.

body_to_auth_data(Body) ->
    case maps:get(<<"result">>, Body, <<"ignore">>) of
        <<"allow">> ->
            extract_auth_data(http, Body);
        <<"deny">> ->
            {error, not_authorized};
        <<"ignore">> ->
            ignore;
        _ ->
            ignore
    end.

extract_auth_data(Source, Body) ->
    IsSuperuser = emqx_authn_utils:is_superuser(Body),
    Attrs = emqx_authn_utils:client_attrs(Body),
    try
        ExpireAt = expire_at(Body),
        ACL = acl(ExpireAt, Source, Body),
        ClientIdOverride = clientid_override(Body),
        Result = merge_maps([ExpireAt, IsSuperuser, ACL, ClientIdOverride, Attrs]),
        {ok, Result}
    catch
        throw:{bad_acl_rule, Reason} ->
            %% it's a invalid token, so ok to log
            ?TRACE_AUTHN_PROVIDER("bad_acl_rule", Reason#{http_body => Body}),
            {error, bad_username_or_password};
        throw:Reason ->
            ?TRACE_AUTHN_PROVIDER("bad_response_body", Reason#{http_body => Body}),
            {error, bad_username_or_password}
    end.

merge_maps([]) -> #{};
merge_maps([Map | Maps]) -> maps:merge(Map, merge_maps(Maps)).

%% Return either an empty map, or a map with `expire_at` at millisecond precision
%% Millisecond precision timestamp is required by `auth_expire_at`
%% emqx_channel:schedule_connection_expire/1
expire_at(Body) ->
    case expire_sec(Body) of
        undefined ->
            #{};
        Sec ->
            #{expire_at => erlang:convert_time_unit(Sec, second, millisecond)}
    end.

expire_sec(#{<<"expire_at">> := ExpireTime}) when is_integer(ExpireTime) ->
    Now = erlang:system_time(second),
    NowMs = erlang:convert_time_unit(Now, second, millisecond),
    case ExpireTime < Now of
        true ->
            throw(#{
                cause => "'expire_at' is in the past.",
                system_time => Now,
                expire_at => ExpireTime
            });
        false when ExpireTime > (NowMs div 2) ->
            throw(#{
                cause => "'expire_at' does not appear to be a Unix epoch time in seconds.",
                system_time => Now,
                expire_at => ExpireTime
            });
        false ->
            ExpireTime
    end;
expire_sec(#{<<"expire_at">> := _}) ->
    throw(#{cause => "'expire_at' is not an integer (Unix epoch time in seconds)."});
expire_sec(_) ->
    undefined.

acl(#{expire_at := ExpireTimeMs}, Source, #{<<"acl">> := Rules}) ->
    #{
        acl => #{
            source_for_logging => Source,
            rules => emqx_authz_rule_raw:parse_and_compile_rules(Rules),
            %% It's seconds level precision (like JWT) for authz
            %% see emqx_authz_client_info:check/1
            expire => erlang:convert_time_unit(ExpireTimeMs, millisecond, second)
        }
    };
acl(_NoExpire, Source, #{<<"acl">> := Rules}) ->
    #{
        acl => #{
            source_for_logging => Source,
            rules => emqx_authz_rule_raw:parse_and_compile_rules(Rules)
        }
    };
acl(_, _, _) ->
    #{}.

clientid_override(#{<<"clientid_override">> := ClientIdOverride} = _Body) when
    is_binary(ClientIdOverride)
->
    #{clientid_override => ClientIdOverride};
clientid_override(_Body) ->
    #{}.

safely_parse_body(ContentType, Body) ->
    try
        parse_body(ContentType, Body)
    catch
        _Class:Reason ->
            ?TRACE_AUTHN_PROVIDER(
                error,
                "parse_http_response_failed",
                #{content_type => ContentType, body => Body, reason => Reason}
            ),
            {error, invalid_body}
    end.

parse_body(<<"application/json", _/binary>>, Body) ->
    {ok, emqx_utils_json:decode(Body)};
parse_body(<<"application/x-www-form-urlencoded", _/binary>>, Body) ->
    NBody = maps:from_list(cow_qs:parse_qs(Body)),
    {ok, NBody};
parse_body(_ContentType, _) ->
    erlang:throw(unsupported_content_type).

request_for_log(Credential, #{url := Url, method := Method} = State) ->
    SafeCredential = emqx_authn_utils:without_password(Credential),
    case generate_request(SafeCredential, State) of
        {ok, {PathQuery, Headers}} ->
            #{
                method => Method,
                url => Url,
                path_query => PathQuery,
                headers => Headers
            };
        {ok, {PathQuery, Headers, Body}} ->
            #{
                method => Method,
                url => Url,
                path_query => PathQuery,
                headers => Headers,
                body => Body
            };
        %% we can't get here actually because the real request was already generated
        %% successfully, so generating it with hidden password won't fail either.
        {error, Reason} ->
            #{
                method => Method,
                url => Url,
                error => Reason
            }
    end.

response_for_log({ok, StatusCode, Headers}) ->
    #{status => StatusCode, headers => Headers};
response_for_log({ok, StatusCode, Headers, Body}) ->
    #{status => StatusCode, headers => Headers, body => Body};
response_for_log({error, Error}) ->
    #{error => Error}.

ensure_binary_names(Headers) ->
    emqx_utils_maps:binary_key_map(Headers).
