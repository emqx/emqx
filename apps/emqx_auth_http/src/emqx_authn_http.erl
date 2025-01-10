%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authn_http).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-export([
    with_validated_config/2,
    generate_request/2,
    request_for_log/2,
    response_for_log/1,
    extract_auth_data/2,
    safely_parse_body/2
]).

-define(DEFAULT_CONTENT_TYPE, <<"application/json">>).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    create(Config).

create(Config0) ->
    with_validated_config(Config0, fun(Config, State) ->
        ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
        % {Config, State} = parse_config(Config0),
        {ok, _Data} = emqx_authn_utils:create_resource(
            ResourceId,
            emqx_bridge_http_connector,
            Config
        ),
        {ok, State#{resource_id => ResourceId}}
    end).

update(Config0, #{resource_id := ResourceId} = _State) ->
    with_validated_config(Config0, fun(Config, NState) ->
        % {Config, NState} = parse_config(Config0),
        case emqx_authn_utils:update_resource(emqx_bridge_http_connector, Config, ResourceId) of
            {error, Reason} ->
                error({load_config_error, Reason});
            {ok, _} ->
                {ok, NState#{resource_id => ResourceId}}
        end
    end).

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

with_validated_config(Config, Fun) ->
    Pipeline = [
        fun check_ssl_opts/1,
        fun normalize_headers/1,
        fun check_method_headers/1,
        fun parse_config/1
    ],
    case emqx_utils:pipeline(Pipeline, Config, undefined) of
        {ok, NConfig, ProviderState} ->
            Fun(NConfig, ProviderState);
        {error, Reason, _} ->
            {error, Reason}
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
    Vars = BasePathVars ++ BaseQueryVars ++ BodyVars,
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    State = #{
        method => Method,
        path => Path,
        headers => maps:to_list(Headers),
        base_path_template => BasePathTemplate,
        base_query_template => BaseQueryTemplate,
        body_template => BodyTemplate,
        cache_key_template => CacheKeyTemplate,
        request_timeout => RequestTimeout,
        url => RawUrl
    },
    {ok,
        Config#{
            request_base => RequestBase,
            pool_type => random
        },
        State}.

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
        Result = merge_maps([ExpireAt, IsSuperuser, ACL, Attrs]),
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
    {ok, emqx_utils_json:decode(Body, [return_maps])};
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
