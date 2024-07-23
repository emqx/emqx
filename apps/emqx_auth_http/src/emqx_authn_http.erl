%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

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
        request_timeout := RequestTimeout
    } = State
) ->
    Request = generate_request(Credential, State),
    Response = emqx_resource:simple_sync_query(ResourceId, {Method, Request, RequestTimeout}),
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
        fun check_headers/1,
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

check_headers(#{headers := Headers, method := get}) ->
    case maps:is_key(<<"content-type">>, Headers) of
        false ->
            ok;
        true ->
            {error, {invalid_headers, <<"HTTP GET requests cannot include content-type header.">>}}
    end;
check_headers(_) ->
    ok.

parse_config(
    #{
        method := Method,
        url := RawUrl,
        headers := Headers,
        request_timeout := RequestTimeout
    } = Config
) ->
    {RequestBase, Path, Query} = emqx_auth_utils:parse_url(RawUrl),
    State = #{
        method => Method,
        path => Path,
        headers => ensure_header_name_type(Headers),
        base_path_template => emqx_authn_utils:parse_str(Path),
        base_query_template => emqx_authn_utils:parse_deep(
            cow_qs:parse_qs(Query)
        ),
        body_template => emqx_authn_utils:parse_deep(maps:get(body, Config, #{})),
        request_timeout => RequestTimeout,
        url => RawUrl
    },
    {ok,
        Config#{
            request_base => RequestBase,
            pool_type => random
        },
        State}.

generate_request(Credential, #{
    method := Method,
    headers := Headers0,
    base_path_template := BasePathTemplate,
    base_query_template := BaseQueryTemplate,
    body_template := BodyTemplate
}) ->
    Headers = maps:to_list(Headers0),
    Path = emqx_authn_utils:render_urlencoded_str(BasePathTemplate, Credential),
    Query = emqx_authn_utils:render_deep(BaseQueryTemplate, Credential),
    Body = emqx_authn_utils:render_deep(BodyTemplate, Credential),
    case Method of
        get ->
            NPathQuery = append_query(to_list(Path), to_list(Query) ++ maps:to_list(Body)),
            {NPathQuery, Headers};
        post ->
            NPathQuery = append_query(to_list(Path), to_list(Query)),
            ContentType = proplists:get_value(<<"content-type">>, Headers),
            NBody = serialize_body(ContentType, Body),
            {NPathQuery, Headers, NBody}
    end.

append_query(Path, []) ->
    Path;
append_query(Path, Query) ->
    Path ++ "?" ++ binary_to_list(qs(Query)).

qs(KVs) ->
    qs(KVs, []).

qs([], Acc) ->
    <<$&, Qs/binary>> = iolist_to_binary(lists:reverse(Acc)),
    Qs;
qs([{K, V} | More], Acc) ->
    qs(More, [["&", uri_encode(K), "=", uri_encode(V)] | Acc]).

serialize_body(<<"application/json">>, Body) ->
    emqx_utils_json:encode(Body);
serialize_body(<<"application/x-www-form-urlencoded">>, Body) ->
    qs(maps:to_list(Body));
serialize_body(undefined, _) ->
    throw("missing_content_type_header").

handle_response(Headers, Body) ->
    ContentType = proplists:get_value(<<"content-type">>, Headers),
    case safely_parse_body(ContentType, Body) of
        {ok, NBody} ->
            body_to_auth_data(NBody);
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(
                error,
                "parse_http_response_failed",
                #{content_type => ContentType, body => Body, reason => Reason}
            ),
            ignore
    end.

body_to_auth_data(Body) ->
    case maps:get(<<"result">>, Body, <<"ignore">>) of
        <<"allow">> ->
            IsSuperuser = emqx_authn_utils:is_superuser(Body),
            Attrs = emqx_authn_utils:client_attrs(Body),
            try
                ExpireAt = expire_at(Body),
                ACL = acl(ExpireAt, Body),
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
            end;
        <<"deny">> ->
            {error, not_authorized};
        <<"ignore">> ->
            ignore;
        _ ->
            ignore
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

acl(#{expire_at := ExpireTimeMs}, #{<<"acl">> := Rules}) ->
    #{
        acl => #{
            source_for_logging => http,
            rules => emqx_authz_rule_raw:parse_and_compile_rules(Rules),
            %% It's seconds level precision (like JWT) for authz
            %% see emqx_authz_client_info:check/1
            expire => erlang:convert_time_unit(ExpireTimeMs, millisecond, second)
        }
    };
acl(_NoExpire, #{<<"acl">> := Rules}) ->
    #{
        acl => #{
            source_for_logging => http,
            rules => emqx_authz_rule_raw:parse_and_compile_rules(Rules)
        }
    };
acl(_, _) ->
    #{}.

safely_parse_body(ContentType, Body) ->
    try
        parse_body(ContentType, Body)
    catch
        _Class:_Reason ->
            {error, invalid_body}
    end.

parse_body(<<"application/json", _/binary>>, Body) ->
    {ok, emqx_utils_json:decode(Body, [return_maps])};
parse_body(<<"application/x-www-form-urlencoded", _/binary>>, Body) ->
    Flags = [<<"result">>, <<"is_superuser">>],
    RawMap = maps:from_list(cow_qs:parse_qs(Body)),
    NBody = maps:with(Flags, RawMap),
    {ok, NBody};
parse_body(ContentType, _) ->
    {error, {unsupported_content_type, ContentType}}.

uri_encode(T) ->
    emqx_http_lib:uri_encode(to_list(T)).

request_for_log(Credential, #{url := Url, method := Method} = State) ->
    SafeCredential = emqx_authn_utils:without_password(Credential),
    case generate_request(SafeCredential, State) of
        {PathQuery, Headers} ->
            #{
                method => Method,
                url => Url,
                path_query => PathQuery,
                headers => Headers
            };
        {PathQuery, Headers, Body} ->
            #{
                method => Method,
                url => Url,
                path_query => PathQuery,
                headers => Headers,
                body => Body
            }
    end.

response_for_log({ok, StatusCode, Headers}) ->
    #{status => StatusCode, headers => Headers};
response_for_log({ok, StatusCode, Headers, Body}) ->
    #{status => StatusCode, headers => Headers, body => Body};
response_for_log({error, Error}) ->
    #{error => Error}.

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.

ensure_header_name_type(Headers) ->
    Fun = fun
        (Key, _Val, Acc) when is_binary(Key) ->
            Acc;
        (Key, Val, Acc) when is_atom(Key) ->
            Acc2 = maps:remove(Key, Acc),
            BinKey = erlang:atom_to_binary(Key),
            Acc2#{BinKey => Val}
    end,
    maps:fold(Fun, Headers, Headers).
