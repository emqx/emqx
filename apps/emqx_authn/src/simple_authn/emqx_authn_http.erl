%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([ namespace/0
        , roots/0
        , fields/1
        , validations/0
        ]).

-export([ refs/0
        , create/2
        , update/2
        , authenticate/2
        , destroy/1
        ]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn-http".

roots() ->
    [ {config, hoconsc:mk(hoconsc:union(refs()),
                          #{})}
    ].

fields(get) ->
    [ {method,          #{type => get,
                          default => post}}
    , {headers,         fun headers_no_content_type/1}
    ] ++ common_fields();

fields(post) ->
    [ {method,          #{type => post,
                          default => post}}
    , {headers,         fun headers/1}
    ] ++ common_fields().

common_fields() ->
    [ {mechanism,       hoconsc:enum(['password-based'])}
    , {backend,         hoconsc:enum(['http'])}
    , {url,             fun url/1}
    , {body,            fun body/1}
    , {request_timeout, fun request_timeout/1}
    ] ++ emqx_authn_schema:common_fields()
    ++ maps:to_list(maps:without([ base_url
                                 , pool_type],
                    maps:from_list(emqx_connector_http:fields(config)))).

validations() ->
    [ {check_ssl_opts, fun check_ssl_opts/1}
    , {check_headers, fun check_headers/1}
    ].

url(type) -> binary();
url(validator) -> [fun check_url/1];
url(nullable) -> false;
url(_) -> undefined.

headers(type) -> map();
headers(converter) ->
    fun(Headers) ->
       maps:merge(default_headers(), transform_header_name(Headers))
    end;
headers(default) -> default_headers();
headers(_) -> undefined.

headers_no_content_type(type) -> map();
headers_no_content_type(converter) ->
    fun(Headers) ->
       maps:merge(default_headers_no_content_type(), transform_header_name(Headers))
    end;
headers_no_content_type(default) -> default_headers_no_content_type();
headers_no_content_type(_) -> undefined.

body(type) -> map();
body(validator) -> [fun check_body/1];
body(_) -> undefined.

request_timeout(type) -> emqx_schema:duration_ms();
request_timeout(default) -> <<"5s">>;
request_timeout(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [ hoconsc:ref(?MODULE, get)
    , hoconsc:ref(?MODULE, post)
    ].

create(_AuthenticatorID, Config) ->
    create(Config).

create(#{method := Method,
         url := URL,
         headers := Headers,
         body := Body,
         request_timeout := RequestTimeout} = Config) ->
    #{path := Path,
      query := Query} = URIMap = parse_url(URL),
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    State = #{method          => Method,
              path            => Path,
              base_query      => cow_qs:parse_qs(list_to_binary(Query)),
              headers         => maps:to_list(Headers),
              body            => maps:to_list(Body),
              request_timeout => RequestTimeout,
              resource_id => ResourceId},
    case emqx_resource:create_local(ResourceId,
                                    emqx_connector_http,
                                    Config#{base_url => maps:remove(query, URIMap),
                                            pool_type => random}) of
        {ok, already_created} ->
            {ok, State};
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

update(Config, State) ->
    case create(Config) of
        {ok, NewState} ->
            ok = destroy(State),
            {ok, NewState};
        {error, Reason} ->
            {error, Reason}
    end.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(Credential, #{resource_id := ResourceId,
                           method := Method,
                           request_timeout := RequestTimeout} = State) ->
    Request = generate_request(Credential, State),
    case emqx_resource:query(ResourceId, {Method, Request, RequestTimeout}) of
        {ok, 204, _Headers} -> {ok, #{is_superuser => false}};
        {ok, 200, _Headers} -> {ok, #{is_superuser => false}};
        {ok, 200, Headers, Body} ->
            ContentType = proplists:get_value(<<"content-type">>, Headers, <<"application/json">>),
            case safely_parse_body(ContentType, Body) of
                {ok, NBody} ->
                    %% TODO: Return by user property
                    UserProperty = maps:remove(<<"is_superuser">>, NBody),
                    IsSuperuser = emqx_authn_utils:is_superuser(NBody),
                    {ok, IsSuperuser#{user_property => UserProperty}};
                {error, _Reason} ->
                    {ok, #{is_superuser => false}}
            end;
        {error, Reason} ->
            ?SLOG(error, #{msg => "http_server_query_failed",
                           resource => ResourceId,
                           reason => Reason}),
            ignore;
        Other ->
            Output = may_append_body(#{resource => ResourceId}, Other),
            case erlang:element(2, Other) of
                Code5xx when Code5xx >= 500 andalso Code5xx < 600 ->
                    ?SLOG(error, Output#{msg => "http_server_error",
                                         code => Code5xx}),
                    ignore;
                Code4xx when Code4xx >= 400 andalso Code4xx < 500 ->
                    ?SLOG(warning, Output#{msg => "refused_by_http_server",
                                           code => Code4xx}),
                    {error, not_authorized};
                OtherCode ->
                    ?SLOG(error, Output#{msg => "undesired_response_code",
                                           code => OtherCode}),
                    ignore
            end
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

check_url(URL) ->
    case emqx_http_lib:uri_parse(URL) of
        {ok, _} -> true;
        {error, _} -> false
    end.

check_body(Body) ->
    lists:all(
      fun erlang:is_binary/1,
      maps:values(Body)).

default_headers() ->
    maps:put(<<"content-type">>,
             <<"application/json">>,
             default_headers_no_content_type()).

default_headers_no_content_type() ->
    #{ <<"accept">> => <<"application/json">>
     , <<"cache-control">> => <<"no-cache">>
     , <<"connection">> => <<"keep-alive">>
     , <<"keep-alive">> => <<"timeout=5">>
     }.

transform_header_name(Headers) ->
    maps:fold(fun(K0, V, Acc) ->
                  K = list_to_binary(string:to_lower(to_list(K0))),
                  maps:put(K, V, Acc)
              end, #{}, Headers).

check_ssl_opts(Conf) ->
    case parse_url(hocon_schema:get_value("config.url", Conf)) of
        #{scheme := https} ->
            case hocon_schema:get_value("config.ssl.enable", Conf) of
                true -> ok;
                false -> false
            end;
        #{scheme := http} ->
            ok
    end.

check_headers(Conf) ->
    Method = to_bin(hocon_schema:get_value("config.method", Conf)),
    Headers = hocon_schema:get_value("config.headers", Conf),
    Method =:= <<"post">> orelse (not maps:is_key(<<"content-type">>, Headers)).

parse_url(URL) ->
    {ok, URIMap} = emqx_http_lib:uri_parse(URL),
    case maps:get(query, URIMap, undefined) of
        undefined ->
            URIMap#{query => ""};
        _ ->
            URIMap
    end.

generate_request(Credential, #{method := Method,
                               path := Path,
                               base_query := BaseQuery,
                               headers := Headers,
                               body := Body0}) ->
    Body = replace_placeholders(Body0, Credential),
    case Method of
        get ->
            NPath = append_query(Path, BaseQuery ++ Body),
            {NPath, Headers};
        post ->
            NPath = append_query(Path, BaseQuery),
            ContentType = proplists:get_value(<<"content-type">>, Headers),
            NBody = serialize_body(ContentType, Body),
            {NPath, Headers, NBody}
    end.

replace_placeholders(KVs, Credential) ->
    replace_placeholders(KVs, Credential, []).

replace_placeholders([], _Credential, Acc) ->
    lists:reverse(Acc);
replace_placeholders([{K, V0} | More], Credential, Acc) ->
    case emqx_authn_utils:replace_placeholder(V0, Credential) of
        undefined ->
            error({cannot_get_variable, V0});
        V ->
            replace_placeholders(More, Credential, [{K, emqx_authn_utils:bin(V)} | Acc])
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
    emqx_json:encode(Body);
serialize_body(<<"application/x-www-form-urlencoded">>, Body) ->
    qs(Body).

safely_parse_body(ContentType, Body) ->
    try parse_body(ContentType, Body) of
        Result -> Result
    catch
        _Class:_Reason ->
            {error, invalid_body}
    end.

parse_body(<<"application/json">>, Body) ->
    {ok, emqx_json:decode(Body, [return_maps])};
parse_body(<<"application/x-www-form-urlencoded">>, Body) ->
    {ok, maps:from_list(cow_qs:parse_qs(Body))};
parse_body(ContentType, _) ->
    {error, {unsupported_content_type, ContentType}}.

may_append_body(Output, {ok, _, _, Body}) ->
    Output#{body => Body};
may_append_body(Output, {ok, _, _}) ->
    Output.

uri_encode(T) ->
    emqx_http_lib:uri_encode(to_bin(T)).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B).

to_bin(A) when is_atom(A) ->
    atom_to_binary(A);
to_bin(B) when is_binary(B) ->
    B;
to_bin(L) when is_list(L) ->
    list_to_binary(L).
