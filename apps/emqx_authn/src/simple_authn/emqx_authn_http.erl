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

-export([ roots/0
        , fields/1
        , validations/0
        ]).

-export([ create/1
        , update/2
        , authenticate/2
        , destroy/1
        ]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

roots() ->
    [ {config, {union, [ hoconsc:t(get)
                       , hoconsc:t(post)
                       ]}}
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
    [ {type,            {enum, ['password-based:http-server']}}
    , {url,             fun url/1}
    , {form_data,       fun form_data/1}
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
url(nullable) -> false;
url(validate) -> [fun check_url/1];
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

%% TODO: Using map()
form_data(type) -> map();
form_data(nullable) -> false;
form_data(validate) -> [fun check_form_data/1];
form_data(_) -> undefined.

request_timeout(type) -> non_neg_integer();
request_timeout(default) -> 5000;
request_timeout(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(#{ method := Method
        , url := URL
        , headers := Headers
        , form_data := FormData
        , request_timeout := RequestTimeout
        , '_unique' := Unique
        } = Config) ->
    #{path := Path,
      query := Query} = URIMap = parse_url(URL),
    State = #{ method          => Method
             , path            => Path
             , base_query      => cow_qs:parse_qs(list_to_binary(Query))
             , headers         => normalize_headers(Headers)
             , form_data       => maps:to_list(FormData)
             , request_timeout => RequestTimeout
             , '_unique'       => Unique
             },
    case emqx_resource:create_local(Unique,
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
authenticate(Credential, #{'_unique' := Unique,
                           method := Method,
                           request_timeout := RequestTimeout} = State) ->
    try
        Request = generate_request(Credential, State),
        case emqx_resource:query(Unique, {Method, Request, RequestTimeout}) of
            {ok, 204, _Headers} -> {ok, #{superuser => false}};
            {ok, 200, Headers, Body} ->
                ContentType = proplists:get_value(<<"content-type">>, Headers, <<"application/json">>),
                case safely_parse_body(ContentType, Body) of
                    {ok, NBody} ->
                        %% TODO: Return by user property
                        {ok, #{superuser => maps:get(<<"superuser">>, NBody, false),
                               user_property => NBody}};
                    {error, _Reason} ->
                        {ok, #{superuser => false}}
                end;
            {error, _Reason} ->
                ignore
        end
    catch
        error:Reason ->
            ?LOG(warning, "The following error occurred in '~s' during authentication: ~p", [Unique, Reason]),
            ignore
    end.

destroy(#{'_unique' := Unique}) ->
    _ = emqx_resource:remove_local(Unique),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

check_url(URL) ->
    case emqx_http_lib:uri_parse(URL) of
        {ok, _} -> true;
        {error, _} -> false
    end.

check_form_data(FormData) ->
    lists:any(fun({_, V}) ->
                  not is_binary(V)
              end, maps:to_list(FormData)).

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
                  K = list_to_binary(string:to_lower(binary_to_list(K0))),
                  maps:put(K, V, Acc)
              end, #{}, Headers).

check_ssl_opts(Conf) ->
    emqx_connector_http:check_ssl_opts("url", Conf).

check_headers(Conf) ->
    Method = hocon_schema:get_value("method", Conf),
    Headers = hocon_schema:get_value("headers", Conf),
    case Method =:= get andalso maps:get(<<"content-type">>, Headers, undefined) =/= undefined of
        true -> false;
        false -> true
    end.

parse_url(URL) ->
    {ok, URIMap} = emqx_http_lib:uri_parse(URL),
    case maps:get(query, URIMap, undefined) of
        undefined ->
            URIMap#{query => ""};
        _ ->
            URIMap
    end.

normalize_headers(Headers) ->
    [{atom_to_binary(K), V} || {K, V} <- maps:to_list(Headers)].

generate_request(Credential, #{method := Method,
                               path := Path,
                               base_query := BaseQuery,
                               headers := Headers,
                               form_data := FormData0}) ->
    FormData = replace_placeholders(FormData0, Credential),
    case Method of
        get ->
            NPath = append_query(Path, BaseQuery ++ FormData),
            {NPath, Headers};
        post ->
            NPath = append_query(Path, BaseQuery),
            ContentType = proplists:get_value(<<"content-type">>, Headers),
            Body = serialize_body(ContentType, FormData),
            {NPath, Headers, Body}
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
    qs(More, [["&", emqx_http_lib:uri_encode(K), "=", emqx_http_lib:uri_encode(V)] | Acc]).

serialize_body(<<"application/json">>, FormData) ->
    emqx_json:encode(FormData);
serialize_body(<<"application/x-www-form-urlencoded">>, FormData) ->
    qs(FormData).

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
