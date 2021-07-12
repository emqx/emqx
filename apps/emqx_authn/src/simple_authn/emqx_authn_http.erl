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
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0
        , fields/1
        , validations/0
        ]).

-type accept() :: 'application/json' | 'application/x-www-form-urlencoded'.
-type content_type() :: accept().

-reflect_type([ accept/0
              , content_type/0
              ]).

-export([ create/3
        , update/4
        , authenticate/2
        , destroy/1
        ]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

structs() -> [""].

fields("") ->
    [ {config, #{type => hoconsc:union(
                          [ hoconsc:ref(?MODULE, get)
                          , hoconsc:ref(?MODULE, post)
                          ])}}
    ];

fields(get) ->
    [ {method,          #{type => get,
                          default => get}}
    ] ++ common_fields();

fields(post) ->
    [ {method,          #{type => post,
                          default => get}}
    , {content_type,    fun content_type/1}
    ] ++ common_fields().
common_fields() ->
    [ {url,             fun url/1}
    , {accept,          fun accept/1}
    , {headers,         fun headers/1}
    , {form_data,       fun form_data/1}
    , {request_timeout, fun request_timeout/1}
    ] ++ proplists:delete(base_url, emqx_connector_http:fields()).

validations() ->
    [ {check_ssl_opts, fun check_ssl_opts/1} ].

url(type) -> binary();
url(nullable) -> false;
url(validate) -> [fun check_url/1];
url(_) -> undefined.

accept(type) -> accept();
accept(default) -> 'application/json';
accept(_) -> undefined.

content_type(type) -> content_type();
content_type(default) -> 'application/json';
content_type(_) -> undefined.

headers(type) -> list();
headers(default) -> [];
headers(_) -> undefined.

form_data(type) -> binary();
form_data(nullable) -> false;
form_data(validate) -> [fun check_form_data/1];
form_data(_) -> undefined.

request_timeout(type) -> non_neg_integer();
request_timeout(default) -> 5000;
request_timeout(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(ChainID, AuthenticatorName,
        #{method := Method,
          url := URL,
          accept := Accept,
          content_type := ContentType,
          headers := Headers,
          form_data := FormData,
          request_timeout := RequestTimeout} = Config) ->
    NHeaders = maps:merge(#{<<"accept">> => atom_to_binary(Accept, utf8),
                            <<"content-type">> => atom_to_binary(ContentType, utf8)}, Headers),
    NFormData = preprocess_form_data(FormData),
    #{path := Path,
      query := Query} = URIMap = parse_url(URL),
    BaseURL = generate_base_url(URIMap),
    State = #{method          => Method,
              path            => Path,
              base_query      => cow_qs:parse_qs(Query),
              accept          => Accept,
              content_type    => ContentType,
              headers         => NHeaders,
              form_data       => NFormData,
              request_timeout => RequestTimeout},
    ResourceID = <<ChainID/binary, "/", AuthenticatorName/binary>>,
    case emqx_resource:create_local(ResourceID, emqx_connector_http, Config#{base_url := BaseURL}) of
        {ok, _} ->
            {ok, State#{resource_id => ResourceID}};
        {error, already_created} ->
            {ok, State#{resource_id => ResourceID}};
        {error, Reason} ->
            {error, Reason}
    end.

update(_ChainID, _AuthenticatorName, Config, #{resource_id := ResourceID} = State) ->
    case emqx_resource:update_local(ResourceID, emqx_connector_http, Config, []) of
        {ok, _} -> {ok, State};
        {error, Reason} -> {error, Reason}
    end.

authenticate(ClientInfo, #{resource_id := ResourceID,
                           method := Method,
                           request_timeout := RequestTimeout} = State) ->
    Request = generate_request(ClientInfo, State),
    case emqx_resource:query(ResourceID, {Method, Request, RequestTimeout}) of
        {ok, 204, _Headers} -> ok;
        {ok, 200, Headers, Body} ->
            ContentType = proplists:get_value(<<"content-type">>, Headers, <<"application/json">>),
            case safely_parse_body(ContentType, Body) of
                {ok, _NBody} ->
                    %% TODO: Return by user property
                    ok;
                {error, Reason} ->
                    {stop, Reason}
            end;
        {error, _Reason} ->
            ignore
    end.

destroy(#{resource_id := ResourceID}) ->
    _ = emqx_resource:remove_local(ResourceID),
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
    KVs = binary:split(FormData, [<<"&">>], [global]),
    case false =:= lists:any(fun(T) -> T =:= <<>> end, KVs) of
        true ->
            NKVs = [list_to_tuple(binary:split(KV, [<<"=">>], [global])) || KV <- KVs],
            false =:= 
                lists:any(fun({K, V}) ->
                              K =:= <<>> orelse V =:= <<>>;
                             (_) ->
                              true
                          end, NKVs);
        false ->
            false
    end.

check_ssl_opts(Conf) ->
    URL = hocon_schema:get_value("url", Conf),
    {ok, #{scheme := Scheme}} = emqx_http_lib:uri_parse(URL),
    SSLOpts = hocon_schema:get_value("ssl_opts", Conf),
    case {Scheme, SSLOpts} of
        {http, undefined} -> true;
        {http, _} -> false;
        {https, undefined} -> false;
        {https, _} -> true
    end.

preprocess_form_data(FormData) ->
    KVs = binary:split(FormData, [<<"&">>], [global]),
    [list_to_tuple(binary:split(KV, [<<"=">>], [global])) || KV <- KVs].

parse_url(URL) ->
    {ok, URIMap} = emqx_http_lib:uri_parse(URL),
    case maps:get(query, URIMap, undefined) of
        undefined ->
            URIMap#{query => ""};
        _ ->
            URIMap
    end.

generate_base_url(#{scheme := Scheme,
                    host := Host,
                    port := Port}) ->
    iolist_to_binary(io_lib:format("~p://~s:~p", [Scheme, Host, Port])).

generate_request(ClientInfo, #{method := Method,
                               path := Path,
                               base_query := BaseQuery,
                               content_type := ContentType,
                               headers := Headers,
                               form_data := FormData0}) ->
    FormData = replace_placeholders(FormData0, ClientInfo),
    case Method of
        get ->
            NPath = append_query(Path, BaseQuery ++ FormData),
            {NPath, Headers};
        post ->
            NPath = append_query(Path, BaseQuery),
            Body = serialize_body(ContentType, FormData),
            {NPath, Headers, Body}
    end.

replace_placeholders(FormData0, ClientInfo) ->
    FormData = lists:map(fun({K, V0}) ->
                             case replace_placeholder(V0, ClientInfo) of
                                 undefined -> {K, undefined};
                                 V -> {K, bin(V)}
                             end
                         end, FormData0),
    lists:filter(fun({_, V}) ->
                    V =/= undefined
                 end, FormData).

replace_placeholder(<<"${mqtt-username}">>, ClientInfo) ->
    maps:get(username, ClientInfo, undefined);
replace_placeholder(<<"${mqtt-clientid}">>, ClientInfo) ->
    maps:get(clientid, ClientInfo, undefined);
replace_placeholder(<<"${ip-address}">>, ClientInfo) ->
    maps:get(peerhost, ClientInfo, undefined);
replace_placeholder(<<"${cert-subject}">>, ClientInfo) ->
    maps:get(dn, ClientInfo, undefined);
replace_placeholder(<<"${cert-common-name}">>, ClientInfo) ->
    maps:get(cn, ClientInfo, undefined);
replace_placeholder(Constant, _) ->
    Constant.

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

serialize_body('application/json', FormData) ->
    emqx_json:encode(FormData);
serialize_body('application/x-www-form-urlencoded', FormData) ->
    qs(FormData).

safely_parse_body(ContentType, Body) ->
    try parse_body(ContentType, Body) of
        Result -> Result
    catch
        _Class:_Reason ->
            {error, invalid_body}
    end.

parse_body(<<"application/json">>, Body) ->
    {ok, emqx_json:decode(Body)};
parse_body(<<"application/x-www-form-urlencoded">>, Body) ->
    {ok, cow_qs:parse_qs(Body)};
parse_body(ContentType, _) ->
    {error, {unsupported_content_type, ContentType}}.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.