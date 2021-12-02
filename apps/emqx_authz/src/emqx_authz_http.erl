%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_http).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

%% AuthZ Callbacks
-export([ authorize/4
        , description/0
        , parse_url/1
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

description() ->
    "AuthZ with http".

authorize(Client, PubSub, Topic,
            #{type := http,
              url := #{path := Path} = URL,
              headers := Headers,
              method := Method,
              request_timeout := RequestTimeout,
              annotations := #{id := ResourceID}
             } = Source) ->
    Request = case Method of
                  get  ->
                      Query = maps:get(query, URL, ""),
                      Path1 = replvar(Path ++ "?" ++ Query, PubSub, Topic, Client),
                      {Path1, maps:to_list(Headers)};
                  _ ->
                      Body0 = serialize_body(
                                maps:get('Accept', Headers, <<"application/json">>),
                                maps:get(body, Source, #{})
                              ),
                      Body1 = replvar(Body0, PubSub, Topic, Client),
                      Path1 = replvar(Path, PubSub, Topic, Client),
                      {Path1, maps:to_list(Headers), Body1}
              end,
    case emqx_resource:query(ResourceID, {Method, Request, RequestTimeout}) of
        {ok, 200, _Headers} ->
            {matched, allow};
        {ok, 204, _Headers} ->
            {matched, allow};
        {ok, 200, _Headers, _Body} ->
            {matched, allow};
        {ok, _Status, _Headers, _Body} ->
            nomatch;
        {error, Reason} ->
            ?SLOG(error, #{msg => "http_server_query_failed",
                           resource => ResourceID,
                           reason => Reason}),
            ignore
    end.

parse_url(URL)
  when URL =:= undefined ->
    #{};
parse_url(URL) ->
    {ok, URIMap} = emqx_http_lib:uri_parse(URL),
    case maps:get(query, URIMap, undefined) of
        undefined ->
            URIMap#{query => ""};
        _ ->
            URIMap
    end.

query_string(Body) ->
    query_string(maps:to_list(Body), []).

query_string([], Acc) ->
    <<$&, Str/binary>> = iolist_to_binary(lists:reverse(Acc)),
    Str;
query_string([{K, V} | More], Acc) ->
    query_string( More
                , [ ["&", emqx_http_lib:uri_encode(K), "=", emqx_http_lib:uri_encode(V)]
                  | Acc]).

serialize_body(<<"application/json">>, Body) ->
    jsx:encode(Body);
serialize_body(<<"application/x-www-form-urlencoded">>, Body) ->
    query_string(Body).

replvar(Str0, PubSub, Topic,
        #{username := Username,
          clientid := Clientid,
          peerhost := IpAddress,
          protocol := Protocol,
          mountpoint := Mountpoint
         }) when is_list(Str0);
                 is_binary(Str0) ->
    NTopic = emqx_http_lib:uri_encode(Topic),
    Str1 = re:replace( Str0, emqx_authz:ph_to_re(?PH_S_CLIENTID)
                     , bin(Clientid), [global, {return, binary}]),
    Str2 = re:replace( Str1, emqx_authz:ph_to_re(?PH_S_USERNAME)
                     , bin(Username), [global, {return, binary}]),
    Str3 = re:replace( Str2, emqx_authz:ph_to_re(?PH_S_HOST)
                     , inet_parse:ntoa(IpAddress), [global, {return, binary}]),
    Str4 = re:replace( Str3, emqx_authz:ph_to_re(?PH_S_PROTONAME)
                     , bin(Protocol), [global, {return, binary}]),
    Str5 = re:replace( Str4, emqx_authz:ph_to_re(?PH_S_MOUNTPOINT)
                     , bin(Mountpoint), [global, {return, binary}]),
    Str6 = re:replace( Str5, emqx_authz:ph_to_re(?PH_S_TOPIC)
                     , bin(NTopic), [global, {return, binary}]),
    Str7 = re:replace( Str6, emqx_authz:ph_to_re(?PH_S_ACTION)
                     , bin(PubSub), [global, {return, binary}]),
    Str7.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.
