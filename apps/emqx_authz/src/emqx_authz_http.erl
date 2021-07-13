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

%% AuthZ Callbacks
-export([ authorize/4
        , description/0
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

description() ->
    "AuthZ with http".

authorize(Client, PubSub, Topic,
            #{resource_id := ResourceID,
              type := http,
              config := #{url := #{path := Path} = Url,
                          headers := Headers,
                          method := Method,
                          request_timeout := RequestTimeout} = Config
             }) ->
    Request = case Method of
                  get  -> 
                      Query = maps:get(query, Url, ""),
                      Path1 = replvar(Path ++ "?" ++ Query, PubSub, Topic, Client),
                      {Path1, maps:to_list(Headers)};
                  _ ->
                      Body0 = serialize_body(
                                maps:get('Accept', Headers, <<"application/json">>),
                                maps:get(body, Config, #{})
                              ),
                      Body1 = replvar(Body0, PubSub, Topic, Client),
                      Path1 = replvar(Path, PubSub, Topic, Client),
                      {Path1, maps:to_list(Headers), Body1}
              end,
    case emqx_resource:query(ResourceID,  {Method, Request, RequestTimeout}) of
        {ok, 204, _Headers} -> {matched, allow};
        {ok, 200, _Headers, _Body} -> {matched, allow};
        _ -> nomatch
    end.

query_string(Body) ->
    query_string(maps:to_list(Body), []).

query_string([], Acc) ->
    <<$&, Str/binary>> = iolist_to_binary(lists:reverse(Acc)),
    Str;
query_string([{K, V} | More], Acc) ->
    query_string(More, [["&", emqx_http_lib:uri_encode(K), "=", emqx_http_lib:uri_encode(V)] | Acc]).

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
    Str1 = re:replace(Str0, "%c", Clientid, [global, {return, binary}]),
    Str2 = re:replace(Str1, "%u", Username, [global, {return, binary}]),
    Str3 = re:replace(Str2, "%a", inet_parse:ntoa(IpAddress), [global, {return, binary}]),
    Str4 = re:replace(Str3, "%r", bin(Protocol), [global, {return, binary}]),
    Str5 = re:replace(Str4, "%m", Mountpoint, [global, {return, binary}]),
    Str6 = re:replace(Str5, "%t", NTopic, [global, {return, binary}]),
    Str7 = re:replace(Str6, "%A", bin(PubSub), [global, {return, binary}]),
    Str7.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.
