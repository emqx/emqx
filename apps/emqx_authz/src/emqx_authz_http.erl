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

-behaviour(emqx_authz).

%% AuthZ Callbacks
-export([ description/0
        , init/1
        , destroy/1
        , dry_run/1
        , authorize/4
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

description() ->
    "AuthZ with http".

init(Source) ->
    case emqx_authz_utils:create_resource(emqx_connector_http, Source) of
        {error, Reason} -> error({load_config_error, Reason});
        {ok, Id} -> Source#{annotations => #{id => Id}}
    end.

destroy(#{annotations := #{id := Id}}) ->
    ok = emqx_resource:remove(Id).

dry_run(Source) ->
    emqx_resource:create_dry_run(emqx_connector_http, Source).

authorize(Client, PubSub, Topic,
            #{type := http,
              query := Query,
              path := Path,
              headers := Headers,
              method := Method,
              request_timeout := RequestTimeout,
              annotations := #{id := ResourceID}
             } = Source) ->
    Request = case Method of
                  get ->
                      Path1 = replvar(
                                Path ++ "?" ++ Query,
                                PubSub,
                                Topic,
                                maps:to_list(Client),
                                fun var_uri_encode/1),

                      {Path1, maps:to_list(Headers)};

                  _ ->
                      Body0 = maps:get(body, Source, #{}),
                      Body1 = replvar_deep(
                                Body0,
                                PubSub,
                                Topic,
                                maps:to_list(Client),
                                fun var_bin_encode/1),

                      Body2 = serialize_body(
                                maps:get(<<"content-type">>, Headers, <<"application/json">>),
                                Body1),

                      Path1 = replvar(
                                Path,
                                PubSub,
                                Topic,
                                maps:to_list(Client),
                                fun var_uri_encode/1),

                      {Path1, maps:to_list(Headers), Body2}
              end,
    HttpResult = emqx_resource:query(ResourceID, {Method, Request, RequestTimeout}),
    case HttpResult of
        {ok, 200, _Headers} ->
            {matched, allow};
        {ok, 204, _Headers} ->
            {matched, allow};
        {ok, 200, _Headers, _Body} ->
            {matched, allow};
        {ok, _Status, _Headers} ->
            nomatch;
        {ok, _Status, _Headers, _Body} ->
            nomatch;
        {error, Reason} ->
            ?SLOG(error, #{msg => "http_server_query_failed",
                           resource => ResourceID,
                           reason => Reason}),
            ignore
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


replvar_deep(Map, PubSub, Topic, Vars, VarEncode) when is_map(Map) ->
    maps:from_list(
      lists:map(
        fun({Key, Value}) ->
                {replvar(Key, PubSub, Topic, Vars, VarEncode),
                 replvar_deep(Value, PubSub, Topic, Vars, VarEncode)}
        end,
        maps:to_list(Map)));
replvar_deep(List, PubSub, Topic, Vars, VarEncode) when is_list(List) ->
    lists:map(
      fun(Value) ->
              replvar_deep(Value, PubSub, Topic, Vars, VarEncode)
      end,
      List);
replvar_deep(Number, _PubSub, _Topic, _Vars, _VarEncode) when is_number(Number) ->
    Number;
replvar_deep(Binary, PubSub, Topic, Vars, VarEncode) when is_binary(Binary) ->
    replvar(Binary, PubSub, Topic, Vars, VarEncode).

replvar(Str0, PubSub, Topic, [], VarEncode) ->
    NTopic = emqx_http_lib:uri_encode(Topic),
    Str1 = re:replace(Str0, emqx_authz:ph_to_re(?PH_S_TOPIC),
                      VarEncode(NTopic), [global, {return, binary}]),
    re:replace(Str1, emqx_authz:ph_to_re(?PH_S_ACTION),
               VarEncode(PubSub), [global, {return, binary}]);


replvar(Str, PubSub, Topic, [{username, Username} | Rest], VarEncode) ->
    Str1 = re:replace(Str, emqx_authz:ph_to_re(?PH_S_USERNAME),
                      VarEncode(Username), [global, {return, binary}]),
    replvar(Str1, PubSub, Topic, Rest, VarEncode);

replvar(Str, PubSub, Topic, [{clientid, Clientid} | Rest], VarEncode) ->
    Str1 = re:replace(Str, emqx_authz:ph_to_re(?PH_S_CLIENTID),
                      VarEncode(Clientid), [global, {return, binary}]),
    replvar(Str1, PubSub, Topic, Rest, VarEncode);

replvar(Str, PubSub, Topic, [{peerhost, IpAddress}  | Rest], VarEncode) ->
    Str1 = re:replace(Str, emqx_authz:ph_to_re(?PH_S_PEERHOST),
                      VarEncode(inet_parse:ntoa(IpAddress)), [global, {return, binary}]),
    replvar(Str1, PubSub, Topic, Rest, VarEncode);

replvar(Str, PubSub, Topic, [{protocol, Protocol} | Rest], VarEncode) ->
    Str1 = re:replace(Str, emqx_authz:ph_to_re(?PH_S_PROTONAME),
                      VarEncode(Protocol), [global, {return, binary}]),
    replvar(Str1, PubSub, Topic, Rest, VarEncode);

replvar(Str, PubSub, Topic, [{mountpoint, Mountpoint} | Rest], VarEncode) ->
    Str1 = re:replace(Str, emqx_authz:ph_to_re(?PH_S_MOUNTPOINT),
                      VarEncode(Mountpoint), [global, {return, binary}]),
    replvar(Str1, PubSub, Topic, Rest, VarEncode);

replvar(Str, PubSub, Topic, [_Unknown | Rest], VarEncode) ->
    replvar(Str, PubSub, Topic, Rest, VarEncode).

var_uri_encode(S) ->
    emqx_http_lib:uri_encode(bin(S)).

var_bin_encode(S) ->
    bin(S).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.
