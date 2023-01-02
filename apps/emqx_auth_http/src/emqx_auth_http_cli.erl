%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_http_cli).

-include("emqx_auth_http.hrl").

-export([ request/6
        , request/7
        , feedvar/2
        , feedvar/3
        ]).

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------

request(PoolName, Method, Path, Headers, Params, Timeout) ->
    request(PoolName, Method, Path, Headers, Params, Timeout, ?DEFAULT_RETRY_TIMES).

request(PoolName, get, Path, Headers, Params, Timeout, Retry) ->
    NewPath = Path ++ "?" ++ binary_to_list(cow_qs:qs(bin_kw(Params))),
    reply(ehttpc:request(PoolName, get, {NewPath, Headers}, Timeout, Retry));

request(PoolName, post, Path, Headers, Params, Timeout, Retry) ->
    Body = case proplists:get_value(<<"content-type">>, Headers) of
               "application/x-www-form-urlencoded" ->
                   cow_qs:qs(bin_kw(Params));
               "application/json" -> 
                   emqx_json:encode(bin_kw(Params))
           end,
    reply(ehttpc:request(PoolName, post, {Path, Headers, Body}, Timeout, Retry)).

reply({ok, StatusCode, _Headers}) ->
    {ok, StatusCode, <<>>};
reply({ok, StatusCode, _Headers, Body}) ->
    {ok, StatusCode, Body};
reply({error, Reason}) ->
    {error, Reason}.

%% TODO: move this conversion to cuttlefish config and schema
bin_kw(KeywordList) when is_list(KeywordList) ->
    [{bin(K), bin(V)} || {K, V} <- KeywordList].

bin(Atom) when is_atom(Atom) ->
    list_to_binary(atom_to_list(Atom));
bin(Int) when is_integer(Int) ->
    integer_to_binary(Int);
bin(Float) when is_float(Float) ->
    float_to_binary(Float, [{decimals, 12}, compact]);
bin(List) when is_list(List)->
    list_to_binary(List);
bin(Binary) when is_binary(Binary) ->
    Binary.

%%--------------------------------------------------------------------
%% Feed Variables
%%--------------------------------------------------------------------

feedvar(Params, ClientInfo = #{clientid := ClientId,
                               protocol := Protocol,
                               peerhost := Peerhost}) ->
    lists:map(fun({Param, "%u"}) -> {Param, maps:get(username, ClientInfo, null)};
                 ({Param, "%c"}) -> {Param, ClientId};
                 ({Param, "%r"}) -> {Param, Protocol};
                 ({Param, "%a"}) -> {Param, inet:ntoa(Peerhost)};
                 ({Param, "%P"}) -> {Param, maps:get(password, ClientInfo, null)};
                 ({Param, "%p"}) -> {Param, maps:get(sockport, ClientInfo, null)};
                 ({Param, "%C"}) -> {Param, maps:get(cn, ClientInfo, null)};
                 ({Param, "%d"}) -> {Param, maps:get(dn, ClientInfo, null)};
                 ({Param, "%A"}) -> {Param, maps:get(access, ClientInfo, null)};
                 ({Param, "%t"}) -> {Param, maps:get(topic, ClientInfo, null)};
                 ({Param, "%m"}) -> {Param, maps:get(mountpoint, ClientInfo, null)};
                 ({Param, Var})  -> {Param, Var}
              end, Params).

feedvar(Params, Var, Val) ->
    lists:map(fun({Param, Var0}) when Var0 == Var ->
                      {Param, Val};
                 ({Param, Var0}) ->
                      {Param, Var0}
              end, Params).

