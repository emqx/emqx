%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exproto_types).

-include_lib("emqx_libs/include/emqx.hrl").
-include_lib("emqx_libs/include/types.hrl").

-import(proplists, [get_value/2]).

-export([ parse/2
        , serialize/2
        ]).

-type(clientinfo() :: #{ proto_name := maybe(binary())
                       , proto_ver  := maybe(non_neg_integer())
                       , clientid   := maybe(binary())
                       , username   := maybe(binary())
                       , mountpoint := maybe(binary())
                       , keepalive  := maybe(non_neg_integer())
                       }).

-type(conninfo() :: #{ socktype := tcp | tls | udp | dtls
                     , peername := emqx_types:peername()
                     , sockname := emqx_types:sockname()
                     , peercert := nossl | binary() | list()
                     , conn_mod := atom()
                     }).

-export_type([conninfo/0, clientinfo/0]).

-define(UP_DATA_SCHEMA_CLIENTINFO, 
            [ {proto_name, optional, binary}
            , {proto_ver, optional, [integer, binary]}
            , {clientid, optional, binary}
            , {username, optional, binary}
            , {mountpoint, optional, binary}
            , {keepalive, optional, integer}
            ]).

-define(UP_DATA_SCHEMA_MESSAGE,
            [ {id, {optional, fun emqx_guid:gen/0}, binary}
            , {qos, required, [{enum, [0, 1, 2]}]}
            , {from, optional, [binary, atom]}
            , {topic, required, binary}
            , {payload, required, binary}
            , {timestamp, {optional, fun() -> erlang:system_time(millisecond) end}, integer}
            ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(parse(clientinfo | message, list())
     -> {error, any()}
      | clientinfo()
      | emqx_types:message()).
parse(clientinfo, Params) ->
    to_map(do_parsing(?UP_DATA_SCHEMA_CLIENTINFO, Params));

parse(message, Params) ->
    to_message(do_parsing(?UP_DATA_SCHEMA_MESSAGE, Params));

parse(Type, _) ->
    {error, {unkown_type, Type}}.

%% @private
to_map(Err = {error, _}) ->
    Err;
to_map(Ls) ->
    maps:from_list(Ls).

%% @private
to_message(Err = {error, _}) -> 
    Err;
to_message(Ls) ->
    #message{
       id = get_value(id, Ls),
       qos = get_value(qos, Ls),
       from = get_value(from, Ls),
       topic = get_value(topic, Ls),
       payload = get_value(payload, Ls),
       timestamp = get_value(timestamp, Ls)}.

-spec(serialize(Type, Struct)
      -> {error, any()}
       | [{atom(), any()}]
    when Type :: conninfo | message,
         Struct :: conninfo() | emqx_types:message()).
serialize(conninfo, #{socktype := A1,
                      peername := A2,
                      sockname := A3,
                      peercert := Peercert
                     }) ->
    [{socktype, A1},
     {peername, A2},
     {sockname, A3},
     {peercert, do_serializing(peercert, Peercert)}];

serialize(message, Msg) ->
    [{id, emqx_message:id(Msg)},
     {qos, emqx_message:qos(Msg)},
     {from, emqx_message:from(Msg)},
     {topic, emqx_message:topic(Msg)},
     {payload, emqx_message:payload(Msg)},
     {timestamp, emqx_message:timestamp(Msg)}];

serialize(Type, _) ->
    {error, {unkown_type, Type}}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

do_parsing(Schema, Params) ->
    try do_parsing(Schema, Params, [])
    catch
        throw:{badarg, Reason} -> {error, Reason}
    end.
do_parsing([], _Params, Acc) ->
    lists:reverse(Acc);
do_parsing([Indictor = {Key, _Optional, Type} | More], Params, Acc) ->
    Value = case get_value(Key, Params) of
                undefined -> do_generating(Indictor);
                InParam -> do_typing(Key, InParam, Type)
            end,
    do_parsing(More, Params, [{Key, Value} | Acc]).

%% @private 
do_generating({Key, required, _}) ->
    throw({badarg, errmsg("~s is required", [Key])});
do_generating({_, optional, _}) ->
    undefined;
do_generating({_, {_, Generator}, _}) when is_function(Generator) ->
    Generator();
do_generating({_, {_, Default}, _}) ->
    Default.

%% @private 
do_typing(Key, InParam, Types) when is_list(Types) ->
    case length(lists:filter(fun(T) -> is_x_type(InParam, T) end, Types)) of
        0 ->
            throw({badarg, errmsg("~s: value ~p data type is not validate to ~p", [Key, InParam, Types])});
        _ ->
            InParam
    end;
do_typing(Key, InParam, Type) ->
    do_typing(Key, InParam, [Type]).

% @private
is_x_type(P, atom) when is_atom(P) -> true;
is_x_type(P, binary) when is_binary(P) -> true;
is_x_type(P, integer) when is_integer(P) -> true;
is_x_type(P, {enum, Ls}) ->
    lists:member(P, Ls);
is_x_type(_, _) -> false.

do_serializing(peercert, nossl) ->
    nossl;
do_serializing(peercert, Peercert) ->
    [{dn, esockd_peercert:subject(Peercert)},
     {cn, esockd_peercert:common_name(Peercert)}].

errmsg(Fmt, Args) ->
    lists:flatten(io_lib:format(Fmt, Args)).

