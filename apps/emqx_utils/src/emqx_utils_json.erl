%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_json).

-compile(inline).

-export([
    encode/1,
    encode/2,
    encode_proplist/1,
    encode_proplist/2,
    safe_encode/1,
    safe_encode/2
]).

-compile(
    {inline, [
        encode/1,
        encode/2
    ]}
).

-export([
    decode/1,
    decode/2,
    decode_proplist/1,
    safe_decode/1,
    safe_decode/2
]).

-compile(
    {inline, [
        decode/1,
        decode/2
    ]}
).

-export([is_json/1]).

-compile({inline, [is_json/1]}).

%% See `jiffy:encode_options()`.
-type encode_options() :: [encode_option()].
-type encode_option() :: uescape | pretty | force_utf8.

%% See `jiffy:decode_options()`.
-type decode_options() :: [decode_option()].
-type decode_option() :: return_maps | return_trailer | dedupe_keys | copy_strings.

-type json_text() :: iolist() | binary().
-type json_term() :: jiffy:json_value().
-type json_term_proplist() :: jiffy:json_value() | [{atom() | binary(), json_term_proplist()}].

-export_type([json_text/0, json_term/0]).
-export_type([decode_options/0, encode_options/0]).

-spec encode(json_term()) -> json_text().
encode(Term) ->
    encode(Term, [force_utf8]).

-spec encode(json_term(), encode_options()) -> json_text().
encode(Term, Opts) ->
    to_binary(jiffy:encode(Term, Opts)).

-spec encode_proplist(json_term_proplist()) -> json_text().
encode_proplist(Term) ->
    encode_proplist(Term, [force_utf8]).

-spec encode_proplist(json_term_proplist(), encode_options()) -> json_text().
encode_proplist(Term, Opts) ->
    to_binary(jiffy:encode(to_ejson(Term), Opts)).

-spec safe_encode(json_term()) ->
    {ok, json_text()} | {error, Reason :: term()}.
safe_encode(Term) ->
    safe_encode(Term, [force_utf8]).

-spec safe_encode(json_term(), encode_options()) ->
    {ok, json_text()} | {error, Reason :: term()}.
safe_encode(Term, Opts) ->
    try encode(Term, Opts) of
        Json -> {ok, Json}
    catch
        error:Reason ->
            {error, Reason}
    end.

-spec decode(json_text()) -> json_term().
decode(Json) ->
    decode(Json, [return_maps]).

-spec decode(json_text(), decode_options()) -> json_term().
decode(Json, Opts) ->
    jiffy:decode(Json, Opts).

-spec decode_proplist(json_text()) -> json_term_proplist().
decode_proplist(Json) ->
    from_ejson(jiffy:decode(Json, [])).

-spec safe_decode(json_text()) ->
    {ok, json_term()} | {error, Reason :: term()}.
safe_decode(Json) ->
    safe_decode(Json, [return_maps]).

-spec safe_decode(json_text(), decode_options()) ->
    {ok, json_term()} | {error, Reason :: term()}.
safe_decode(Json, Opts) ->
    try decode(Json, Opts) of
        Term -> {ok, Term}
    catch
        error:Reason ->
            {error, Reason}
    end.

-spec is_json(json_text()) -> boolean().
is_json(Json) ->
    element(1, safe_decode(Json)) =:= ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

-compile(
    {inline, [
        to_ejson/1,
        from_ejson/1
    ]}
).

to_ejson([{_, _} | _] = L) ->
    {[{K, to_ejson(V)} || {K, V} <- L]};
to_ejson(L) when is_list(L) ->
    [to_ejson(E) || E <- L];
to_ejson(M) when is_map(M) ->
    maps:map(fun(_K, V) -> to_ejson(V) end, M);
to_ejson(T) ->
    T.

from_ejson(L) when is_list(L) ->
    [from_ejson(E) || E <- L];
from_ejson({[]}) ->
    [];
from_ejson({L}) ->
    [{Name, from_ejson(Value)} || {Name, Value} <- L];
from_ejson(T) ->
    T.

to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L) ->
    iolist_to_binary(L).
