%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([
    best_effort_json/1,
    best_effort_json/2,
    best_effort_json_obj/1,
    format/2,
    json_kv/3,
    json_key/1
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type encode_options() :: [encode_option()].
-type encode_option() :: uescape | pretty | force_utf8.

-type decode_options() :: [decode_option()].
%% `return_trailer | dedupe_keys | copy_strings' are accepted but ignored;
%% they were jiffy-specific. `return_maps' is the default and accepted as a no-op.
-type decode_option() :: return_maps | return_trailer | dedupe_keys | copy_strings.

-type json_text() :: iolist() | binary().
-type json_term() ::
    null
    | boolean()
    | binary()
    | number()
    | atom()
    | [json_term()]
    | #{atom() | binary() => json_term()}
    %% jiffy-style ejson object (preserved for backward compatibility):
    | {[{atom() | binary(), json_term()}]}.
-type json_term_proplist() :: json_term() | [{atom() | binary(), json_term_proplist()}].

-export_type([json_text/0, json_term/0]).
-export_type([decode_options/0, encode_options/0]).

-spec encode(json_term()) -> json_text().
encode(Term) ->
    encode(Term, [force_utf8]).

-spec encode(json_term(), encode_options()) -> json_text().
encode(Term, Opts) ->
    do_encode(Term, encoder(Opts, false), Opts).

-spec encode_proplist(json_term_proplist()) -> json_text().
encode_proplist(Term) ->
    encode_proplist(Term, [force_utf8]).

-spec encode_proplist(json_term_proplist(), encode_options()) -> json_text().
encode_proplist(Term, Opts) ->
    %% `proplist' mode makes the encoder treat any `[{K, V}, ...]' list as a
    %% JSON object, so no pre-pass conversion of the term is needed.
    do_encode(Term, encoder(Opts, true), Opts).

do_encode(Term, Encoder, Opts) ->
    Bin = iolist_to_binary(json:encode(Term, Encoder)),
    case lists:member(pretty, Opts) of
        true -> pretty_print(Bin);
        false -> Bin
    end.

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
    Bin = to_binary(Json),
    case lists:member(return_maps, Opts) of
        true ->
            json:decode(Bin);
        false ->
            %% Empty option list keeps the jiffy-era behavior of returning ejson
            %% form and preserving object key order.
            {Term, _Acc, _Rest} = json:decode(Bin, [], ejson_decoders()),
            Term
    end.

ejson_decoders() ->
    #{
        object_push => fun(K, V, Acc) -> [{K, V} | Acc] end,
        object_finish => fun(Acc, OldAcc) -> {{lists:reverse(Acc)}, OldAcc} end
    }.

-spec decode_proplist(json_text()) -> json_term_proplist().
decode_proplist(Json) ->
    from_ejson(decode(Json, [])).

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

%% @doc Format for logging.
format(Term, Config) ->
    json(Term, Config).

%% @doc Format a list() or map() to JSON object.
%% This is used for CLI result prints,
%% or HTTP API result formatting.
%% The JSON object is pretty-printed.
%% NOTE: do not use this function for logging.
best_effort_json(Input) ->
    best_effort_json(Input, [pretty, force_utf8]).
best_effort_json(Input, Opts) ->
    JsonReady = best_effort_json_obj(Input),
    emqx_utils_json:encode(JsonReady, Opts).

best_effort_json_obj(Input) ->
    Config = #{depth => unlimited, single_line => true, chars_limit => unlimited},
    best_effort_json_obj(Input, Config).

best_effort_json_obj(List, Config) when is_list(List) ->
    try
        json_obj(convert_tuple_list_to_map(List), Config)
    catch
        _:_ ->
            [json(I, Config) || I <- List]
    end;
best_effort_json_obj(Map, Config) ->
    try
        json_obj(Map, Config)
    catch
        _:_ ->
            emqx_utils_log:format("~0p", [Map], Config)
    end.

%% This function will throw if the list do not only contain tuples or if there
%% are duplicate keys.
convert_tuple_list_to_map(List) ->
    %% Crash if this is not a tuple list
    CandidateMap = maps:from_list(List),
    %% Crash if there are duplicates
    NumberOfItems = length(List),
    NumberOfItems = maps:size(CandidateMap),
    CandidateMap.

json(A, _) when is_atom(A) -> A;
json(I, _) when is_integer(I) -> I;
json(F, _) when is_float(F) -> F;
json(P, C) when is_pid(P) -> json(pid_to_list(P), C);
json(P, C) when is_port(P) -> json(port_to_list(P), C);
json(F, C) when is_function(F) -> json(erlang:fun_to_list(F), C);
json(B, Config) when is_binary(B) ->
    best_effort_unicode(B, Config);
json(M, Config) when is_list(M), is_tuple(hd(M)), tuple_size(hd(M)) =:= 2 ->
    best_effort_json_obj(M, Config);
json(L, Config) when is_list(L) ->
    case lists:all(fun erlang:is_binary/1, L) of
        true ->
            %% string array
            L;
        false ->
            try unicode:characters_to_binary(L, utf8) of
                B when is_binary(B) -> B;
                _ -> [json(I, Config) || I <- L]
            catch
                _:_ ->
                    [json(I, Config) || I <- L]
            end
    end;
json(Map, Config) when is_map(Map) ->
    best_effort_json_obj(Map, Config);
json({'$array$', List}, Config) when is_list(List) ->
    [json(I, Config) || I <- List];
json(Term, Config) ->
    emqx_utils_log:format("~0p", [Term], Config).

json_obj(Data, Config) ->
    maps:fold(
        fun(K, V, D) ->
            {K1, V1} = json_kv(K, V, Config),
            maps:put(K1, V1, D)
        end,
        maps:new(),
        Data
    ).

json_kv(K0, V, Config) ->
    K = json_key(K0),
    case is_map(V) of
        true -> {K, best_effort_json_obj(V, Config)};
        false -> {K, json(V, Config)}
    end.

json_key(A) when is_atom(A) -> json_key(atom_to_binary(A, utf8));
json_key(Term) ->
    try unicode:characters_to_binary(Term, utf8) of
        OK when is_binary(OK) andalso OK =/= <<>> ->
            OK;
        _ ->
            throw({badkey, Term})
    catch
        _:_ ->
            throw({badkey, Term})
    end.

%%--------------------------------------------------------------------
%% Encoder
%%--------------------------------------------------------------------

%% Custom encoder fun for `json:encode/2`. Handles:
%%   * jiffy-style ejson objects `{[]}` and `{[{K, V}, ...]}`.
%%   * `Proplist' mode (set by `encode_proplist') — also treat any
%%     `[{K, V}, ...]' list as a JSON object.
%%   * `uescape` — escape every non-ASCII codepoint as `\uXXXX`.
%%   * `force_utf8` — replace invalid UTF-8 bytes with U+FFFD instead of crashing.
encoder(Opts, Proplist) ->
    Uescape = lists:member(uescape, Opts),
    ForceUtf8 = lists:member(force_utf8, Opts),
    fun
        ({[]}, _Enc) ->
            <<"{}">>;
        ({L}, Enc) when is_list(L) ->
            json:encode_key_value_list(L, Enc);
        (B, _Enc) when is_binary(B) ->
            encode_binary(B, Uescape, ForceUtf8);
        ([{_, _} | _] = L, Enc) when Proplist ->
            json:encode_key_value_list(L, Enc);
        (Other, Enc) ->
            json:encode_value(Other, Enc)
    end.

encode_binary(B, Uescape, ForceUtf8) ->
    B1 =
        case ForceUtf8 of
            true -> sanitize_utf8(B);
            false -> B
        end,
    case Uescape of
        true -> json:encode_binary_escape_all(B1);
        false -> json:encode_binary(B1)
    end.

%% Replace invalid UTF-8 byte sequences with the Unicode replacement character
%% U+FFFD, matching jiffy's `force_utf8` behavior. Returns the original binary
%% unchanged when it is already valid UTF-8.
sanitize_utf8(B) ->
    case is_valid_utf8(B) of
        true -> B;
        false -> do_sanitize_utf8(B, <<>>)
    end.

is_valid_utf8(<<>>) -> true;
is_valid_utf8(<<_/utf8, R/binary>>) -> is_valid_utf8(R);
is_valid_utf8(_) -> false.

do_sanitize_utf8(<<>>, Acc) ->
    Acc;
do_sanitize_utf8(<<C/utf8, R/binary>>, Acc) ->
    do_sanitize_utf8(R, <<Acc/binary, C/utf8>>);
do_sanitize_utf8(<<_:8, R/binary>>, Acc) ->
    do_sanitize_utf8(R, <<Acc/binary, 16#FFFD/utf8>>).

%%--------------------------------------------------------------------
%% Pretty printer
%%--------------------------------------------------------------------

%% Pretty-print a compact JSON binary into the format used by the EMQX CLI:
%%   * 2-space indent per nesting level
%%   * `\n<indent>` after `{` `[` `,`
%%   * `\n<indent>` before `}` `]`
%%   * `" : "` between key and value
%%   * empty container body keeps a `\n<inner>\n<outer>` pair
%% This affects the CLI output format, consult the team before changing the format.
pretty_print(Bin) ->
    pretty(Bin, 0, <<>>, false, false).

%% pretty(Rest, Depth, Acc, InString, Escaped)
pretty(<<>>, _D, Acc, _, _) ->
    Acc;
pretty(<<C, R/binary>>, D, Acc, true, true) ->
    pretty(R, D, <<Acc/binary, C>>, true, false);
pretty(<<$\\, R/binary>>, D, Acc, true, false) ->
    pretty(R, D, <<Acc/binary, $\\>>, true, true);
pretty(<<$", R/binary>>, D, Acc, true, false) ->
    pretty(R, D, <<Acc/binary, $">>, false, false);
pretty(<<C, R/binary>>, D, Acc, true, false) ->
    pretty(R, D, <<Acc/binary, C>>, true, false);
pretty(<<$", R/binary>>, D, Acc, false, _) ->
    pretty(R, D, <<Acc/binary, $">>, true, false);
pretty(<<${, R/binary>>, D, Acc, false, _) ->
    D1 = D + 1,
    pretty(R, D1, <<Acc/binary, ${, $\n, (indent(D1))/binary>>, false, false);
pretty(<<$[, R/binary>>, D, Acc, false, _) ->
    D1 = D + 1,
    pretty(R, D1, <<Acc/binary, $[, $\n, (indent(D1))/binary>>, false, false);
pretty(<<$}, R/binary>>, D, Acc, false, _) ->
    D1 = D - 1,
    pretty(R, D1, <<Acc/binary, $\n, (indent(D1))/binary, $}>>, false, false);
pretty(<<$], R/binary>>, D, Acc, false, _) ->
    D1 = D - 1,
    pretty(R, D1, <<Acc/binary, $\n, (indent(D1))/binary, $]>>, false, false);
pretty(<<$,, R/binary>>, D, Acc, false, _) ->
    pretty(R, D, <<Acc/binary, $,, $\n, (indent(D))/binary>>, false, false);
pretty(<<$:, R/binary>>, D, Acc, false, _) ->
    pretty(R, D, <<Acc/binary, " : ">>, false, false);
pretty(<<C, R/binary>>, D, Acc, false, _) ->
    pretty(R, D, <<Acc/binary, C>>, false, false).

indent(D) ->
    binary:copy(<<"  ">>, D).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

-compile(
    {inline, [
        from_ejson/1
    ]}
).

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

best_effort_unicode(Input, Config) ->
    try unicode:characters_to_binary(Input, utf8) of
        B when is_binary(B) -> B;
        _ -> emqx_utils_log:format("~p", [Input], Config)
    catch
        _:_ ->
            emqx_utils_log:format("~p", [Input], Config)
    end.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

-ifdef(TEST).

%% NOTE: pretty-printing format is asserted in the test
%% This affects the CLI output format, consult the team before changing
%% the format.
best_effort_json_test() ->
    ?assertEqual(
        <<"{\n  \n}">>,
        best_effort_json([])
    ),
    ?assertEqual(
        <<"{\n  \"key\" : [\n    \n  ]\n}">>,
        best_effort_json(#{key => []})
    ),
    ?assertEqual(
        <<"[\n  {\n    \"key\" : [\n      \n    ]\n  }\n]">>,
        best_effort_json([#{key => []}])
    ),
    %% List is IO Data
    ?assertMatch(
        #{<<"what">> := <<"hej\n">>},
        emqx_utils_json:decode(best_effort_json(#{what => [<<"hej">>, 10]}))
    ),
    %% Force list to be interpreted as an array
    ?assertMatch(
        #{<<"what">> := [<<"hej">>, 10]},
        emqx_utils_json:decode(
            best_effort_json(#{what => {'$array$', [<<"hej">>, 10]}})
        )
    ),
    %% IO Data inside an array
    ?assertMatch(
        #{<<"what">> := [<<"hej">>, 10, <<"hej\n">>]},
        emqx_utils_json:decode(
            best_effort_json(#{
                what => {'$array$', [<<"hej">>, 10, [<<"hej">>, 10]]}
            })
        )
    ),
    %% Array inside an array
    ?assertMatch(
        #{<<"what">> := [<<"hej">>, 10, [<<"hej">>, 10]]},
        emqx_utils_json:decode(
            best_effort_json(#{
                what => {'$array$', [<<"hej">>, 10, {'$array$', [<<"hej">>, 10]}]}
            })
        )
    ),
    ok.

config() ->
    #{
        chars_limit => unlimited,
        depth => unlimited,
        single_line => true
    }.

string_array_test() ->
    Array = #{<<"arr">> => [<<"a">>, <<"b">>]},
    Encoded = emqx_utils_json:encode(json(Array, config())),
    ?assertEqual(Array, emqx_utils_json:decode(Encoded)).

iolist_test() ->
    Iolist = #{iolist => ["a", ["b"]]},
    Concat = #{<<"iolist">> => <<"ab">>},
    Encoded = emqx_utils_json:encode(json(Iolist, config())),
    ?assertEqual(Concat, emqx_utils_json:decode(Encoded)).

encode_pretty_empty_object_matches_jiffy_format_test() ->
    ?assertEqual(<<"{\n  \n}">>, encode(#{}, [pretty])).

encode_force_utf8_handles_invalid_bytes_test() ->
    Invalid = <<255, 254, 253>>,
    Encoded = encode(Invalid, [force_utf8]),
    %% Round-trips to a valid binary without crashing.
    ?assertMatch(B when is_binary(B), decode(Encoded)).

encode_uescape_escapes_non_ascii_test() ->
    ?assertEqual(<<"\"h\\u00E9llo\"">>, encode(<<"héllo"/utf8>>, [uescape])).

decode_object_returns_map_test() ->
    ?assertEqual(#{<<"a">> => 1}, decode(<<"{\"a\":1}">>)).

decode_invalid_json_throws_under_decode_test() ->
    ?assertError(_, decode(<<"not-json">>)).

safe_decode_returns_error_test() ->
    ?assertMatch({error, _}, safe_decode(<<"not-json">>)).

encode_proplist_roundtrip_test() ->
    %% `decode_proplist/1` strips jiffy's outer 1-tuple wrapper, matching the
    %% pre-existing SUITE expectations in `emqx_utils_json_SUITE`.
    ?assertEqual(
        [{<<"k">>, 1}],
        decode_proplist(encode_proplist({[{<<"k">>, 1}]}))
    ).

is_json_basic_test() ->
    ?assert(is_json(<<"{}">>)),
    ?assertNot(is_json(<<"not-json">>)).

-endif.
