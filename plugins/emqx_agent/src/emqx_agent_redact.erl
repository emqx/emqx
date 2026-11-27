%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_agent_redact).

-moduledoc """
Credential masking for Agent configs. Most of the work is done by
`emqx_utils:redact/1` and `emqx_utils:deobfuscate/2`. This module adds header
lists of maps with name and value keys
(`[#{~"name" => ~"Authorization", ~"value" => ~"secret"}]`), the form the
Agent config uses for HTTP tool headers. `emqx_utils_redact` before 6.3.4
leaves such lists unchanged. With a newer broker, the lists are already
masked or restored, and the passes here change nothing.
""".

-export([redact/1, deobfuscate/2]).

-define(HEADERS, <<"headers">>).
-define(NAME, <<"name">>).
-define(VALUE, <<"value">>).
-define(REDACT_VAL, <<"******">>).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Mask credentials in a config, including sensitive headers in header lists.".
-spec redact(term()) -> term().
redact(Term) ->
    redact_header_lists(emqx_utils:redact(Term)).

-doc """
Replace masked values in a submitted config with the stored values.
A masked header is restored from the last stored header with exactly the same name.
""".
-spec deobfuscate(map(), map()) -> map().
deobfuscate(NewConf, OldConf) ->
    restore_header_lists(emqx_utils:deobfuscate(NewConf, OldConf), OldConf).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

redact_header_lists(Map) when is_map(Map) ->
    maps:map(fun redact_field/2, Map);
redact_header_lists([X | Xs]) ->
    %% Note: we could be dealing with an improper list
    [redact_header_lists(X) | redact_header_lists(Xs)];
redact_header_lists(Term) ->
    Term.

redact_field(?HEADERS, Headers) when is_list(Headers) ->
    lists:map(fun redact_header/1, Headers);
redact_field(_Key, Value) ->
    redact_header_lists(Value).

redact_header(#{?NAME := Name, ?VALUE := Value} = Header) when map_size(Header) == 2 ->
    %% The broker decides whether the header is sensitive and how to mask the value.
    [{_, Masked}] = emqx_utils_redact:redact_headers([{Name, Value}]),
    Header#{?VALUE := Masked};
redact_header(Term) ->
    Term.

%% Only maps are walked, the same way as emqx_utils:deobfuscate/2 does.
restore_header_lists(NewConf, OldConf) when is_map(NewConf) ->
    maps:map(fun(Key, Value) -> restore_field(Key, Value, OldConf) end, NewConf);
restore_header_lists(NewConf, _OldConf) ->
    NewConf.

%% OldConf is the stored value that holds the field, of any type.
restore_field(?HEADERS, Headers, #{?HEADERS := OldHeaders}) when
    is_list(Headers), is_list(OldHeaders)
->
    restore_header_list(Headers, OldHeaders);
restore_field(?HEADERS, Headers, _OldConf) when is_list(Headers) ->
    restore_header_list(Headers, []);
restore_field(Key, Value, #{} = OldConf) ->
    case OldConf of
        #{Key := OldValue} -> restore_header_lists(Value, OldValue);
        #{} -> restore_header_lists(Value, #{})
    end;
restore_field(_Key, Value, _OldConf) ->
    restore_header_lists(Value, #{}).

restore_header_list(Headers, OldHeaders) ->
    lists:flatmap(fun(Header) -> restore_header(Header, OldHeaders) end, Headers).

%% A masked header is looked up in the stored list by its exact name, and the
%% last stored header with that name is used. Names that differ in case do not
%% match: emqx_agent_config normalizes header names before restoring. A masked
%% header without a stored value is dropped.
restore_header(#{?NAME := Name, ?VALUE := ?REDACT_VAL} = Header, OldHeaders) when
    map_size(Header) == 2
->
    case is_sensitive_header(Name) of
        true ->
            case find_header(Name, OldHeaders) of
                {ok, OldValue} -> [Header#{?VALUE := OldValue}];
                error -> []
            end;
        false ->
            [Header]
    end;
restore_header(Header, _OldHeaders) ->
    [Header].

is_sensitive_header(Name) ->
    Probe = <<"probe">>,
    emqx_utils_redact:redact_headers([{Name, Probe}]) =/= [{Name, Probe}].

find_header(Name, OldHeaders) ->
    case [V || #{?NAME := N, ?VALUE := V} = H <- OldHeaders, map_size(H) == 2, N =:= Name] of
        [] -> error;
        Matching -> {ok, lists:last(Matching)}
    end.
