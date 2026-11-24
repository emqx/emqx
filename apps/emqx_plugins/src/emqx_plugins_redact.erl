%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_redact).

-moduledoc """
Credential masking for plugin configs returned and accepted by the plugin config API.

The main differences from common `emqx_utils_redact`:
- This module works with binaries since it is used to redact and deobfuscate only Avro JSON.
  Non-binary values are not masked, because a binary mask would violate the Avro schema.
- For redacting values, it handles unions with string type, for example
  `#{<<"password">> => #{<<"string">> => <<"secret">>}}` is masked as
  `#{<<"password">> => #{<<"string">> => <<"******">>}}`.
- It deobfuscates lists of entities presented as maps or records identifying them by
  `id` or `type + id` fields. Unmatched masked values are dropped.

These differences come from Avro specifics of the plugin configs.
""".

-export([redact/2, deobfuscate/3]).

-define(HEADERS, <<"headers">>).

-doc "Mask the credentials in a plugin config.".
-spec redact(binary() | string(), map()) -> map().
redact(_NameVsn, Config) when is_map(Config) ->
    redact_map(Config).

-doc """
Replace the masked values in a submitted plugin config with the stored values.
A masked value without a stored value is dropped.
""".
-spec deobfuscate(binary() | string(), map(), map()) -> map().
deobfuscate(_NameVsn, Config, OldConfig) when is_map(Config), is_map(OldConfig) ->
    restore_map(Config, OldConfig).

%%--------------------------------------------------------------------
%% Redact
%%--------------------------------------------------------------------

redact_map(Map) ->
    maps:map(fun redact_field/2, Map).

redact_field(Key, Value) ->
    case is_sensitive_key(Key) of
        true -> redact_secret(Value);
        false when Key =:= ?HEADERS -> emqx_utils_redact:redact_headers(Value);
        false -> redact_any(Value)
    end.

redact_any(Map) when is_map(Map) ->
    redact_map(Map);
redact_any(List) when is_list(List) ->
    [redact_any(Elem) || Elem <- List];
redact_any(Value) ->
    Value.

%% Mask the value of a sensitive field. Only a binary (a JSON string) is masked,
%% because the mask is a binary and must stay valid for the field's Avro type:
%%   ~"p"                     -> ~"******"
%%   #{~"string" => ~"p"}     -> #{~"string" => ~"******"}   (union branch kept)
%%   null, 7, true, [~"p"]    -> not changed
%%   #{~"user" => ~"u", ~"password" => ~"p"}
%%                            -> #{~"user" => ~"u", ~"password" => ~"******"}
%%                               (a record is not replaced, its fields are checked)
redact_secret(Value) when is_binary(Value) ->
    mask(Value);
redact_secret(#{} = Union) when map_size(Union) =:= 1 ->
    case maps:to_list(Union) of
        [{Branch, Value}] when is_binary(Value) -> #{Branch => mask(Value)};
        _ -> redact_any(Union)
    end;
redact_secret(Value) ->
    redact_any(Value).

%% Mask a binary with the generic redaction, which returns a binary for a binary:
%%   ~"p"              -> ~"******"
%%   ~"file:///etc/pw" -> not changed (a file path)
%%   ~"${token}"       -> not changed (a single placeholder)
mask(Value) ->
    #{<<"password">> := Masked} = emqx_utils:redact(#{<<"password">> => Value}),
    Masked.

%%--------------------------------------------------------------------
%% Restore
%%--------------------------------------------------------------------

%% A field whose masked value has no stored value is left out of the result.
restore_map(Map, OldMap) ->
    maps:filtermap(fun(Key, Value) -> restore_field(Key, Value, OldMap) end, Map).

%% Returns `{true, Restored}', or `false' to leave the field out.
%% OldMap is the stored map that holds the field.
restore_field(Key, Value, OldMap) ->
    case is_sensitive_key(Key) of
        true -> restore_secret(Key, Value, OldMap);
        false when Key =:= ?HEADERS -> {true, restore_headers(Value, OldMap)};
        false -> {true, restore_nested(Key, Value, OldMap)}
    end.

restore_secret(Key, Value, OldMap) ->
    case {is_masked(Value), OldMap} of
        {true, #{Key := OldValue}} -> {true, OldValue};
        {true, #{}} -> false;
        {false, #{}} -> {true, restore_nested(Key, Value, OldMap)}
    end.

restore_headers(Headers, OldMap) ->
    #{?HEADERS := Restored} = emqx_utils_redact:deobfuscate(#{?HEADERS => Headers}, OldMap),
    Restored.

restore_nested(Key, Map, OldMap) when is_map(Map) ->
    case OldMap of
        #{Key := #{} = OldValue} -> restore_map(Map, OldValue);
        #{} -> restore_map(Map, #{})
    end;
restore_nested(Key, List, OldMap) when is_list(List) ->
    case OldMap of
        #{Key := OldValue} when is_list(OldValue) -> restore_list(List, OldValue);
        #{} -> restore_list(List, [])
    end;
restore_nested(_Key, Value, _OldMap) ->
    Value.

restore_list(List, OldList) ->
    OldRecords = [E || E <- OldList, entry_id(E) =/= error],
    [restore_record(Elem, OldRecords) || Elem <- List].

restore_record(Elem, OldRecords) when is_map(Elem) ->
    Id = entry_id(Elem),
    case [E || Id =/= error, E <- OldRecords, entry_id(E) =:= Id] of
        [OldRecord | _] -> restore_map(Elem, OldRecord);
        [] -> restore_map(Elem, #{})
    end;
restore_record(List, _OldRecords) when is_list(List) ->
    restore_list(List, []);
restore_record(Elem, _OldRecords) ->
    Elem.

entry_id(Elem) when is_map(Elem) ->
    case record_body(Elem) of
        #{<<"id">> := Id} = Body -> {ok, {maps:get(<<"type">>, Body, undefined), Id}};
        _ -> error
    end;
entry_id(_Elem) ->
    error.

%% Avro JSON encodes a union of records as a map with the record name as its
%% only key, for example #{~"Http" => #{~"type" => ~"http", ~"id" => ~"a"}}.
%%
%% Any map with one key and a map value is taken as such a union value. The
%% record name is not checked: Avro allows any name, for example ~"http" or
%% ~"com.example.Http". A record with one field whose value is a map, such as
%% #{~"config" => #{~"id" => ~"a"}}, looks the same and is also unwrapped.
%% Matching is still correct in that case: the submitted and the stored entries
%% have the same shape, so both identities are read from the same inner map.
record_body(Elem) ->
    case maps:to_list(Elem) of
        [{_Record, #{} = Body}] -> Body;
        _ -> Elem
    end.

is_masked(Value) when is_binary(Value) ->
    Value =:= emqx_utils_redact:redacted_value();
is_masked(#{} = Union) when map_size(Union) =:= 1 ->
    maps:values(Union) =:= [emqx_utils_redact:redacted_value()];
is_masked(_) ->
    false.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

is_sensitive_key(Key) ->
    emqx_utils_redact:is_sensitive_key(Key).
