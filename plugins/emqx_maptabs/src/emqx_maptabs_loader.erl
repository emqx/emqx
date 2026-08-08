%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Parses and validates mapping-table JSON files.
%%
%% A table file is a JSON array of row objects. Each row must have a
%% "key" field; the remaining fields are the row's value map. Validation
%% is fail-closed: any invalid row rejects the whole file.
-module(emqx_maptabs_loader).

-feature(maybe_expr, enable).

-export([
    parse/1,
    validate_name/1,
    table_name_from_path/1
]).

-define(NAME_RE, "^[a-zA-Z0-9_-]+$").

%% Returns `{ok, #{rows := [{Key, ValueMap}], row_count := N, version := Hex}}'
%% or `{error, Reason}'. Keys keep their native JSON type but must be an
%% integer or a string; float keys are rejected to avoid inexact matching.
-spec parse(binary()) -> {ok, map()} | {error, term()}.
parse(Bin) when is_binary(Bin) ->
    maybe
        {ok, Rows0} ?= decode(Bin),
        {ok, Rows} ?= validate_rows(Rows0),
        {ok, #{
            rows => Rows,
            row_count => length(Rows),
            version => version(Bin)
        }}
    end.

-spec validate_name(binary()) -> ok | {error, term()}.
validate_name(Name) when is_binary(Name), Name =/= <<>> ->
    case re:run(Name, ?NAME_RE, [{capture, none}]) of
        match -> ok;
        nomatch -> {error, #{reason => invalid_table_name, name => Name}}
    end;
validate_name(Name) ->
    {error, #{reason => invalid_table_name, name => Name}}.

%% Derives the table name from a file path: base name without the
%% mandatory ".json" extension.
-spec table_name_from_path(file:filename_all()) -> {ok, binary()} | {error, term()}.
table_name_from_path(Path) ->
    Base = filename:basename(Path),
    maybe
        ".json" ?= string:lowercase(filename:extension(Base)),
        Name = unicode:characters_to_binary(filename:rootname(Base)),
        ok ?= validate_name(Name),
        {ok, Name}
    else
        {error, Reason} -> {error, Reason};
        _ -> {error, #{reason => not_a_json_file, path => unicode:characters_to_binary(Path)}}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

decode(Bin) ->
    case emqx_utils_json:safe_decode(Bin) of
        {ok, Rows} when is_list(Rows) ->
            {ok, Rows};
        {ok, Other} ->
            {error, #{reason => not_a_json_array, value => Other}};
        {error, Reason} ->
            {error, #{reason => invalid_json, detail => Reason}}
    end.

validate_rows(Rows) ->
    validate_rows(Rows, 1, #{}, []).

validate_rows([], _N, _Seen, Acc) ->
    {ok, lists:reverse(Acc)};
validate_rows([Row | Rest], N, Seen, Acc) when is_map(Row) ->
    maybe
        {ok, Key} ?= row_key(Row, N),
        ok ?= check_duplicate(Key, Seen, N),
        Values = maps:remove(<<"key">>, Row),
        validate_rows(Rest, N + 1, Seen#{Key => true}, [{Key, Values} | Acc])
    end;
validate_rows([Row | _], N, _Seen, _Acc) ->
    {error, #{reason => row_not_an_object, row_number => N, row => Row}}.

row_key(Row, N) ->
    case maps:find(<<"key">>, Row) of
        {ok, Key} when is_integer(Key); is_binary(Key) ->
            {ok, Key};
        {ok, Key} when is_float(Key) ->
            {error, #{reason => float_key, row_number => N, key => Key}};
        {ok, Key} ->
            {error, #{reason => invalid_key_type, row_number => N, key => Key}};
        error ->
            {error, #{reason => missing_key, row_number => N}}
    end.

check_duplicate(Key, Seen, N) ->
    case is_map_key(Key, Seen) of
        true -> {error, #{reason => duplicate_key, row_number => N, key => Key}};
        false -> ok
    end.

version(Bin) ->
    binary:encode_hex(erlang:md5(Bin), lowercase).
