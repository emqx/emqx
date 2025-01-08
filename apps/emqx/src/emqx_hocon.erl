%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc HOCON schema help module
-module(emqx_hocon).

-export([
    format_path/1,
    check/2,
    check/3,
    compact_errors/2,
    format_error/1,
    format_error/2,
    make_schema/1,
    load_and_check/2
]).

%% @doc Format hocon config field path to dot-separated string in iolist format.
-spec format_path([atom() | string() | binary()]) -> iolist().
format_path([]) -> "";
format_path([Name]) -> iol(Name);
format_path([Name | Rest]) -> [iol(Name), "." | format_path(Rest)].

%% @doc Plain check the input config.
%% The input can either be `richmap' or plain `map'.
%% Always return plain map with atom keys.
-spec check(hocon_schema:schema(), hocon:config() | iodata()) ->
    {ok, hocon:config()} | {error, any()}.
check(Schema, Conf) ->
    %% TODO: remove required
    %% fields should state required or not in their schema
    Opts = #{atom_key => true, required => false},
    check(Schema, Conf, Opts).

check(Schema, Conf, Opts) when is_map(Conf) ->
    try
        {ok, hocon_tconf:check_plain(Schema, Conf, Opts)}
    catch
        throw:Errors:Stacktrace ->
            compact_errors(Errors, Stacktrace)
    end;
check(Schema, HoconText, Opts) ->
    case hocon:binary(HoconText, #{format => map}) of
        {ok, MapConfig} ->
            check(Schema, MapConfig, Opts);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Check if the error error term is a hocon check error.
%% Return {true, FirstError}, otherwise false.
%% NOTE: Hocon tries to be comprehensive, so it returns all found errors
-spec format_error(term()) -> {ok, binary()} | false.
format_error(X) ->
    format_error(X, #{}).

format_error({_Schema, [#{kind := K} = First | Rest] = All}, Opts) when
    K =:= validation_error orelse K =:= translation_error
->
    Update =
        case maps:get(no_stacktrace, Opts, false) of
            true ->
                fun no_stacktrace/1;
            false ->
                fun(X) -> X end
        end,
    case Rest of
        [] ->
            {ok, emqx_logger_jsonfmt:best_effort_json(Update(First), [])};
        _ ->
            {ok, emqx_logger_jsonfmt:best_effort_json(lists:map(Update, All), [])}
    end;
format_error(_Other, _) ->
    false.

make_schema(Fields) ->
    #{roots => Fields, fields => #{}}.

%% Ensure iolist()
iol(B) when is_binary(B) -> B;
iol(A) when is_atom(A) -> atom_to_binary(A, utf8);
iol(L) when is_list(L) -> L.

no_stacktrace(Map) ->
    maps:without([stacktrace], Map).

%% @doc HOCON tries to be very informative about all the detailed errors
%% it's maybe too much when reporting to the user
-spec compact_errors(any(), Stacktrace :: list()) -> {error, any()}.
compact_errors({SchemaModule, Errors}, Stacktrace) ->
    compact_errors(SchemaModule, Errors, Stacktrace);
compact_errors(ErrorContext0, _Stacktrace) when is_map(ErrorContext0) ->
    case ErrorContext0 of
        #{exception := #{schema_module := _Mod, message := _Msg} = Detail} ->
            Error0 = maps:remove(exception, ErrorContext0),
            Error = maps:merge(Error0, Detail),
            {error, Error};
        _ ->
            {error, ErrorContext0}
    end.

compact_errors(SchemaModule, [Error0 | More], _Stacktrace) when is_map(Error0) ->
    Error1 =
        case length(More) of
            0 ->
                Error0;
            N ->
                Error0#{unshown_errors_count => N}
        end,
    Error =
        case is_atom(SchemaModule) of
            true ->
                Error1#{schema_module => SchemaModule};
            false ->
                Error1
        end,
    {error, Error};
compact_errors(SchemaModule, Error, Stacktrace) ->
    %% unexpected, we need the stacktrace reported
    %% if this happens it's a bug in hocon_tconf
    {error, #{
        schema_module => SchemaModule,
        exception => Error,
        stacktrace => Stacktrace
    }}.

%% @doc This is only used in static check scripts in the CI.
-spec load_and_check(module(), file:name_all()) -> {ok, term()} | {error, any()}.
load_and_check(SchemaModule, File) ->
    try
        do_load_and_check(SchemaModule, File)
    catch
        throw:Reason:Stacktrace ->
            compact_errors(Reason, Stacktrace)
    end.

do_load_and_check(SchemaModule, File) ->
    case hocon:load(File, #{format => map}) of
        {ok, Conf} ->
            Opts = #{atom_key => false, required => false},
            %% here we check only the provided root keys
            %% because the examples are all provided only with one (or maybe two) roots
            %% and some roots have required fields.
            RootKeys = maps:keys(Conf),
            {ok, hocon_tconf:check_plain(SchemaModule, Conf, Opts, RootKeys)};
        {error, {parse_error, Reason}} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.
