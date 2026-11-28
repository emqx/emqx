%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc HOCON schema that defines _secret_ concept.
-module(emqx_schema_secret).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("typerefl/include/types.hrl").

-export([mk/1]).

%% HOCON Schema API
-export([convert_secret/2, source/1]).

-export([file_sources/1, deny_file_sources/1]).

%% For config checks and for validators
-export([check_opts/1, unwrap_in_validator/2]).

%% @doc Secret value.
-type t() :: binary().

%% @doc Source of the secret value.
%% * "file://...": file path to a file containing secret value.
%% * other binaries: secret value itself.
-type source() :: iodata().

-type secret() :: binary() | function().
-reflect_type([secret/0]).

-define(SCHEMA, #{
    required => false,
    format => <<"password">>,
    sensitive => true,
    converter => fun ?MODULE:convert_secret/2
}).

%% The check option that lets a validator read the file of a `file://` secret.
-define(READ_SECRET_FILES, read_secret_files).

-dialyzer({nowarn_function, source/1}).
%% The argument is known to be a closure, but not to be a secret.
-dialyzer({no_opaque, [file_source/1, is_file_secret/1]}).

%%

-spec mk(#{atom() => _}) -> hocon_schema:field_schema().
mk(Overrides = #{}) ->
    hoconsc:mk(secret(), maps:merge(?SCHEMA, Overrides)).

convert_secret(undefined, #{}) ->
    undefined;
convert_secret(Secret, #{make_serializable := true}) ->
    unicode:characters_to_binary(source(Secret));
convert_secret(Secret, #{}) when is_function(Secret, 0) ->
    Secret;
convert_secret(Secret, #{}) when is_integer(Secret) ->
    wrap(integer_to_binary(Secret));
convert_secret(Secret, #{}) ->
    try unicode:characters_to_binary(Secret) of
        String when is_binary(String) ->
            wrap(String);
        {error, _, _} ->
            throw(invalid_string)
    catch
        error:_ ->
            throw(invalid_type)
    end.

-spec wrap(source()) -> emqx_secret:t(t()).
wrap(<<"file://", _Filename/binary>> = Secret) ->
    emqx_secret:wrap_load({file, Secret});
wrap(Secret) ->
    emqx_secret:wrap(Secret).

-spec source(emqx_secret:t(t())) -> source().
source(Secret) when is_function(Secret) ->
    source(emqx_secret:term(Secret));
source({file, Filename}) ->
    <<"file://", Filename/binary>>;
source(Secret) ->
    Secret.

-doc """
Return the options for a check of a config of `Namespace`.

They tell the validators whether they can read the file of a `file://` secret,
see `unwrap_in_validator/2`. Only a check of the global config can.
""".
-spec check_opts(?global_ns | binary()) -> hocon_tconf:opts().
check_opts(?global_ns) ->
    #{?READ_SECRET_FILES => true};
check_opts(Namespace) when is_binary(Namespace) ->
    #{}.

-doc """
Unwrap a secret in a schema validator.

A validator runs in each check of a config: in the boot script, at boot, for the
body of an API request, in a dry run, for a namespace. Most of these checks do not
know who set the secret, and some run before the secret loader has its rules. So
a validator reads the file of a `file://` secret only in a check that has the
options of `check_opts/1` for the global config. Two checks have them: the one of
`emqx_config_handler` for a config update, and the one of a dry run.

In each other check the result is `file`. The validator must then accept the
secret without a look at its value. The value is checked when it is used.
""".
-spec unwrap_in_validator(emqx_secret:t(Value) | Value, hocon_tconf:opts()) ->
    {value, Value} | file.
unwrap_in_validator(Secret, Opts) ->
    case is_file_secret(Secret) andalso not is_map_key(?READ_SECRET_FILES, Opts) of
        true -> file;
        false -> {value, emqx_secret:unwrap(Secret)}
    end.

is_file_secret(Secret) when is_function(Secret, 0) ->
    case file_source(Secret) of
        {ok, _Source} -> true;
        false -> false
    end;
is_file_secret(_Value) ->
    false.

-doc """
Return the `file://` sources of the secrets in `Term` that load their value from a file.

`Term` is a checked config: a map, a list, or a value in one of them.
""".
-spec file_sources(term()) -> [binary()].
file_sources(Term) ->
    {_, Sources} = deny_file_sources(Term),
    Sources.

-doc """
Replace each secret in `Term` that loads its value from a file with an empty secret.

`Term` is a checked config: a map, a list, or a value in one of them.
Return the new term and the `file://` sources of the replaced secrets.
""".
-spec deny_file_sources(Term) -> {Term, [binary()]} when Term :: term().
deny_file_sources(Term0) ->
    {Term, Sources} = deny_file_sources(Term0, []),
    {Term, lists:reverse(Sources)}.

deny_file_sources(Map, Acc0) when is_map(Map) ->
    maps:fold(
        fun(Key, Value0, {MapAcc, Acc1}) ->
            {Value, Acc} = deny_file_sources(Value0, Acc1),
            {MapAcc#{Key := Value}, Acc}
        end,
        {Map, Acc0},
        Map
    );
deny_file_sources([Head0 | Tail0], Acc0) ->
    {Head, Acc1} = deny_file_sources(Head0, Acc0),
    {Tail, Acc} = deny_file_sources(Tail0, Acc1),
    {[Head | Tail], Acc};
deny_file_sources(Secret, Acc) when is_function(Secret, 0) ->
    case file_source(Secret) of
        {ok, Source} -> {emqx_secret:wrap(<<>>), [Source | Acc]};
        false -> {Secret, Acc}
    end;
deny_file_sources(Term, Acc) ->
    {Term, Acc}.

file_source(Secret) ->
    try emqx_secret:term(Secret) of
        {file, Source} -> {ok, unicode:characters_to_binary(Source)};
        _ -> false
    catch
        %% A closure that is not a secret.
        error:badarg -> false
    end.
