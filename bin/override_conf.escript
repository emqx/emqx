#!/usr/bin/env escript
%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%%! -pa _build/default/lib/cuttlefish/ebin

main(Args) ->
    try
        #{etc := EtcDir, lib := LibDir} = parse_args(Args),
        KSs = load_keys(LibDir),
        Matches = match_schema(getenv(), KSs),
        override_conf(Matches, EtcDir)
    catch
        _:Reason ->
            io:format("error when reading environment values: ~p~n", [Reason]),
            halt(1)
    end.

parse_args(Args) ->
    parse_args(Args, #{}).
parse_args([], Res) ->
    Res;
parse_args(["-l", LibDir| More], Res) ->
    parse_args(More, Res#{lib => LibDir});
parse_args(["-e", EtcDir| More], Res) ->
    parse_args(More, Res#{etc => EtcDir}).

getenv() ->
    Envs = lists:map(fun (E) -> string:split(E, "=", leading) end, os:getenv()),
    do_getenv(Envs, []).

do_getenv([], Acc) ->
    lists:sort(Acc);
do_getenv([["EMQX_" ++ Key, Value]|More], Acc) ->
    do_getenv(More, [{cuttlefish_variable:tokenize(replace(Key)), maybe_quote(Value)}|Acc]);
do_getenv([_Other|More], Acc) ->
    do_getenv(More, Acc).

replace(Key) ->
    string:lowercase(string:replace(Key, "__", ".", all)).

maybe_quote(Value) when Value =:= "true" orelse Value =:= "false" orelse Value =:= "null" ->
    Value;
maybe_quote(Value) ->
    Float = (catch list_to_float(Value)),
    Int = (catch list_to_integer(Value)),
    case {is_number(Float), is_number(Int)} of
        {false, false} -> "\"" ++ Value ++ "\"";
        {_, _} -> Value
    end.

match_schema(Envs, Mappings) ->
    do_match_schema(Envs, Mappings, []).

do_match_schema([], _, Res) ->
    lists:reverse(Res);
do_match_schema([{Key, Value}|MoreE]=Envs, [{KeyM, File}|MoreM]=Mappings, Res) ->
    case cuttlefish_variable:is_fuzzy_match(Key, KeyM) of
        true ->
            Basename = filename:basename(File, ".schema"),
            do_match_schema(MoreE, Mappings, [{Key, Value, Basename}|Res]);
        false ->
            if
                Key > KeyM -> do_match_schema(Envs, MoreM, Res);
                Key < KeyM -> do_match_schema(MoreE, Mappings, Res)
            end
    end.

load_keys(LibDir) ->
    Files = [ filename:join(LibDir, Filename)  || Filename <- filelib:wildcard("emqx*/priv/*.schema", LibDir)],
    do_load_keys(Files).

do_load_keys(Files) ->
    do_load_keys(Files, []).
do_load_keys([], Acc) ->
    lists:sort(Acc);
do_load_keys([File|More], Acc) ->
    {_, Mappings, _} = cuttlefish_schema:files([File]),
    KSs = lists:map(fun (Mapping) -> {element(2, Mapping), File} end, Mappings),
    do_load_keys(More, lists:reverse(KSs, Acc)).

override_conf(Matches, EtcDir) ->
    ensure_deleted(EtcDir),
    [ write(M, EtcDir) || M <- Matches ].

ensure_deleted(EtcDir) ->
    file:delete(filename:join(EtcDir, "emqx.conf.override")),
    Files = [ filename:join(EtcDir, Filename)  || Filename <- filelib:wildcard("*/*.conf.override", EtcDir)],
    lists:map(fun file:delete/1, Files).

write({Key, Value, "emqx"}, EtcDir) ->
    File = filename:join([EtcDir, "emqx" ++ ".conf.override"]),
    do_write(string:join(Key, "."), Value, File);
write({Key, Value, Basename}, EtcDir) ->
    File = filename:join([EtcDir, "plugins", Basename ++ ".conf.override"]),
    do_write(string:join(Key, "."), Value, File).

do_write(Key, Value, File) ->
    {ok, IoDevice} = file:open(File, [append]),
    file:write(IoDevice, io_lib:format("~s = ~s~n", [Key, Value])).
