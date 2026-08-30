%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% `emqx ctl actions ...`: inspect action status on the local node, in JSON.
%%
%% Unlike `GET /api/v5/actions/{id}`, this never RPCs to other nodes and never
%% aggregates status across the cluster (see `emqx_bridge_v2_api:aggregate_status/1`).
%% It answers "is *this* node's action usable", which is what a per-pod readiness
%% probe needs and the cluster-aggregated REST `status` field cannot provide.
%%
%% Scope: actions only. Sources and connectors share the same `emqx_bridge_v2`
%% primitives (see `?ROOT_KEY_SOURCES`), so adding `emqx ctl sources ...` later is
%% a matter of adding another top-level clause with its own root key, not a rewrite.
-module(emqx_bridge_v2_cli).

-include_lib("emqx/include/emqx_config.hrl").

-export([load/0, unload/0]).
-export([actions/1, actions_audit_args/1]).

-define(ROOT_KEY_ACTIONS, actions).

%%--------------------------------------------------------------------------------
%% Loading and unloading
%%--------------------------------------------------------------------------------

load() ->
    emqx_ctl:register_command(actions, {?MODULE, actions}, []).

unload() ->
    emqx_ctl:unregister_command(actions).

%% Arguments are a type:name selector and a namespace, never secrets.
actions_audit_args(Args) -> Args.

%%--------------------------------------------------------------------------------
%% `emqx ctl actions ...`
%%--------------------------------------------------------------------------------

actions(["show" | Args]) ->
    run(show, Args);
actions(["status" | Args]) ->
    run(status, Args);
actions(_) ->
    usage().

usage() ->
    emqx_ctl:usage([
        {"actions show [--name <type:name>] [--ns <namespace>]",
            "Show local action config (secrets redacted) and status as JSON.\n"
            "Omit --name to show every action; omit --ns for the global namespace.\n"
            "Always exits 0 on a successful lookup, printing `null` if --name matches\n"
            "nothing; pipe to `jq -e` to turn that into a failing check."},
        {"actions status [--name <type:name>] [--ns <namespace>]",
            "Show local action status as a compact JSON array of\n"
            "{\"<type>:<name>\": \"<status>\"} objects.\n"
            "Omit --name to show every action; omit --ns for the global namespace.\n"
            "Always exits 0 on a successful lookup, printing `[]` if --name matches\n"
            "nothing; pipe to `jq -e` to turn that into a failing check."}
    ]).

run(Cmd, Args) ->
    case collect_opts(Args, #{}) of
        {ok, Opts} ->
            Result = query(Cmd, Opts),
            emqx_ctl:print("~ts~n", [emqx_utils_json:encode(Result, [pretty, force_utf8])]);
        {error, Reason} ->
            emqx_ctl:warning("~ts~n", [Reason]),
            usage()
    end.

%%--------------------------------------------------------------------------------
%% Option parsing
%%
%% `--name` and `--ns` are flags rather than positional arguments, which most
%% `emqx ctl` sub-commands do not need. No dedicated flag-parsing helper exists in
%% `emqx_ctl` or `emqx_mgmt_cli` (checked); the house pattern for the few commands
%% that do take flags (e.g. `emqx_mgmt_cli:collect_session_top_args/2`) is a small
%% recursive accumulator, which this follows.
%%--------------------------------------------------------------------------------

collect_opts([], Acc) ->
    {ok, Acc};
collect_opts(["--name", NameStr | Rest], Acc) ->
    case parse_type_name(NameStr) of
        {ok, TypeName} ->
            collect_opts(Rest, Acc#{name => TypeName});
        error ->
            {error, "invalid --name value, expected <type>:<name>"}
    end;
collect_opts(["--ns", NsStr | Rest], Acc) ->
    collect_opts(Rest, Acc#{ns => unicode:characters_to_binary(NsStr)});
collect_opts(Args, _Acc) ->
    {error, io_lib:format("unknown arguments: ~p", [Args])}.

parse_type_name(Str) ->
    case string:split(Str, ":", leading) of
        [Type, Name] when Type =/= "", Name =/= "" ->
            {ok, {unicode:characters_to_binary(Type), unicode:characters_to_binary(Name)}};
        _ ->
            error
    end.

%%--------------------------------------------------------------------------------
%% Query and formatting
%%--------------------------------------------------------------------------------

query(Cmd, Opts) ->
    Namespace = maps:get(ns, Opts, ?global_ns),
    case maps:find(name, Opts) of
        {ok, {Type, Name}} ->
            case emqx_bridge_v2:lookup(Namespace, ?ROOT_KEY_ACTIONS, Type, Name) of
                {ok, Info} -> single_result(Cmd, Info);
                {error, not_found} -> not_found_result(Cmd)
            end;
        error ->
            Infos = emqx_bridge_v2:list(Namespace, ?ROOT_KEY_ACTIONS),
            list_result(Cmd, Infos)
    end.

%% `show --name` prints the single object, matching the REST `GET .../actions/{id}` shape.
single_result(show, Info) ->
    format_show(Info);
%% `status` always prints an array, even for a single `--name`, per the issue's own examples.
single_result(status, Info) ->
    [format_status(Info)].

%% `--name` matching nothing is not an error: it is valid, quiet input for a probe.
%% `null`/`[]` are both falsy to `jq -e`, so a probe script does not need to special-case it.
not_found_result(show) -> null;
not_found_result(status) -> [].

list_result(show, Infos) ->
    [format_show(Info) || Info <- Infos];
list_result(status, Infos) ->
    [format_status(Info) || Info <- Infos].

format_status(#{type := Type, name := Name, status := Status}) ->
    #{<<Type/binary, ":", Name/binary>> => Status}.

%% Mirrors `emqx_bridge_v2_api:format_resource/3`, minus the cluster-only fields
%% (`node_status`, `rules`) that command reuses `aggregate_status/1` to build, and minus
%% schema-based default-filling, which needs no local-probe justification.
%%
%% `resource_data` (the connector's live resource state: pids, refs, ...) is deliberately
%% left out entirely, same as `format_resource/3` does — it is not JSON-encodable, and there
%% is nothing in it a readiness probe needs beyond `status`/`error`, which `lookup/4` already
%% surfaces.
%%
%% `raw_config` can carry connector secrets set directly on the action (e.g. an HTTP action's
%% `parameters.headers` may hold a bearer token) alongside the action's own reference to a
%% connector by name. Redact exactly like the REST path does before this ever reaches a
%% terminal.
format_show(#{
    namespace := Namespace,
    type := Type,
    name := Name,
    raw_config := RawConf,
    status := Status,
    error := Error
}) ->
    maps:merge(emqx_utils:redact(RawConf), #{
        <<"namespace">> => namespace_out(Namespace),
        <<"type">> => Type,
        <<"name">> => Name,
        <<"node">> => node(),
        <<"status">> => Status,
        <<"error">> => Error
    }).

namespace_out(?global_ns) -> null;
namespace_out(Namespace) when is_binary(Namespace) -> Namespace.
