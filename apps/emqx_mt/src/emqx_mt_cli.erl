%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_cli).

-moduledoc """
CLI for multi-tenancy operations.
""".

-behaviour(emqx_ctl).

-export([load/0, unload/0]).

%% CLI handler
-export([mt/1, mt_audit_args/1]).

load() ->
    ok = emqx_ctl:register_command(mt, {?MODULE, mt}, []).

unload() ->
    ok = emqx_ctl:unregister_command(mt).

mt(["purge_ns", Ns0]) ->
    Ns = unicode:characters_to_binary(Ns0),
    case emqx_mt_config:force_purge_ns(Ns) of
        ok ->
            print_json(#{result => <<"ok">>, namespace => Ns});
        {error, cleanup_incomplete} ->
            print_json(#{
                error => <<"cleanup_incomplete">>,
                namespace => Ns,
                hint => <<"some cleanup steps failed; check logs and re-run the command to retry">>
            })
    end;
mt(_) ->
    emqx_ctl:usage([
        {"mt purge_ns <namespace>",
            "Delete the namespace and purge all data belonging to it.\n"
            "Idempotent: purges any leftover data even if the namespace does not exist,\n"
            "e.g. if a previous deletion was interrupted halfway."}
    ]).

-doc "No sensitive arguments to redact from the audit log.".
mt_audit_args(Args) ->
    Args.

print_json(Payload) ->
    emqx_ctl:print("~ts~n", [emqx_utils_json:best_effort_json(Payload)]).
