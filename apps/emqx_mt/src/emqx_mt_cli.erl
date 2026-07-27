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
            emqx_ctl:print("ok~n");
        {error, cleanup_incomplete} ->
            emqx_ctl:print("Some cleanup steps failed (see logs); re-run the command to retry.~n")
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
