%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_backup_sync_cli).

-export([
    load/0,
    unload/0,
    cmd/1
]).

load() ->
    emqx_ctl:register_command(backup_sync, {?MODULE, cmd}, []).

unload() ->
    emqx_ctl:unregister_command(backup_sync).

cmd(["status"]) ->
    print_status(emqx_backup_sync:status());
cmd(_) ->
    emqx_ctl:usage("backup_sync status", "Show backup synchronization status.").

print_status(Status) ->
    Config = maps:get(config, Status, #{}),
    Health = maps:get(health, Status),
    Output = io_lib:format(
        "Node: ~ts~n"
        "Status: ~ts~n"
        "Health: ~ts~n"
        "Enabled: ~ts~n"
        "Worker: ~ts~n"
        "Selected core node: ~ts~n"
        "Next sync: ~ts~n"
        "Primary API base URL: ~ts~n"
        "Interval: ~ts~n"
        "Root keys: ~ts~n"
        "Table sets: ~ts~n",
        [
            format_value(maps:get(node, Status)),
            format_value(maps:get(status, Status)),
            format_health(Health),
            format_value(maps:get(enabled, Status)),
            format_value(maps:get(worker, Status)),
            format_value(maps:get(selected_core_node, Status)),
            format_next_sync(maps:get(next_sync_ms, Status)),
            format_value(maps:get(primary_base_url, Config, <<>>)),
            format_value(maps:get(interval, Config, <<>>)),
            format_list(maps:get(root_keys, Config, [])),
            format_list(maps:get(table_sets, Config, []))
        ]
    ),
    emqx_ctl:print("~s", [Output]).

format_health(ok) ->
    "ok";
format_health({error, Reason}) ->
    ["error: ", format_value(Reason)].

format_next_sync(undefined) ->
    "not scheduled";
format_next_sync(Ms) when is_integer(Ms) ->
    io_lib:format("~w ms", [Ms]).

format_list([]) ->
    "[]";
format_list(Items) ->
    string:join([unicode:characters_to_list(format_value(Item)) || Item <- Items], ",").

format_value(Value) when is_binary(Value) ->
    unicode:characters_to_list(Value);
format_value(Value) when is_atom(Value) ->
    atom_to_list(Value);
format_value(Value) when is_list(Value) ->
    Value;
format_value(Value) ->
    io_lib:format("~0p", [Value]).
