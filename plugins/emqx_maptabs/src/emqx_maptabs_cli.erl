%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_cli).

-export([
    load/0,
    unload/0,
    cmd/1
]).

load() ->
    emqx_ctl:register_command(maptabs, {?MODULE, cmd}, []).

unload() ->
    emqx_ctl:unregister_command(maptabs).

cmd(["list"]) ->
    print_tables(node(), emqx_maptabs:list_local());
cmd(["status"]) ->
    lists:foreach(
        fun(Node) ->
            print_tables(Node, list_on_node(Node))
        end,
        emqx:running_nodes()
    );
cmd(["load", Path]) ->
    print_result(emqx_maptabs:load_file(Path));
cmd(["reload"]) ->
    print_reload_results(emqx_maptabs:reload_cluster(all));
cmd(["reload", Name]) ->
    print_reload_results(emqx_maptabs:reload_cluster(bin(Name)));
cmd(["get", Name]) ->
    case emqx_maptabs:read_table_file(bin(Name)) of
        {ok, Content} ->
            emqx_ctl:print("~ts~n", [Content]);
        {error, Reason} ->
            print_error(Reason)
    end;
cmd(["delete", Name]) ->
    print_result(emqx_maptabs:delete(bin(Name)));
cmd(_) ->
    emqx_ctl:usage([
        {"maptabs list", "List mapping tables cached on this node"},
        {"maptabs status", "List mapping tables on every running node"},
        {"maptabs load <file>", "Load a table JSON file and replicate it to all nodes"},
        {"maptabs reload [<name>]", "Re-read table files from local disk on all nodes"},
        {"maptabs get <name>", "Print the table file content"},
        {"maptabs delete <name>", "Delete a table on all nodes"}
    ]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

list_on_node(Node) when Node =:= node() ->
    emqx_maptabs:list_local();
list_on_node(Node) ->
    try
        erpc:call(Node, emqx_maptabs, list_local, [], 15000)
    catch
        _:_ -> []
    end.

print_tables(Node, Tables) ->
    emqx_ctl:print("Node: ~ts~n", [atom_to_list(Node)]),
    case Tables of
        [] ->
            emqx_ctl:print("  (no mapping tables)~n");
        _ ->
            lists:foreach(fun print_table/1, Tables)
    end.

print_table(#{
    name := Name,
    row_count := RowCount,
    version := Version,
    hits := Hits,
    misses := Misses
}) ->
    emqx_ctl:print(
        "  ~ts: rows=~w version=~ts hits=~w misses=~w~n",
        [Name, RowCount, Version, Hits, Misses]
    ).

print_reload_results(Results) ->
    lists:foreach(
        fun({Node, Result}) ->
            emqx_ctl:print("~ts: ~ts~n", [atom_to_list(Node), format_result(Result)])
        end,
        Results
    ).

print_result(ok) ->
    emqx_ctl:print("ok~n");
print_result({error, Reason}) ->
    print_error(Reason).

print_error(Reason) ->
    emqx_ctl:print("error: ~0p~n", [Reason]).

format_result(ok) ->
    "ok";
format_result(Results) when is_list(Results) ->
    %% per-table reload results
    io_lib:format("~0p", [Results]);
format_result(Other) ->
    io_lib:format("~0p", [Other]).

bin(Str) ->
    unicode:characters_to_binary(Str).
