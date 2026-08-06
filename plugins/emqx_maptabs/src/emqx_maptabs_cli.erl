%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% CLI for the emqx_maptabs plugin. All command output is JSON.
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
    print_json(emqx_maptabs:list_local());
cmd(["status"]) ->
    print_json([status_entry(Node) || Node <- emqx:running_nodes()]);
cmd(["load", Path]) ->
    print_result(emqx_maptabs:load_file(Path));
cmd(["reload"]) ->
    print_reload_results(emqx_maptabs:reload_cluster(all));
cmd(["reload", Name]) ->
    print_reload_results(emqx_maptabs:reload_cluster(bin(Name)));
cmd(["get", Name]) ->
    case emqx_maptabs:read_table_file(bin(Name)) of
        {ok, Content} ->
            %% the table file content is already JSON
            emqx_ctl:print("~ts~n", [Content]);
        {error, Reason} ->
            print_result({error, Reason})
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

status_entry(Node) ->
    case emqx_maptabs:list_on_node(Node) of
        Tables when is_list(Tables) ->
            #{node => Node, tables => Tables};
        {error, Reason} ->
            #{node => Node, error => json_term(Reason)}
    end.

print_reload_results(Results) ->
    print_json([
        #{node => Node, result => reload_result(Result)}
     || {Node, Result} <- Results
    ]).

%% per-node reload result: `ok', `{error, _}', or a per-table list
reload_result(Results) when is_list(Results) ->
    [#{table => Name, result => reload_result(Result)} || {Name, Result} <- Results];
reload_result(ok) ->
    ok;
reload_result({error, Reason}) ->
    #{result => error, reason => json_term(Reason)};
reload_result(Other) ->
    json_term(Other).

print_result(ok) ->
    print_json(#{result => ok});
print_result({error, Reason}) ->
    print_json(#{result => error, reason => json_term(Reason)}).

print_json(Term) ->
    emqx_ctl:print("~ts~n", [emqx_utils_json:encode(json_term(Term))]).

%% best-effort conversion of internal terms to JSON-encodable structures
json_term(Term) when is_binary(Term); is_number(Term); is_atom(Term) ->
    Term;
json_term(Term) when is_map(Term) ->
    maps:from_list([{json_key(K), json_term(V)} || {K, V} <- maps:to_list(Term)]);
json_term(Term) when is_list(Term) ->
    [json_term(E) || E <- Term];
json_term(Term) ->
    format_bin(Term).

json_key(K) when is_atom(K); is_binary(K) -> K;
json_key(K) -> format_bin(K).

format_bin(Term) ->
    iolist_to_binary(io_lib:format("~0p", [Term])).

bin(Str) ->
    unicode:characters_to_binary(Str).
