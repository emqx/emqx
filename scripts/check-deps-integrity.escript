#!/usr/bin/env escript

%% NOTE: this script should be executed at project root.

-mode(compile).

main([]) ->
    AppsDir = case filelib:is_file("EMQX_ENTERPRISE") of
                  true -> "lib-ee";
                  false -> "lib-ce"
              end,
    true = filelib:is_dir(AppsDir),
    Files = ["rebar.config"] ++
            apps_rebar_config("apps") ++
            apps_rebar_config(AppsDir),
    Deps = collect_deps(Files, #{}),
    case count_bad_deps(Deps) of
        0 ->
            io:format("OK~n");
        N ->
            io:format(standard_error, "~p dependency discrepancies", [N]),
            halt(1)
    end.

apps_rebar_config(Dir) ->
    filelib:wildcard(filename:join([Dir, "*", "rebar.config"])).

%% collect a kv-list of {DepName, [{DepReference, RebarConfigFile}]}
%% the value part should have unique DepReference
collect_deps([], Acc) -> maps:to_list(Acc);
collect_deps([File | Files], Acc) ->
    Deps =
        try
            {ok, Config} = file:consult(File),
            {deps, Deps0} = lists:keyfind(deps, 1, Config),
            Deps0
        catch
            C : E : St ->
                erlang:raise(C, {E, {failed_to_find_deps_in_rebar_config, File}}, St)
        end,
    collect_deps(Files, do_collect_deps(Deps, File, Acc)).

do_collect_deps([], _File, Acc) -> Acc;
do_collect_deps([{Name, Ref} | Deps], File, Acc) ->
    Refs = maps:get(Name, Acc, []),
    do_collect_deps(Deps, File, Acc#{Name => [{Ref, File} | Refs]}).

count_bad_deps([]) -> 0;
count_bad_deps([{Name, Refs0} | Rest]) ->
    Refs = lists:keysort(1, Refs0),
    case is_unique_ref(Refs) of
        true ->
            count_bad_deps(Rest);
        false ->
            io:format(standard_error, "~p:~n~p~n", [Name, Refs]),
            1 + count_bad_deps(Rest)
    end.

is_unique_ref([_]) -> true;
is_unique_ref([{Ref, _File1}, {Ref, File2} | Rest]) ->
    is_unique_ref([{Ref, File2} | Rest]);
is_unique_ref(_) ->
    false.
