#!/usr/bin/env escript

%% This script injects implicit relup dependencies for emqx applications.
%%
%% By 'implicit', it means that it is not feasible to define application
%% dependencies in .app.src files.
%%
%% For instance, during upgrade/downgrade, emqx_dashboard usually requires
%% a restart after (but not before) all plugins are upgraded (and maybe
%% restarted), however, the dependencies are not resolvable at build time
%% when relup is generated.
%%
%% This script is to be executed after compile, with the profile given as the
%% first argument. For each dependency, it modifies the .app file to
%% have the `relup_deps` list extended to application attributes.
%%
%% The `relup_deps` application attribute is then picked up by (EMQ's fork of)
%% `relx` when top-sorting apps to generate relup instructions

-mode(compile).

usage() ->
  "Usage: " ++ escript:script_name() ++ " emqx|emqx-edge".

-type app() :: atom().
-type deps_overlay() :: {re, string()} | app().

%% deps/0 returns the dependency overlays.
%% {re, Pattern} to match application names using regexp pattern
-spec deps(string()) -> [{app(), [deps_overlay()]}].
deps("emqx-edge" ++ _) ->
  %% special case for edge
  base_deps() ++ [{{re, ".+"}, [{exclude, App} || App <- edge_excludes()]}];
deps(_Profile) ->
  base_deps().

edge_excludes() ->
    [ emqx_lwm2m
    , emqx_auth_ldap
    , emqx_auth_pgsql
    , emqx_auth_redis
    , emqx_auth_mongo
    , emqx_lua_hook
    , emqx_exhook
    , emqx_exproto
    , emqx_prometheus
    , emqx_psk_file
    ].

base_deps() ->
  %% make sure emqx_dashboard depends on all other emqx_xxx apps
  %% so the appup instructions for emqx_dashboard is always the last
  %% to be executed
  [ {emqx_dashboard, [{re, "emqx_.*"}]}
  , {emqx_management, [{re, "emqx_.*"}, {exclude, emqx_dashboard}, minirest]}
  , {{re, "emqx_.*"}, [emqx]}
  , {emqx_web_hook, [ehttpc]}
  ].

main([Profile | _]) ->
  ok = inject(Profile);
main(_Args) ->
  io:format(standard_error, "~s", [usage()]),
  erlang:halt(1).

expand_names({Name, Deps}, AppNames) ->
  Names = match_pattern(Name, AppNames),
  [{N, Deps} || N <- Names].

%% merge k-v pairs with v1 ++ v2
merge([], Acc) -> Acc;
merge([{K, V0} | Rest], Acc) ->
  V = case lists:keyfind(K, 1, Acc) of
        {K, V1} -> V1 ++ V0;
        false -> V0
      end,
  NewAcc = lists:keystore(K, 1, Acc, {K, V}),
  merge(Rest, NewAcc).

expand_deps([], _AppNames, Acc) -> Acc;
expand_deps([{exclude, Dep} | Deps], AppNames, Acc) ->
  Matches = expand_deps([Dep], AppNames, []),
  expand_deps(Deps, AppNames, Acc -- Matches);
expand_deps([Dep | Deps], AppNames, Acc) ->
  NewAcc = add_to_list(Acc, match_pattern(Dep, AppNames)),
  expand_deps(Deps, AppNames, NewAcc).

inject(Profile) ->
  LibDir = lib_dir(Profile),
  AppNames = list_apps(LibDir),
  Deps0 = lists:flatmap(fun(Dep) -> expand_names(Dep, AppNames) end, deps(Profile)),
  Deps1 = merge(Deps0, []),
  Deps2 = lists:map(fun({Name, DepsX}) ->
                        NewDeps = expand_deps(DepsX, AppNames, []),
                        {Name, NewDeps}
                    end, Deps1),
  lists:foreach(fun({App, Deps}) -> inject(App, Deps, LibDir) end, Deps2).

%% list the profile/lib dir to get all apps
list_apps(LibDir) ->
  Apps = filelib:wildcard("*", LibDir),
  lists:foldl(fun(App, Acc) -> [App || is_app(LibDir, App)] ++ Acc end, [], Apps).

is_app(_LibDir, "." ++ _) -> false; %% ignore hidden dir
is_app(LibDir, AppName) ->
  Path = filename:join([ebin_dir(LibDir, AppName), AppName ++ ".app"]),
  filelib:is_regular(Path) orelse error({unknown_app, AppName, Path}). %% wtf

lib_dir(Profile) ->
  filename:join(["_build", Profile, lib]).

ebin_dir(LibDir, AppName) -> filename:join([LibDir, AppName, "ebin"]).

inject(App0, DepsToAdd, LibDir) ->
  App = str(App0),
  AppEbinDir = ebin_dir(LibDir, App),
  [AppFile0] = filelib:wildcard("*.app", AppEbinDir),
  AppFile = filename:join(AppEbinDir, AppFile0),
  {ok, [{application, AppName, Props}]} = file:consult(AppFile),
  Deps0 = case lists:keyfind(relup_deps, 1, Props) of
              {_, X} -> X;
              false -> []
          end,
  %% merge extra deps, but do not self-include
  Deps = add_to_list(Deps0, DepsToAdd) -- [App0],
  case Deps =:= [] of
    true -> ok;
    _ ->
      NewProps = lists:keystore(relup_deps, 1, Props, {relup_deps, Deps}),
      AppSpec = {application, AppName, NewProps},
      AppSpecIoData = io_lib:format("~p.", [AppSpec]),
      io:format(user, "updated_relup_deps for ~p~n", [App]),
      file:write_file(AppFile, AppSpecIoData)
  end.

str(A) when is_atom(A) -> atom_to_list(A).

match_pattern({re, Re}, AppNames) ->
  Match = fun(AppName) -> re:run(AppName, Re) =/= nomatch end,
  AppNamesToAdd = lists:filter(Match, AppNames),
  AppsToAdd = lists:map(fun(N) -> list_to_atom(N) end, AppNamesToAdd),
  case AppsToAdd =:= [] of
    true  -> error({nomatch, Re});
    false -> AppsToAdd
  end;
match_pattern(NameAtom, AppNames) ->
  case lists:member(str(NameAtom), AppNames) of
    true  -> [NameAtom];
    false -> error({notfound, NameAtom})
  end.

%% Append elements to list without duplication. No reordering.
add_to_list(List, []) -> List;
add_to_list(List, [H | T]) ->
  case lists:member(H, List) of
    true -> add_to_list(List, T);
    false -> add_to_list(List ++ [H], T)
  end.
