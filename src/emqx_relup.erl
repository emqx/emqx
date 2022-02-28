-module(emqx_relup).

-export([ post_release_upgrade/2
        , post_release_downgrade/2
        ]).

%% what to do after upgraded from a old release vsn.
post_release_upgrade(_FromRelVsn, _) ->
    reload_components().

%% what to do after downgraded to a old release vsn.
post_release_downgrade(_ToRelVsn, _) ->
    reload_components().

-ifdef(EMQX_ENTERPRISE).
reload_components() ->
    io:format("reloading resource providers ...~n"),
    emqx_rule_engine:load_providers(),
    io:format("reloading module providers ...~n"),
    emqx_modules:load_providers(),
    io:format("loading plugins ...~n"),
    emqx_plugins:load().
-else.
reload_components() ->
    io:format("reloading resource providers ...~n"),
    emqx_rule_engine:load_providers(),
    io:format("loading plugins ...~n"),
    emqx_plugins:load().
-endif.
