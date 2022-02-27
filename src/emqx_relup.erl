-module(emqx_relup).

-export([ post_release_upgrade/3
        , post_release_downgrade/3
        ]).

post_release_upgrade(_CurrRelVsn, _FromVsn, _) ->
    reload_components().

post_release_downgrade(_CurrRelVsn, _ToVsn, _) ->
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
