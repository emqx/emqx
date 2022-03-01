-module(emqx_relup).

-export([ post_release_upgrade/2
        , post_release_downgrade/2
        ]).

-define(INFO(FORMAT), io:format("[emqx_relup] " ++ FORMAT ++ "~n")).
-define(INFO(FORMAT, ARGS), io:format("[emqx_relup] " ++ FORMAT ++ "~n", ARGS)).

%% what to do after upgraded from a old release vsn.
post_release_upgrade(_FromRelVsn, _) ->
    ?INFO("emqx has been upgraded to ~s", [emqx_app:get_release()]),
    reload_components().

%% what to do after downgraded to a old release vsn.
post_release_downgrade(_ToRelVsn, _) ->
    ?INFO("emqx has been downgrade to ~s", [emqx_app:get_release()]),
    reload_components().

-ifdef(EMQX_ENTERPRISE).
reload_components() ->
    ?INFO("reloading resource providers ..."),
    emqx_rule_engine:load_providers(),
    ?INFO("reloading module providers ..."),
    emqx_modules:load_providers(),
    ?INFO("loading plugins ..."),
    emqx_plugins:load().
-else.
reload_components() ->
    ?INFO("reloading resource providers ..."),
    emqx_rule_engine:load_providers(),
    ?INFO("loading plugins ..."),
    emqx_plugins:load().
-endif.
