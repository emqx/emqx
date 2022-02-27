-module(emqx_relup).

-export([ post_release_upgrade/3
        , post_release_downgrade/3
        ]).

post_release_upgrade(_CurrRelVsn, _FromVsn, _) ->
    reload_components().

post_release_downgrade(_CurrRelVsn, _ToVsn, _) ->
    reload_components().

reload_components() ->
    io:format("reloading resource providers ..."),
    emqx_rule_engine:load_providers(),
    io:format("loading plugins ..."),
    emqx_plugins:load().
