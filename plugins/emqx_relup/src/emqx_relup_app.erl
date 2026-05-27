-module(emqx_relup_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([
    start/2,
    stop/1
]).

start(_StartType, _StartArgs) ->
    ok = warn_on_invalid_catalog(),
    {ok, Sup} = emqx_relup_sup:start_link(),
    emqx_relup_main:load(application:get_all_env()),
    emqx_ctl:register_command(relup, {emqx_relup_cli, cmd}),
    {ok, Sup}.

warn_on_invalid_catalog() ->
    {_Valid, Errors} = emqx_relup_handler:validate_priv_catalog(),
    lists:foreach(
        fun(ErrMap) ->
            logger:warning(ErrMap#{msg => relup_catalog_entry_invalid})
        end,
        Errors
    ),
    ok.

stop(_State) ->
    emqx_ctl:unregister_command(relup),
    emqx_relup_main:unload().
