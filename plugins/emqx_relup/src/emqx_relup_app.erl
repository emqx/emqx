-module(emqx_relup_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([
    start/2,
    stop/1
]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_relup_sup:start_link(),
    emqx_relup_main:load(application:get_all_env()),
    emqx_ctl:register_command(relup, {emqx_relup_cli, cmd}),
    {ok, Sup}.

stop(_State) ->
    emqx_ctl:unregister_command(relup),
    emqx_relup_main:unload().
