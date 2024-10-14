-module(alinkcore_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = alinkcore_sup:start_link(),
    load_formula(),
    start_mods(),
    alinkcore_mqtt:start(),
    alinkcore_dtu_listen:start('modbus:tcp', 6500),
    alinkcore_t_protocol_listen:start(),
    {ok, Sup}.


stop(_State) ->
    alinkcore_mqtt:stop(),
    ok.

%% internal functions
start_mods() ->
    Mods = application:get_env(alinkcore, mods, []),
    [ok = Mod:start() || Mod <- Mods].

load_formula() ->
    File = code:priv_dir(alinkcore) ++ "/alinkiot_funs.erl",
    case file:read_file(File) of
        {ok, Content} ->
            case catch alinkutil_dynamic_compile:load_from_string(binary_to_list(Content)) of
                {module, Mod} ->
                    logger:info("load mod ~p success", [Mod]);
                Err ->
                    logger:error("load funs failed:~p", [Err])
            end;
        {error, Reason} ->
            logger:error("read file ~p failed :~p", [File, Reason])
    end.
