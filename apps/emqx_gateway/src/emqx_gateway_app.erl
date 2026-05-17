%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_app).

-behaviour(application).

-include_lib("emqx/include/logger.hrl").

-export([start/2, stop/1]).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_gateway_sup:start_link(),
    emqx_gateway_cli:load(),
    load_default_gateway_applications(),
    load_gateway_by_default(),
    emqx_gateway_conf:load(),
    ok = register_session_count_callback(),
    {ok, Sup}.

stop(_State) ->
    ok = unregister_session_count_callback(),
    emqx_gateway_conf:unload(),
    emqx_gateway_cli:unload(),
    ok.

%% emqx_license is a soft dependency: register only if the registry module is
%% on the code path. This keeps standalone gateway tests (which do not bundle
%% emqx_license config) from failing to boot.
register_session_count_callback() ->
    try
        emqx_license_session_count:register_callback(
            ?MODULE, fun emqx_gateway_cm_registry:get_connected_client_count/0
        )
    catch
        error:undef -> ok
    end.

unregister_session_count_callback() ->
    try
        emqx_license_session_count:unregister_callback(?MODULE)
    catch
        error:undef -> ok
    end.

%%--------------------------------------------------------------------
%% Internal funcs

load_default_gateway_applications() ->
    lists:foreach(
        fun(Def) ->
            load_gateway_application(Def)
        end,
        emqx_gateway_utils:find_gateway_definitions()
    ).

load_gateway_application(
    #{
        name := Name,
        callback_module := CbMod,
        config_schema_module := SchemaMod
    }
) ->
    RegistryOptions = [{cbkmod, CbMod}, {schema, SchemaMod}],
    case emqx_gateway_registry:reg(Name, RegistryOptions) of
        ok ->
            ?SLOG(debug, #{
                msg => "register_gateway_succeed",
                callback_module => CbMod
            });
        {error, already_registered} ->
            ?SLOG(error, #{
                msg => "gateway_already_registered",
                name => Name,
                callback_module => CbMod
            })
    end;
load_gateway_application(_) ->
    ?SLOG(error, #{
        msg => "invalid_gateway_defination"
    }).

load_gateway_by_default() ->
    load_gateway_by_default(confs()).

load_gateway_by_default([]) ->
    ok;
load_gateway_by_default([{Type, Confs} | More]) ->
    case emqx_gateway_registry:lookup(Type) of
        undefined ->
            ?SLOG(error, #{
                msg => "skip_to_load_gateway",
                gateway_name => Type
            });
        _ ->
            case emqx_gateway:load(Type, Confs) of
                {ok, _} ->
                    ?SLOG(debug, #{
                        msg => "load_gateway_succeed",
                        gateway_name => Type
                    });
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "load_gateway_failed",
                        gateway_name => Type,
                        reason => Reason
                    })
            end
    end,
    load_gateway_by_default(More).

confs() ->
    maps:to_list(emqx_conf:get([gateway], #{})).
