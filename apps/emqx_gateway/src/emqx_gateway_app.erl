%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
    {ok, Sup}.

stop(_State) ->
    emqx_gateway_conf:unload(),
    emqx_gateway_cli:unload(),
    ok.

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
