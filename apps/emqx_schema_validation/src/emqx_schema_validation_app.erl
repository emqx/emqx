%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_validation_app).

-behaviour(application).

%% `application' API
-export([start/2, stop/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `application' API
%%------------------------------------------------------------------------------

-spec start(application:start_type(), term()) -> {ok, pid()}.
start(_Type, _Args) ->
    {ok, Sup} = emqx_schema_validation_sup:start_link(),
    ok = emqx_schema_validation_config:add_handler(),
    ok = emqx_schema_validation:register_hooks(),
    ok = emqx_schema_validation_config:load(),
    {ok, Sup}.

-spec stop(term()) -> ok.
stop(_State) ->
    ok = emqx_schema_validation_config:unload(),
    ok = emqx_schema_validation:unregister_hooks(),
    ok = emqx_schema_validation_config:remove_handler(),
    ok.
