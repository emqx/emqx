%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iceberg_app).

-behaviour(application).

%% `application' API
-export([start/2, stop/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `application' API
%%------------------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    emqx_bridge_iceberg_impl:load_and_memoize_schema_files(),
    emqx_bridge_iceberg_sup:start_link().

stop(_State) ->
    emqx_bridge_iceberg_impl:forget_schema_files(),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
