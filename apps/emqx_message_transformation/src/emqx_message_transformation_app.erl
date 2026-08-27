%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation_app).

-behaviour(application).

%% `application' API
-export([start/2, prep_stop/1, stop/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `application' API
%%------------------------------------------------------------------------------

-spec start(application:start_type(), term()) -> {ok, pid()}.
start(_Type, _Args) ->
    {ok, Sup} = emqx_message_transformation_sup:start_link(),
    ok = emqx_variform:inject_allowed_module(emqx_message_transformation_bif),
    ok = emqx_message_transformation_config:add_handler(),
    ok = emqx_message_transformation_config:load(),
    ok = emqx_message_transformation:register_hooks(),
    {ok, Sup}.

-spec prep_stop(term()) -> term().
prep_stop(State) ->
    %% Runs before the supervision tree is terminated.  `stop/1' runs after it, when
    %% the registry and its tables are already gone: the hooks must be unregistered
    %% and the transformations unloaded while both are still alive.
    ok = emqx_message_transformation:unregister_hooks(),
    ok = emqx_message_transformation_config:unload(),
    State.

-spec stop(term()) -> ok.
stop(_State) ->
    ok = emqx_message_transformation_config:remove_handler(),
    ok = emqx_variform:erase_allowed_module(emqx_message_transformation_bif),
    ok.
