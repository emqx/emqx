%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation_app).

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
    {ok, Sup} = emqx_message_transformation_sup:start_link(),
    ok = emqx_variform:inject_allowed_module(emqx_message_transformation_bif),
    ok = emqx_message_transformation_config:add_handler(),
    ok = emqx_message_transformation_config:load(),
    ok = emqx_message_transformation:register_hooks(),
    {ok, Sup}.

-spec stop(term()) -> ok.
stop(_State) ->
    ok = emqx_message_transformation:unregister_hooks(),
    ok = emqx_message_transformation_config:unload(),
    ok = emqx_message_transformation_config:remove_handler(),
    ok = emqx_variform:erase_allowed_module(emqx_message_transformation_bif),
    ok.
