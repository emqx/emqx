%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auto_subscribe_handler).

-export([init/1]).

-spec init(hocon:config()) -> {Module :: atom(), Config :: term()}.
init(Config) ->
    do_init(Config).

do_init(Config = #{topics := _Topics}) ->
    Options = emqx_auto_subscribe_internal:init(Config),
    {emqx_auto_subscribe_internal, Options};
do_init(_Config) ->
    erlang:error(not_supported).
