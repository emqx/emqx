%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_fake_api).

-behaviour(emqx_plugin_api).

-export([handle/4]).

handle(_Method, _PathRemainder, _Request, _Context) ->
    {ok, 200, #{}, #{ok => true}}.
