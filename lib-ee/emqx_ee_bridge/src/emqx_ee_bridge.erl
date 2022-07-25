%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge).

-export([
    schema_modules/0,
    info_example_basic/2
]).

schema_modules() ->
    [emqx_ee_bridge_hstream].

info_example_basic(_Type, _Direction) ->
    #{}.
