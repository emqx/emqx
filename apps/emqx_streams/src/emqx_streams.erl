%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams).

-include_lib("emqx/include/emqx_hooks.hrl").

-export([register_hooks/0, unregister_hooks/0]).

%%

-spec register_hooks() -> ok.
register_hooks() ->
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok.
