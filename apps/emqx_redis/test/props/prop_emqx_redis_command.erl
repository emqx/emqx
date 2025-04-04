%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(prop_emqx_redis_command).

-include_lib("proper/include/proper.hrl").

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_split() ->
    ?FORALL(
        Cmd,
        binary(),
        %% Should terminate and not crash
        is_tuple(emqx_redis_command:split(Cmd))
    ).
