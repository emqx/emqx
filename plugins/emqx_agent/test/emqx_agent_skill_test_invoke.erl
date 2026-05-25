%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_test_invoke).

-export([handle_invoke/2]).

handle_invoke(_Context, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    case maps:get(<<"action">>, Args, <<"echo">>) of
        <<"sleep">> ->
            timer:sleep(maps:get(<<"duration">>, Args, 0)),
            {ok, #{<<"done">> => true}};
        <<"crash">> ->
            error(crash_on_purpose);
        _ ->
            {ok, Args}
    end.
