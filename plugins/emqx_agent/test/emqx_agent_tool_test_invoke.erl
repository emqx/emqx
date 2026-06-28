%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_test_invoke).

-define(TOOL_TYPE, <<"agent__query_tools">>).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/2]).

init() ->
    emqx_agent_tool_registry:register_type(?TOOL_TYPE, ?MODULE).

deinit() ->
    emqx_agent_tool_registry:unregister_type(?TOOL_TYPE).

create(#{<<"id">> := ToolId} = Context) ->
    {ok, #{
        tool_id => ToolId,
        type => ?TOOL_TYPE,
        module => ?MODULE,
        display_name => <<"Test Invoke Tool">>,
        description => maps:get(<<"desc">>, Context, <<"Test invocation tool">>),
        context => Context,
        input_schema => #{}
    }}.

destroy(_Tool) ->
    ok.

to_map(#{tool_id := Id, description := Desc}) ->
    #{
        <<"tool_id">> => Id,
        <<"type">> => ?TOOL_TYPE,
        <<"description">> => Desc,
        <<"input_schema">> => #{}
    }.

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
