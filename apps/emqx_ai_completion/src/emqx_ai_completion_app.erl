%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ai_completion_app).

-behaviour(application).

%% `application' API
-export([start/2, stop/1]).

%%------------------------------------------------------------------------------
%% `application' API
%%------------------------------------------------------------------------------

-spec start(application:start_type(), term()) -> {ok, pid()}.
start(_Type, _Args) ->
    {ok, Sup} = emqx_ai_completion_sup:start_link(),
    ok = emqx_ai_completion_registry:create_tab(),
    ok = emqx_ai_completion_registry:create(<<"openai">>, emqx_ai_completion_openai, #{model => <<"gpt-4o">>}),
    ok = emqx_rule_engine:register_external_functions(emqx_ai_completion),
    {ok, Sup}.

-spec stop(term()) -> ok.
stop(_State) ->
    ok.
