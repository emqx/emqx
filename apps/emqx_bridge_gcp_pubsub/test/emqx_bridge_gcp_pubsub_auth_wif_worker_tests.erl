%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_auth_wif_worker_tests).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

setup() ->
    ok = application:ensure_started(gproc),
    {ok, TimeAgent} = emqx_utils_agent:start_link(#{times => [0]}),
    {ok, StepAgent} = emqx_utils_agent:start_link(#{replies => [], requests => []}),
    Mod = emqx_bridge_gcp_pubsub_auth_wif_worker,
    meck:new(Mod, [passthrough]),
    meck:expect(Mod, now_ms, fun() -> ask_time(TimeAgent) end),
    meck:expect(
        Mod,
        request,
        fun(Method, URL, Headers, Body, ReqOpts) ->
            Args = #{
                method => Method,
                url => URL,
                headers => Headers,
                body => Body,
                req_opts => ReqOpts
            },
            ask_reply(StepAgent, Args)
        end
    ),
    #{time_agent => TimeAgent, step_agent => StepAgent}.

cleanup(Ctx) ->
    #{
        time_agent := TimeAgent,
        step_agent := StepAgent
    } = Ctx,
    emqx_utils_agent:stop(TimeAgent),
    emqx_utils_agent:stop(StepAgent),
    meck:unload(),
    ok = application:stop(gproc),
    ok.

insert_fn() ->
    insert_fn(_Opts = #{}).

insert_fn(Opts) ->
    AliasOpts = maps:get(alias_opts, Opts, [reply]),
    Alias = alias(AliasOpts),
    RecvFn = fun(Timeout) ->
        receive
            {Alias, final_token, FinalToken} ->
                {ok, FinalToken}
        after Timeout ->
            timeout
        end
    end,
    {{fun(FinalToken) -> Alias ! {Alias, final_token, FinalToken} end, []}, RecvFn}.

ask_reply(StepAgent, Args) ->
    emqx_utils_agent:get_and_update(StepAgent, fun(St0) ->
        case St0 of
            #{replies := [Reply | Rest], requests := Reqs} ->
                St = St0#{replies := Rest, requests := Reqs ++ [Args]},
                {Reply, St}
        end
    end).

set_replies(StepAgent, Replies) ->
    emqx_utils_agent:update(StepAgent, fun(St0) -> St0#{replies := Replies} end).

get_requests(StepAgent) ->
    #{requests := Reqs} = emqx_utils_agent:get(StepAgent),
    Reqs.

simple_token_reply(Token) ->
    {ok, 200, [{<<"Content-Type">>, <<"application/json">>}],
        emqx_utils_json:encode(#{<<"access_token">> => Token})}.

simple_reply(Code, Body0) ->
    Body =
        case Body0 of
            {raw, B} -> B;
            _ -> emqx_utils_json:encode(Body0)
        end,
    {ok, Code, [{<<"Content-Type">>, <<"application/json">>}], Body}.

ask_time(TimeAgent) ->
    emqx_utils_agent:get_and_update(TimeAgent, fun(St0) ->
        case St0 of
            #{times := [Time]} ->
                {Time, St0};
            #{times := [Time | Times]} ->
                St = St0#{times := Times},
                {Time, St}
        end
    end).

set_times(TimeAgent, Times) ->
    emqx_utils_agent:update(TimeAgent, fun(St0) -> St0#{times := Times} end).

dummy_step(Overrides) ->
    Defaults = #{
        name => please_override,
        method => post,
        lifetime => timer:hours(1),
        url => fun(_StepContext) -> <<"http://auth.server/step1">> end,
        body => fun(_StepContext) -> <<"">> end,
        headers => fun(_StepContext) -> [{<<"Content-Type">>, <<"application/json">>}] end,
        extract_result => fun(#{body := RespBody}) ->
            case emqx_utils_json:safe_decode(RespBody) of
                {ok, #{<<"access_token">> := Token}} ->
                    {ok, #{token => Token}};
                Error ->
                    {error, {bad_resp, Error}}
            end
        end
    },
    maps:merge(Defaults, Overrides).

%% Chain of 3 steps that depend on previous steps
sample_3_steps() ->
    Step1 = dummy_step(#{
        name => step1,
        url => fun(_StepContext) -> <<"http://auth.server/step1">> end
    }),
    Step2 = dummy_step(#{
        name => step2,
        url => fun(_StepContext) -> <<"http://auth.server/step2">> end,
        body =>
            fun
                (#{{step, step1} := _}) -> <<"2">>;
                (StepContext) -> error({missing_steps, StepContext})
            end
    }),
    Step3 = dummy_step(#{
        name => step3,
        url => fun(_StepContext) -> <<"http://auth.server/step3">> end,
        body =>
            fun
                (#{{step, step1} := _, {step, step2} := _}) -> <<"3">>;
                (StepContext) -> error({missing_steps, StepContext})
            end
    }),
    [Step1, Step2, Step3].

eval_steps(Opts) ->
    State0 = emqx_bridge_gcp_pubsub_auth_wif_worker:init_state(Opts),
    do_eval_steps(State0).

do_eval_steps(State0) ->
    case emqx_bridge_gcp_pubsub_auth_wif_worker:do_handle_advance(State0) of
        {done, State} ->
            {done, State};
        {retry, State} ->
            do_eval_steps(State);
        {continue, State} ->
            do_eval_steps(State)
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Simple happy path smoke test simulating a 3-step WIF token exchange.
""".
happy_path_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Ctx) -> ?_test(test_happy_path(Ctx)) end}.

test_happy_path(Ctx) ->
    #{step_agent := StepAgent} = Ctx,
    {InsertFn, RecvFn} = insert_fn(),
    Replies = lists:map(
        fun(N) -> simple_token_reply(integer_to_binary(N)) end,
        lists:seq(1, 3)
    ),
    set_replies(StepAgent, Replies),
    Steps = sample_3_steps(),
    InitOpts = #{
        resource_id => ?FUNCTION_NAME,
        insert_fn => InsertFn,
        steps => Steps
    },
    ?assertMatch({done, _}, eval_steps(InitOpts), #{reqs => get_requests(StepAgent)}),
    ?assertMatch({ok, <<"3">>}, RecvFn(100), #{reqs => get_requests(StepAgent)}),
    ?assertMatch(
        [
            #{url := <<"http://auth.server/step1">>},
            #{url := <<"http://auth.server/step2">>},
            #{url := <<"http://auth.server/step3">>}
        ],
        get_requests(StepAgent)
    ),
    ok.

-doc """
Verifies that we retry a step when it fails.
""".
retry_step_test_() ->
    Mk = fun(N) ->
        {setup, fun setup/0, fun cleanup/1, fun(Ctx) ->
            ?_test(test_retry_step(N, Ctx))
        end}
    end,
    [
        {"first request fails", Mk(1)},
        {"middle request fails", Mk(2)},
        {"final request fails", Mk(3)}
    ].

test_retry_step(StepToRetry, Ctx) ->
    #{step_agent := StepAgent} = Ctx,
    {InsertFn, RecvFn} = insert_fn(),
    Replies0 = lists:map(
        fun(N) -> simple_token_reply(integer_to_binary(N)) end,
        lists:seq(1, 3)
    ),
    {Replies1, Replies2} = lists:split(StepToRetry - 1, Replies0),
    Replies =
        Replies1 ++
            [
                simple_reply(429, <<"calm down">>),
                {error, timeout},
                simple_reply(
                    200,
                    emqx_utils_json:encode(#{<<"success, object">> => <<"but wrong shape">>})
                ),
                simple_reply(301, {raw, <<"<?xml><note>not a json</note>">>}),
                simple_reply(200, {raw, <<"<?xml><note>not a json</note>">>}),
                simple_reply(429, <<"calm down!">>)
            ] ++
            Replies2,
    set_replies(StepAgent, Replies),
    Steps = sample_3_steps(),
    InitOpts = #{
        resource_id => ?FUNCTION_NAME,
        insert_fn => InsertFn,
        steps => Steps
    },
    ?assertMatch({done, _}, eval_steps(InitOpts), #{reqs => get_requests(StepAgent)}),
    ?assertMatch({ok, <<"3">>}, RecvFn(100), #{reqs => get_requests(StepAgent)}),
    RetriedURL = emqx_bridge_v2_testlib:fmt(<<"http://auth.server/step${n}">>, #{n => StepToRetry}),
    ?assertMatch(
        #{
            RetriedURL := [_, _, _, _, _, _, _]
        },
        maps:groups_from_list(fun(#{url := U}) -> U end, get_requests(StepAgent))
    ),
    ok.

-doc """
Verifies that we check whether the token from the last step has expired and, if so,
restart the flow.
""".
restart_when_expired_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Ctx) ->
        ?_test(test_restart_when_expired(Ctx))
    end}.

test_restart_when_expired(Ctx) ->
    #{step_agent := StepAgent, time_agent := TimeAgent} = Ctx,
    {InsertFn, RecvFn} = insert_fn(),
    Replies = [
        simple_token_reply(<<"1">>),
        simple_token_reply(<<"2">>),
        %% Restart
        simple_token_reply(<<"1">>),
        simple_token_reply(<<"2">>),
        simple_token_reply(<<"3">>)
    ],
    set_replies(StepAgent, Replies),
    Steps = sample_3_steps(),
    InitOpts = #{
        resource_id => ?FUNCTION_NAME,
        insert_fn => InsertFn,
        steps => Steps
    },
    %% When we look at our "clock" during the second request, the previous token has
    %% expired.
    Times = lists:flatten([
        %% First token (before and after getting token)
        [0, 0],
        %% Second token (before and after getting token)
        [0, 0],
        %% Before third request: previous token is deemed expired.  Restarts.
        [timer:hours(2)],
        %% Further requests
        0
    ]),
    set_times(TimeAgent, Times),
    ?assertMatch({done, _}, eval_steps(InitOpts), #{reqs => get_requests(StepAgent)}),
    ?assertMatch({ok, <<"3">>}, RecvFn(100), #{reqs => get_requests(StepAgent)}),
    ?assertMatch(
        [
            #{url := <<"http://auth.server/step1">>},
            #{url := <<"http://auth.server/step2">>},
            #{url := <<"http://auth.server/step1">>},
            #{url := <<"http://auth.server/step2">>},
            #{url := <<"http://auth.server/step3">>}
        ],
        get_requests(StepAgent)
    ),
    ok.

-doc """
"Integration" test verifying that the `gen_server` process correctly refreshes the token
chain periodically.
""".
refresh_it_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Ctx) ->
        ?_test(test_refresh_it(Ctx))
    end}.

test_refresh_it(Ctx) ->
    #{step_agent := StepAgent} = Ctx,
    {InsertFn, RecvFn} = insert_fn(#{alias_opts => []}),
    Replies =
        lists:map(
            fun(N) -> simple_token_reply(integer_to_binary(N)) end,
            lists:seq(1, 9)
        ),
    set_replies(StepAgent, Replies),
    Steps0 = sample_3_steps(),
    Steps = lists:map(fun(Step) -> Step#{lifetime := 300} end, Steps0),
    InitOpts = #{
        resource_id => ?FUNCTION_NAME,
        insert_fn => InsertFn,
        steps => Steps
    },
    {ok, Pid} = emqx_bridge_gcp_pubsub_auth_wif_worker:start_link(?FUNCTION_NAME, InitOpts),
    %% First time chain is evaluated.
    ?assertMatch({ok, <<"3">>}, RecvFn(100), #{reqs => get_requests(StepAgent)}),
    %% First refresh
    ?assertMatch({ok, <<"6">>}, RecvFn(1_000), #{reqs => get_requests(StepAgent)}),
    %% Second refresh
    ?assertMatch({ok, <<"9">>}, RecvFn(1_000), #{reqs => get_requests(StepAgent)}),
    ok = gen_server:stop(Pid),
    ok.

-doc """
"Integration" test verifying that the `gen_server` process correctly retries (and later
refreshes) the token chain periodically.
""".
retry_it_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Ctx) ->
        ?_test(test_retry_it(Ctx))
    end}.

test_retry_it(Ctx) ->
    #{step_agent := StepAgent} = Ctx,
    {InsertFn, RecvFn} = insert_fn(#{alias_opts => []}),
    Replies = [
        simple_token_reply(<<"1">>),
        simple_token_reply(<<"2">>),
        %% Induce a retry
        simple_reply(200, emqx_utils_json:encode(#{<<"request">> => <<"successfully failed">>})),
        simple_token_reply(<<"3">>),
        %% Test that the refresh works (the timers don't interfere with one another)
        simple_token_reply(<<"4">>),
        simple_token_reply(<<"5">>),
        simple_token_reply(<<"6">>)
    ],
    set_replies(StepAgent, Replies),
    Steps0 = sample_3_steps(),
    Steps = lists:map(
        fun
            (#{name := step3} = Step) -> Step#{lifetime := timer:seconds(2)};
            (Step) -> Step
        end,
        Steps0
    ),
    InitOpts = #{
        resource_id => ?FUNCTION_NAME,
        insert_fn => InsertFn,
        steps => Steps
    },
    {ok, Pid} = emqx_bridge_gcp_pubsub_auth_wif_worker:start_link(?FUNCTION_NAME, InitOpts),
    %% First time chain is evaluated (with retries)
    ?assertMatch({ok, <<"3">>}, RecvFn(1_500), #{reqs => get_requests(StepAgent)}),
    %% Refresh
    ?assertMatch({ok, <<"6">>}, RecvFn(3_000), #{reqs => get_requests(StepAgent)}),
    ok = gen_server:stop(Pid),
    ok.

-doc """
Mainly covers the behavior of `ensure_token` when it's called before and after first token
is fetched.
""".
ensure_token_it_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Ctx) ->
        [
            {"before", ?_test(test_ensure_token_before(Ctx))},
            {"after", ?_test(test_ensure_token_after(Ctx))}
        ]
    end}.

test_ensure_token_before(Ctx) ->
    #{step_agent := StepAgent} = Ctx,
    {InsertFn, RecvFn} = insert_fn(),
    Replies = [
        simple_token_reply(<<"1">>),
        simple_token_reply(<<"2">>),
        %% Induce a retry to allow for the call to be processed
        {error, timeout},
        simple_token_reply(<<"3">>)
    ],
    set_replies(StepAgent, Replies),
    Steps = sample_3_steps(),
    InitOpts = #{
        resource_id => ?FUNCTION_NAME,
        insert_fn => InsertFn,
        steps => Steps
    },
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := "gcp_wif_worker_will_retry"},
                #{?snk_kind := "go"}
            ),
            ?force_ordering(
                #{?snk_kind := "gcp_wif_worker_ensure_token_before_first_token"},
                #{?snk_kind := "gcp_wif_worker_info_advance_enter", step := step3}
            ),
            {ok, Pid} = emqx_bridge_gcp_pubsub_auth_wif_worker:start_link(?FUNCTION_NAME, InitOpts),
            {_, Ref} = spawn_opt(
                fun() ->
                    ?assertMatch(
                        ok,
                        emqx_bridge_gcp_pubsub_auth_wif_worker:ensure_token(?FUNCTION_NAME, 2_000)
                    )
                end,
                [link, monitor]
            ),
            ?tp("go", #{}),
            ?assertMatch({ok, <<"3">>}, RecvFn(3_000), #{reqs => get_requests(StepAgent)}),
            ?assertReceive({'DOWN', Ref, _, _, _}),
            ok = gen_server:stop(Pid),
            ok
        end,
        []
    ),
    snabbkaffe:stop(),
    ok.

test_ensure_token_after(Ctx) ->
    #{step_agent := StepAgent} = Ctx,
    {InsertFn, RecvFn} = insert_fn(),
    Replies =
        lists:map(
            fun(N) -> simple_token_reply(integer_to_binary(N)) end,
            lists:seq(1, 3)
        ),
    set_replies(StepAgent, Replies),
    Steps = sample_3_steps(),
    InitOpts = #{
        resource_id => ?FUNCTION_NAME,
        insert_fn => InsertFn,
        steps => Steps
    },
    {ok, Pid} = emqx_bridge_gcp_pubsub_auth_wif_worker:start_link(?FUNCTION_NAME, InitOpts),
    ?assertMatch({ok, <<"3">>}, RecvFn(3_000), #{reqs => get_requests(StepAgent)}),
    ?assertMatch(
        ok,
        emqx_bridge_gcp_pubsub_auth_wif_worker:ensure_token(?FUNCTION_NAME, 1_000)
    ),
    ok = gen_server:stop(Pid),
    ok.

-doc """
For coverage, makes some real HTTP calls without mocking.
""".
http_it_test_() ->
    Setup = fun() ->
        {ok, _} = application:ensure_all_started(hackney),
        {ok, _} = application:ensure_all_started(ranch),
        {ok, {Port, Server}} = emqx_utils_http_test_server:start_link(random, "/", false),
        emqx_utils_http_test_server:set_handler(fun(Req, St) ->
            Rep = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                emqx_utils_json:encode(#{<<"access_token">> => <<"token">>}),
                Req
            ),
            {ok, Rep, St}
        end),
        #{port => Port, server => Server}
    end,
    Cleanup = fun(_Ctx) ->
        emqx_utils_http_test_server:stop(),
        application:stop(hackney),
        application:stop(ranch),
        ok
    end,
    Case = fun(Ctx) ->
        #{port := Port} = Ctx,
        Steps = [
            dummy_step(#{
                name => step1,
                url => fun(_StepContext) ->
                    <<"http://127.0.0.1:", (integer_to_binary(Port))/binary>>
                end
            })
        ],
        {InsertFn, RecvFn} = insert_fn(),
        InitOpts = #{
            resource_id => ?FUNCTION_NAME,
            insert_fn => InsertFn,
            steps => Steps
        },
        ?assertMatch({done, _}, eval_steps(InitOpts)),
        ?assertMatch({ok, <<"token">>}, RecvFn(100)),
        ok
    end,
    {setup, Setup, Cleanup, fun(Ctx) -> ?_test(Case(Ctx)) end}.
