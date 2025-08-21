%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_dispatch_bif_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_mq, emqx_mq_test_utils:config()}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Check various bifs are available in dispatch variform expressions
t_topic(_Config) ->
    Compiled = emqx_mq_utils:dispatch_variform_compile("m.topic(message)"),
    Message = emqx_message:make(<<"clientid1">>, <<"topic/1">>, <<"payload1">>),
    ?assertEqual(
        {ok, <<"topic/1">>},
        emqx_variform:render(Compiled, #{message => Message}, #{eval_as_string => false})
    ).

t_clientid(_Config) ->
    Compiled = emqx_mq_utils:dispatch_variform_compile("m.clientid(message)"),
    Message = emqx_message:make(<<"clientid1">>, <<"topic/1">>, <<"payload1">>),
    ?assertEqual(
        {ok, <<"clientid1">>},
        emqx_variform:render(Compiled, #{message => Message}, #{eval_as_string => false})
    ).
