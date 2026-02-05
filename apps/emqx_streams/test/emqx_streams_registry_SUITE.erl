%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_registry_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_streams_internal.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, emqx_streams_test_utils:cth_config(emqx)},
            {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
            {emqx_streams, emqx_streams_test_utils:cth_config(emqx_streams)}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_streams_test_utils:cleanup_streams(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_streams_test_utils:cleanup_streams().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_crud(_Config) ->
    {ok, _} = create_stream(<<"stream-1">>, <<"a/b/c">>),
    {ok, _} = create_stream(<<"stream-2">>, <<"a/b/#">>),
    {ok, _} = create_stream(<<"stream-3">>, <<"a/#">>),
    {ok, _} = create_stream(<<"stream-4">>, <<"a/+/d">>),
    ?assertMatch(
        {ok, #{name := <<"stream-1">>}},
        emqx_streams_registry:find(<<"stream-1">>)
    ),
    ?assertEqual(
        not_found,
        emqx_streams_registry:find(<<"nonexistent-stream">>)
    ),
    ?assertMatch(
        [
            #{topic_filter := <<"a/b/c">>},
            #{topic_filter := <<"a/b/#">>},
            #{topic_filter := <<"a/#">>}
        ],
        emqx_streams_registry:match(<<"a/b/c">>)
    ),
    ?assertMatch(
        {ok, #{name := <<"stream-4">>}},
        emqx_streams_registry:find(<<"stream-4">>)
    ),
    ?assertMatch(
        [
            #{topic_filter := <<"a/+/d">>},
            #{topic_filter := <<"a/#">>}
        ],
        emqx_streams_registry:match(<<"a/x/d">>)
    ),
    ok = emqx_streams_registry:delete(<<"stream-3">>),
    ?assertMatch(
        [
            #{topic_filter := <<"a/+/d">>}
        ],
        emqx_streams_registry:match(<<"a/x/d">>)
    ),
    ok = emqx_streams_registry:delete_all(),
    ?assertMatch(
        [],
        emqx_streams_registry:match(<<"a/x/d">>)
    ).

%% Verify that we are able to operate with pre-6.1.1 streams
t_pre_611(_Config) ->
    Stream0 = emqx_streams_test_utils:fill_stream_defaults(#{topic_filter => <<"a/b/c">>}),
    {ok, _} = emqx_streams_registry:create_pre_611_stream(Stream0),
    {ok, #{topic_filter := <<"a/b/c">>} = Stream} = emqx_streams_registry:find(<<"/a/b/c">>),
    ?assertEqual(<<"/a/b/c">>, emqx_streams_prop:name(Stream)),
    ?assertEqual(ok, emqx_streams_registry:delete(<<"/a/b/c">>)),
    ?assertEqual(not_found, emqx_streams_registry:find(<<"/a/b/c">>)),
    ?assertEqual([], emqx_streams_registry:match(<<"a/b/c">>)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

create_stream(Name, TopicFilter) ->
    emqx_streams_test_utils:create_stream(#{name => Name, topic_filter => TopicFilter}).
