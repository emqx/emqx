%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_registry_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_mq_internal.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, emqx_mq_test_utils:cth_config(emqx)},
            {emqx_mq, emqx_mq_test_utils:cth_config(emqx_mq)}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_crud(_Config) ->
    {ok, _} = create_mq(<<"mq-1">>, <<"a/b/c">>),
    {ok, _} = create_mq(<<"mq-2">>, <<"a/b/#">>),
    {ok, _} = create_mq(<<"mq-3">>, <<"a/#">>),
    {ok, _} = create_mq(<<"mq-4">>, <<"a/+/d">>),
    ?assertMatch(
        {ok, #{name := <<"mq-1">>}},
        emqx_mq_registry:find(<<"mq-1">>)
    ),
    ?assertEqual(
        not_found,
        emqx_mq_registry:find(<<"nonexistent-mq">>)
    ),
    ?assertMatch(
        [
            #{topic_filter := <<"a/b/c">>},
            #{topic_filter := <<"a/b/#">>},
            #{topic_filter := <<"a/#">>}
        ],
        emqx_mq_registry:match(<<"a/b/c">>)
    ),
    ?assertMatch(
        {ok, #{name := <<"mq-4">>}},
        emqx_mq_registry:find(<<"mq-4">>)
    ),
    ?assertMatch(
        [
            #{topic_filter := <<"a/+/d">>},
            #{topic_filter := <<"a/#">>}
        ],
        emqx_mq_registry:match(<<"a/x/d">>)
    ),
    ok = emqx_mq_registry:delete(<<"mq-3">>),
    ?assertMatch(
        [
            #{topic_filter := <<"a/+/d">>}
        ],
        emqx_mq_registry:match(<<"a/x/d">>)
    ),
    ok = emqx_mq_registry:delete_all(),
    ?assertMatch(
        [],
        emqx_mq_registry:match(<<"a/x/d">>)
    ).

t_validate_name(_Config) ->
    ?assertMatch(
        {ok, _},
        emqx_mq_registry:create(
            emqx_mq_test_utils:fill_mq_defaults(#{name => <<"mq-1">>, topic_filter => <<"a/b/c">>})
        )
    ),
    ?assertEqual(
        {error, invalid_name},
        emqx_mq_registry:create(
            emqx_mq_test_utils:fill_mq_defaults(#{name => <<"mq-1/2">>, topic_filter => <<"a/b/c">>})
        )
    ).

%% Verify that index is cleaned up if queue state creation fails with error.
t_create_error(_Config) ->
    ok = meck:new(emqx_ds, [passthrough, no_history]),
    ok = meck:expect(emqx_ds, trans, fun(_, _) -> {error, recoverable, leader_unavailable} end),
    ?assertMatch(
        {error, _},
        emqx_mq_registry:create(emqx_mq_test_utils:fill_mq_defaults(#{topic_filter => <<"x/y/z">>}))
    ),
    ?assertMatch(
        [],
        emqx_mq_registry:match(<<"x/y/z">>)
    ),
    ok = meck:unload(emqx_ds).

%% Verify that index is cleaned up if queue state creation fails with exception.
t_create_exception(_Config) ->
    ok = meck:new(emqx_ds, [passthrough, no_history]),
    ok = meck:expect(emqx_ds, trans, fun(_, _) -> meck:exception(error, oops) end),
    ?assertMatch(
        {error, _},
        emqx_mq_registry:create(emqx_mq_test_utils:fill_mq_defaults(#{topic_filter => <<"x/y/z">>}))
    ),
    ?assertMatch(
        [],
        emqx_mq_registry:match(<<"x/y/z">>)
    ),
    ok = meck:unload(emqx_ds).

%% Verify that we are able to operate with pre-6.1.1 MQs
t_pre_611(_Config) ->
    MQ0 = emqx_mq_test_utils:fill_mq_defaults(#{topic_filter => <<"a/b/c">>}),
    ok = emqx_mq_registry:create_pre_611_queue(MQ0),
    {ok, #{topic_filter := <<"a/b/c">>} = MQ} = emqx_mq_registry:find(<<"/a/b/c">>),
    ?assertEqual(<<"/a/b/c">>, emqx_mq_prop:name(MQ)),
    ?assertEqual(ok, emqx_mq_registry:delete(<<"/a/b/c">>)),
    ?assertEqual(not_found, emqx_mq_registry:find(<<"/a/b/c">>)),
    ?assertEqual([], emqx_mq_registry:match(<<"a/b/c">>)).

-doc """
Verify that `delete_legacy/0` deletes a pre-6.1.1 queue with its state and messages,
keeps a named queue, and returns 0 on a second call.
""".
t_delete_legacy(_Config) ->
    ok = emqx_mq_registry:create_pre_611_queue(
        emqx_mq_test_utils:fill_mq_defaults(#{topic_filter => <<"a/b/c">>})
    ),
    {ok, #{id := LegacyId} = LegacyMQ} = emqx_mq_registry:find(<<"/a/b/c">>),
    {ok, NamedMQ} = create_mq(<<"mq-1">>, <<"a/b/#">>),
    ok = insert_message(LegacyMQ),
    ok = insert_message(NamedMQ),
    ?retry(100, 50, ?assertMatch([_], emqx_mq_message_db:dirty_read_all(LegacyMQ))),
    ?assertEqual(1, emqx_mq_registry:delete_legacy()),
    ?assertEqual(not_found, emqx_mq_registry:find(<<"/a/b/c">>)),
    ?assertEqual(not_found, emqx_mq_state_storage:find_mq(LegacyId)),
    ?assertNot(lists:member(LegacyId, emqx_mq_state_storage:mq_ids())),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(LegacyMQ)),
    ?assertMatch({ok, #{name := <<"mq-1">>}}, emqx_mq_registry:find(<<"mq-1">>)),
    ?assertMatch([_], emqx_mq_message_db:dirty_read_all(NamedMQ)),
    ?assertMatch([#{topic_filter := <<"a/b/#">>}], emqx_mq_registry:match(<<"a/b/c">>)),
    ?assertEqual(0, emqx_mq_registry:delete_legacy()).

-doc """
Verify that `delete_legacy/0` returns 0 and keeps all queues when no pre-6.1.1 queue exists.
""".
t_delete_legacy_none(_Config) ->
    {ok, _} = create_mq(<<"mq-1">>, <<"a/b/c">>),
    {ok, _} = create_mq(<<"mq-2">>, <<"a/#">>),
    ?assertEqual(0, emqx_mq_registry:delete_legacy()),
    ?assertMatch({ok, _}, emqx_mq_registry:find(<<"mq-1">>)),
    ?assertMatch({ok, _}, emqx_mq_registry:find(<<"mq-2">>)).

-doc "A marked MQ is skipped by match and list, is listed by list_all, and cannot be updated".
t_mark_deleting(_Config) ->
    {ok, MQ0} = create_mq(<<"mq-1">>, <<"a/#">>),
    {ok, _} = create_mq(<<"mq-2">>, <<"a/b">>),
    ok = emqx_mq_registry:mark_deleting(MQ0),
    ?assert(emqx_mq_registry:is_deleting(MQ0)),
    ?assertMatch([#{topic_filter := <<"a/b">>}], emqx_mq_registry:match(<<"a/b">>)),
    ?assertMatch([#{name := <<"mq-2">>}], emqx_utils_stream:consume(emqx_mq_registry:list())),
    ?assertMatch({ok, [#{name := <<"mq-2">>}], undefined}, emqx_mq_registry:list(undefined, 10)),
    {ok, All0, undefined} = emqx_mq_registry:list_all(undefined, 10),
    ?assertEqual(2, length(All0)),
    ?assertMatch([#{name := <<"mq-1">>}], [MQ || #{deleted_at := _} = MQ <- All0]),
    ?assertMatch({ok, #{name := <<"mq-1">>}}, emqx_mq_registry:find(<<"mq-1">>)),
    ?assertEqual({error, being_deleted}, emqx_mq_registry:update(<<"mq-1">>, MQ0)),
    %% A pre-6.1.1 MQ is marked through its legacy key
    ok = emqx_mq_registry:create_pre_611_queue(
        emqx_mq_test_utils:fill_mq_defaults(#{topic_filter => <<"c/d">>})
    ),
    {ok, MQ1} = emqx_mq_registry:find(<<"/c/d">>),
    ok = emqx_mq_registry:mark_deleting(MQ1),
    ?assert(emqx_mq_registry:is_deleting(MQ1)),
    ?assertEqual([], emqx_mq_registry:match(<<"c/d">>)).

-doc "Messages published to a topic of a marked MQ are not stored in it".
t_mark_deleting_publish(_Config) ->
    {ok, MQ0} = create_mq(<<"mq-1">>, <<"a/#">>),
    {ok, MQ1} = create_mq(<<"mq-2">>, <<"a/#">>),
    ok = emqx_mq_registry:mark_deleting(MQ0),
    _ = emqx_broker:publish(emqx_message:make(<<"c">>, 1, <<"a/b">>, <<"data">>)),
    ?retry(100, 20, ?assertNotEqual([], emqx_mq_message_db:dirty_read_all(MQ1))),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ0)).

-doc "Marking an MQ stops its consumer, and the consumer of a marked MQ does not start".
t_mark_deleting_consumer(_Config) ->
    {ok, #{id := Id} = MQ} = create_mq(<<"mq-1">>, <<"a/#">>),
    {ok, _} = emqx_mq_sup:start_consumer(Id, [MQ]),
    ok = emqx_mq_registry:mark_deleting(MQ),
    ?retry(100, 20, ?assertEqual(not_found, emqx_mq_consumer:find(Id))),
    ?assertEqual({error, mq_being_deleted}, emqx_mq_sup:start_consumer(Id, [MQ])).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

create_mq(Name, TopicFilter) ->
    emqx_mq_test_utils:create_mq(#{name => Name, topic_filter => TopicFilter}).

insert_message(MQ) ->
    Message = emqx_message:make(<<"c1">>, 1, <<"a/b/c">>, <<"payload">>),
    emqx_mq_message_db:insert(MQ, Message).
