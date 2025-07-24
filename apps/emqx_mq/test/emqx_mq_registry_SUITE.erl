%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_registry_SUITE).

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
            emqx_mq
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_crud(_Config) ->
    ok = emqx_mq_registry:create(<<"a/b/c">>, true),
    ok = emqx_mq_registry:create(<<"a/b/#">>, true),
    ok = emqx_mq_registry:create(<<"a/#">>, true),
    ok = emqx_mq_registry:create(<<"a/+/d">>, true),
    ?assertMatch(
        [
            #{topic_filter := <<"a/b/c">>},
            #{topic_filter := <<"a/b/#">>},
            #{topic_filter := <<"a/#">>}
        ],
        emqx_mq_registry:find(<<"a/b/c">>)
    ),
    ?assertMatch(
        [
            #{topic_filter := <<"a/+/d">>},
            #{topic_filter := <<"a/#">>}
        ],
        emqx_mq_registry:find(<<"a/x/d">>)
    ),
    ok = emqx_mq_registry:delete(<<"a/#">>),
    ?assertMatch(
        [
            #{topic_filter := <<"a/+/d">>}
        ],
        emqx_mq_registry:find(<<"a/x/d">>)
    ),
    ok = emqx_mq_registry:delete_all(),
    ?assertMatch(
        [],
        emqx_mq_registry:find(<<"a/x/d">>)
    ).
