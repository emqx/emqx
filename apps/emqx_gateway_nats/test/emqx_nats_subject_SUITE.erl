%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_subject_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [t_matches, t_subset, t_intersects, t_invalid_subject].

t_matches(_) ->
    lists:foreach(
        fun({Subject, Filter, Expected}) ->
            ?assertEqual(Expected, emqx_nats_subject:matches(tokens(Subject), tokens(Filter)))
        end,
        [
            {<<"$private.secret">>, <<"*.secret">>, true},
            {<<"$private.secret">>, <<">">>, true},
            {<<"$private">>, <<"$private.>">>, false},
            {<<"foo.bar">>, <<"foo.*">>, true},
            {<<"foo.bar.baz">>, <<"foo.*">>, false},
            {<<"foo.bar.baz">>, <<"foo.>">>, true},
            {<<"foo">>, <<"foo.>">>, false},
            {<<"foo.bar">>, <<"foo.*.>">>, false},
            {<<"foo.bar.baz">>, <<"foo.*.>">>, true},
            {<<"foo.bar">>, <<"FOO.*">>, false},
            {<<"中文.消息"/utf8>>, <<"中文.*"/utf8>>, true}
        ]
    ).

t_subset(_) ->
    lists:foreach(
        fun({Subject, Filter, Expected}) ->
            ?assertEqual(Expected, emqx_nats_subject:is_subset(tokens(Subject), tokens(Filter)))
        end,
        [
            {<<"$private.>">>, <<">">>, true},
            {<<"$private.secret">>, <<"*.secret">>, true},
            {<<"$private.*">>, <<"*.secret">>, false},
            {<<"foo.*">>, <<"foo.>">>, true},
            {<<"foo.>">>, <<"foo.*">>, false},
            {<<"foo.>">>, <<"foo.*.>">>, false},
            {<<"foo.*.>">>, <<"foo.>">>, true},
            {<<"foo.*.>">>, <<"foo.*.>">>, true},
            {<<"foo.*.bar">>, <<"foo.*.*">>, true},
            {<<"foo.*.bar">>, <<"foo.x.*">>, false},
            {<<"foo">>, <<"foo.>">>, false},
            {<<"foo.bar">>, <<"foo.bar.baz">>, false},
            {<<">">>, <<"*">>, false},
            {<<">">>, <<"*.>">>, false}
        ]
    ).

t_intersects(_) ->
    lists:foreach(
        fun({Subject, Filter, Expected}) ->
            A = tokens(Subject),
            B = tokens(Filter),
            ?assertEqual(Expected, emqx_nats_subject:intersects(A, B)),
            ?assertEqual(Expected, emqx_nats_subject:intersects(B, A))
        end,
        [
            {<<"$private.>">>, <<"*.secret">>, true},
            {<<"$private.secret">>, <<"*.secret">>, true},
            {<<"$private.public">>, <<"*.secret">>, false},
            {<<">">>, <<"$private.>">>, true},
            {<<"foo.*.bar">>, <<"foo.x.*">>, true},
            {<<"foo.*.bar">>, <<"foo.x.baz">>, false},
            {<<"foo.*">>, <<"foo.*.>">>, false},
            {<<"foo.>">>, <<"foo.*.>">>, true},
            {<<"foo">>, <<"foo.>">>, false},
            {<<"foo.>">>, <<"bar.>">>, false},
            {<<"*">>, <<">">>, true},
            {<<"foo.bar">>, <<"foo.bar.baz">>, false}
        ]
    ).

t_invalid_subject(_) ->
    lists:foreach(
        fun(Subject) ->
            ?assertException(error, {invalid_subject, _}, tokens(Subject))
        end,
        [<<>>, <<"foo..bar">>, <<"foo.>.bar">>, <<"foo/#">>, <<"foo.+">>]
    ).

tokens(Subject) ->
    emqx_nats_subject:tokens(Subject).
