%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_cursor_tests).

-include_lib("eunit/include/eunit.hrl").

-define(B64(BIN), base64:encode(BIN, #{mode => urlsafe, padding => false})).

cursor_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun reusable/0,
        fun wrong_domain/0,
        fun expired/0,
        fun limit_drops_oldest/0,
        fun limit_drops_expired_first/0,
        fun bad_id/0
    ]}.

setup() ->
    {ok, Pid} = emqx_mgmt_cursor:start_link(),
    unlink(Pid),
    Pid.

cleanup(Pid) ->
    application:unset_env(emqx_management, cursor_max_open),
    application:unset_env(emqx_management, cursor_ttl),
    gen_server:stop(Pid).

reusable() ->
    State = [1, 2, #{1 => {cont, make_ref()}}],
    Id = emqx_mgmt_cursor:store(dom, State),
    ?assertMatch({match, _}, re:run(Id, <<"^[A-Za-z0-9_-]+$">>)),
    ?assertEqual({ok, State}, emqx_mgmt_cursor:lookup(dom, Id)),
    ?assertEqual({ok, State}, emqx_mgmt_cursor:lookup(dom, Id)).

wrong_domain() ->
    Id = emqx_mgmt_cursor:store(dom_a, x),
    ?assertEqual({error, bad_cursor}, emqx_mgmt_cursor:lookup(dom_b, Id)),
    ?assertEqual({ok, x}, emqx_mgmt_cursor:lookup(dom_a, Id)).

expired() ->
    application:set_env(emqx_management, cursor_ttl, 0),
    Id = emqx_mgmt_cursor:store(dom, x),
    ?assertEqual({error, bad_cursor}, emqx_mgmt_cursor:lookup(dom, Id)).

limit_drops_oldest() ->
    application:set_env(emqx_management, cursor_max_open, 2),
    Id1 = emqx_mgmt_cursor:store(dom_a, 1),
    Id2 = emqx_mgmt_cursor:store(dom_a, 2),
    IdB = emqx_mgmt_cursor:store(dom_b, 1),
    Id3 = emqx_mgmt_cursor:store(dom_a, 3),
    ?assertEqual({error, bad_cursor}, emqx_mgmt_cursor:lookup(dom_a, Id1)),
    ?assertEqual({ok, 2}, emqx_mgmt_cursor:lookup(dom_a, Id2)),
    ?assertEqual({ok, 3}, emqx_mgmt_cursor:lookup(dom_a, Id3)),
    ?assertEqual({ok, 1}, emqx_mgmt_cursor:lookup(dom_b, IdB)).

limit_drops_expired_first() ->
    application:set_env(emqx_management, cursor_max_open, 2),
    application:set_env(emqx_management, cursor_ttl, 0),
    _ = emqx_mgmt_cursor:store(dom, 1),
    application:set_env(emqx_management, cursor_ttl, 60_000),
    Id2 = emqx_mgmt_cursor:store(dom, 2),
    Id3 = emqx_mgmt_cursor:store(dom, 3),
    ?assertEqual({ok, 2}, emqx_mgmt_cursor:lookup(dom, Id2)),
    ?assertEqual({ok, 3}, emqx_mgmt_cursor:lookup(dom, Id3)).

bad_id() ->
    Id = emqx_mgmt_cursor:store(dom, x),
    <<Rand:16/binary, _/binary>> = base64:decode(Id, #{mode => urlsafe, padding => false}),
    [
        ?assertEqual({error, bad_cursor}, emqx_mgmt_cursor:lookup(dom, Input))
     || Input <- [
            <<"not base64 !@#">>,
            <<>>,
            ?B64(Rand),
            ?B64(<<Rand/binary, "no_such_node_atom_7f3a@nowhere">>),
            ?B64(term_to_binary([1, 1, #{1 => 1, 2 => undefined}])),
            undefined
        ]
    ],
    ?assertEqual({ok, x}, emqx_mgmt_cursor:lookup(dom, Id)).
