%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_mqtt_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

%% A message forwarded to a remote cluster does not carry the local
%% `message_persisted' header.
forward_drops_persisted_header_test() ->
    Msg = emqx_message:make(<<"t/link">>, <<"payload">>),
    Persisted = emqx_message:set_header(message_persisted, true, Msg),
    ok = meck:new(emqx_resource, [no_link]),
    try
        ok = meck:expect(emqx_resource, query, fun(_ResId, Query, _QueryOpts) -> Query end),
        FwdMsg = emqx_cluster_link_mqtt:forward(
            <<"remote">>, #delivery{sender = self(), message = Persisted}
        ),
        ?assertEqual(Msg#message.headers, FwdMsg#message.headers)
    after
        ok = meck:unload(emqx_resource)
    end.

-define(ACTOR, 10).
-define(INCARNATION, 11).
-define(ROUTES, 12).
-define(TARGET_CLUSTER, 13).
-define(PROTO_VER, 14).
-define(RESULT, 15).
-define(NEED_BOOTSTRAP, 16).

route_op_types_test_() ->
    Actor = <<"ps-routes-v1">>,
    ActorInfo = #{actor => Actor, incarnation => 0},
    Base = #{?ACTOR => Actor, ?INCARNATION => 0},
    Init = Base#{
        '$op' => <<"actor_init">>, ?TARGET_CLUSTER => <<"east">>, ?PROTO_VER => 1
    },
    Heartbeat = Base#{'$op' => <<"heartbeat">>},
    Topic = <<"sensor/+">>,
    ID = <<"$s/group/sensor/#">>,
    Add = {Topic, ID},
    Delete = {100, Topic, ID},
    Route = Base#{'$op' => <<"route">>, ?ROUTES => [Add, Delete]},
    Unknown = Base#{'$op' => <<"other">>},
    Invalid = {error, invalid_route_op},
    [
        ?_assertEqual(
            {actor_init, ActorInfo, #{target_cluster => <<"east">>, proto_ver => 1}},
            decode_op(Init)
        ),
        ?_assertEqual({heartbeat, ActorInfo}, decode_op(Heartbeat#{99 => <<"extension">>})),
        ?_assertEqual({route_updates, ActorInfo, [{add, Add}, {delete, Add}]}, decode_op(Route)),
        ?_assertEqual({route_updates, ActorInfo, []}, decode_op(Route#{?ROUTES := []})),
        ?_assertEqual({error, {unknown_payload, redacted}}, decode_op(Unknown)),
        [
            ?_assertEqual(Invalid, decode_op(Op#{?INCARNATION := BadIncarnation}))
         || Op <- [Init, Heartbeat, Route], BadIncarnation <- [<<"1">>, 1.0]
        ],
        [?_assertEqual(Invalid, decode_op(Op#{?ACTOR := 42})) || Op <- [Init, Heartbeat, Route]],
        ?_assertEqual(Invalid, decode_op(maps:remove(?INCARNATION, Route))),
        ?_assertEqual(Invalid, decode_op(Init#{?TARGET_CLUSTER := east})),
        ?_assertEqual(Invalid, decode_op(Init#{?PROTO_VER := <<"1">>})),
        [
            ?_assertEqual(Invalid, decode_op(Route#{?ROUTES := BadRoutes}))
         || BadRoutes <- [
                Add,
                [Add | invalid],
                [Add, {42, ID}],
                [{Topic, 42}],
                [{100, 42, ID}],
                [{100, Topic, 42}],
                [{101, Topic, ID}]
            ]
        ]
    ].

actor_init_ack_types_test_() ->
    Actor = <<"node@east">>,
    Ack = #{
        '$op' => <<"actor_init_ack">>,
        ?ACTOR => Actor,
        ?PROTO_VER => 1,
        ?RESULT => ok,
        ?NEED_BOOTSTRAP => true
    },
    Error = {error, <<"unknown_cluster">>},
    [
        [
            ?_assertEqual(
                {actor_init_ack, #{
                    actor => Actor, proto_ver => 1, result => Result, need_bootstrap => Bootstrap
                }},
                decode_ack(Ack#{?RESULT := Result, ?NEED_BOOTSTRAP := Bootstrap})
            )
         || {Result, Bootstrap} <- [{ok, true}, {ok, false}, {Error, undefined}, {Error, false}]
        ],
        ?_assertEqual({error, invalid_actor_init_ack}, emqx_cluster_link_mqtt:decode_resp(<<>>)),
        ?_assertEqual({error, invalid_actor_init_ack}, decode_ack(maps:remove(?PROTO_VER, Ack))),
        [
            ?_assertEqual({error, invalid_actor_init_ack}, decode_ack(maps:merge(Ack, Fields)))
         || Fields <- [
                #{?ACTOR => 42},
                #{?PROTO_VER => <<"1">>},
                #{?NEED_BOOTSTRAP => undefined},
                #{?NEED_BOOTSTRAP => 1},
                #{?RESULT => Error},
                #{?RESULT => error, ?NEED_BOOTSTRAP => undefined},
                #{'$op' => <<"heartbeat">>}
            ]
        ]
    ].

decode_op(Map) ->
    emqx_cluster_link_mqtt:decode_route_op(term_to_binary(Map)).

decode_ack(Map) ->
    emqx_cluster_link_mqtt:decode_resp(term_to_binary(Map)).
