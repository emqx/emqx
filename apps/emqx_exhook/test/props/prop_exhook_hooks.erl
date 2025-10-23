%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(prop_exhook_hooks).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").

-import(
    emqx_proper_types,
    [
        conninfo/0,
        clientinfo/0,
        sessioninfo/0,
        message/0,
        connack_return_code/0,
        topictab/0,
        topic/0,
        subopts/0,
        pubsub/0
    ]
).

-define(CONF_DEFAULT, <<
    "\n"
    "exhook {\n"
    "  servers =\n"
    "    [ { name = default,\n"
    "        url = \"http://127.0.0.1:9000\"\n"
    "      }\n"
    "    ]\n"
    "}\n"
>>).

-define(ALL(Vars, Types, Exprs),
    ?SETUP(
        fun() ->
            State = do_setup(),
            fun() -> do_teardown(State) end
        end,
        ?FORALL(Vars, Types, Exprs)
    )
).
-define(DEFAULT_CLUSTER_NAME_ATOM, emqxcl).
-define(DEFAULT_CLUSTER_NAME_BIN, <<"emqxcl">>).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_client_connect() ->
    ?ALL(
        {ConnInfo, ConnProps, Meta},
        {conninfo(), conn_properties(), request_meta()},
        begin
            ok = emqx_hooks:run('client.connect', [ConnInfo, ConnProps]),
            {'on_client_connect', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    props => properties(ConnProps),
                    conninfo => from_conninfo(ConnInfo),
                    meta => Meta,
                    user_props => []
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_client_connack() ->
    ?ALL(
        {ConnInfo, Rc, AckProps, Meta},
        {conninfo(), connack_return_code(), ack_properties(), request_meta()},
        begin
            ok = emqx_hooks:run('client.connack', [ConnInfo, Rc, AckProps]),
            {'on_client_connack', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    props => properties(AckProps),
                    result_code => atom_to_binary(Rc, utf8),
                    conninfo => from_conninfo(ConnInfo),
                    meta => Meta,
                    user_props => []
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_client_authenticate() ->
    ?ALL(
        {ClientInfo0, AuthResult, Meta},
        {clientinfo(), authresult(), request_meta()},
        begin
            ClientInfo = inject_magic_into(username, ClientInfo0),
            OutAuthResult = emqx_hooks:run_fold('client.authenticate', [ClientInfo], AuthResult),
            ExpectedAuthResult =
                case maps:get(username, ClientInfo) of
                    <<"baduser">> ->
                        {error, not_authorized};
                    <<"gooduser">> ->
                        ok;
                    <<"normaluser">> ->
                        ok;
                    _ ->
                        %% the exhook server will return ignore if the
                        %% username is not in the whitelist
                        %% so we need to return the original auth result
                        AuthResult
                end,
            ?assertEqual(ExpectedAuthResult, OutAuthResult),

            {'on_client_authenticate', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    result => authresult_to_bool(AuthResult),
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_client_authorize() ->
    MkResult = fun(Result) -> #{result => Result, from => exhook} end,
    ?ALL(
        {ClientInfo0, PubSub, Topic, Result, Meta},
        {
            clientinfo(),
            pubsub(),
            topic(),
            oneof([MkResult(allow), MkResult(deny)]),
            request_meta()
        },
        begin
            ClientInfo = inject_magic_into(username, ClientInfo0),
            OutResult = emqx_hooks:run_fold(
                'client.authorize',
                [ClientInfo, PubSub, Topic],
                Result
            ),
            ExpectedOutResult =
                case maps:get(username, ClientInfo) of
                    <<"baduser">> -> MkResult(deny);
                    <<"gooduser">> -> MkResult(allow);
                    <<"normaluser">> -> MkResult(allow);
                    _ -> Result
                end,
            ?assertEqual(ExpectedOutResult, OutResult),

            {'on_client_authorize', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    result => aclresult_to_bool(Result),
                    type => pubsub_to_enum(PubSub),
                    topic => Topic,
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_client_connected() ->
    ?ALL(
        {ClientInfo, ConnInfo, Meta},
        {clientinfo(), conninfo(), request_meta()},
        begin
            ok = emqx_hooks:run('client.connected', [ClientInfo, ConnInfo]),
            {'on_client_connected', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_client_disconnected() ->
    ?ALL(
        {ClientInfo, Reason, ConnInfo, Meta},
        {clientinfo(), shutdown_reason(), conninfo(), request_meta()},
        begin
            ok = emqx_hooks:run('client.disconnected', [ClientInfo, Reason, ConnInfo]),
            {'on_client_disconnected', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    reason => stringfy(Reason),
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_client_subscribe() ->
    ?ALL(
        {ClientInfo, SubProps, TopicTab, Meta},
        {clientinfo(), sub_properties(), topictab(), request_meta()},
        begin
            ok = emqx_hooks:run('client.subscribe', [ClientInfo, SubProps, TopicTab]),
            {'on_client_subscribe', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    props => properties(SubProps),
                    topic_filters => topicfilters(TopicTab),
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta,
                    user_props => []
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_client_unsubscribe() ->
    ?ALL(
        {ClientInfo, UnSubProps, TopicTab, Meta},
        {clientinfo(), unsub_properties(), topictab(), request_meta()},
        begin
            ok = emqx_hooks:run('client.unsubscribe', [ClientInfo, UnSubProps, TopicTab]),
            {'on_client_unsubscribe', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    props => properties(UnSubProps),
                    topic_filters => topicfilters(TopicTab),
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta,
                    user_props => []
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_session_created() ->
    ?ALL(
        {ClientInfo, SessInfo, Meta},
        {clientinfo(), sessioninfo(), request_meta()},
        begin
            ok = emqx_hooks:run('session.created', [ClientInfo, SessInfo]),
            {'on_session_created', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_session_subscribed() ->
    ?ALL(
        {ClientInfo, Topic, SubOpts, Meta},
        {clientinfo(), topic(), subopts(), request_meta()},
        begin
            ok = emqx_hooks:run('session.subscribed', [ClientInfo, Topic, SubOpts]),
            {'on_session_subscribed', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    topic => Topic,
                    subopts => subopts(SubOpts),
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_session_unsubscribed() ->
    ?ALL(
        {ClientInfo, Topic, SubOpts, Meta},
        {clientinfo(), topic(), subopts(), request_meta()},
        begin
            ok = emqx_hooks:run('session.unsubscribed', [ClientInfo, Topic, SubOpts]),
            {'on_session_unsubscribed', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    topic => Topic,
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_session_resumed() ->
    ?ALL(
        {ClientInfo, SessInfo, Meta},
        {clientinfo(), sessioninfo(), request_meta()},
        begin
            ok = emqx_hooks:run('session.resumed', [ClientInfo, SessInfo]),
            {'on_session_resumed', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_session_discared() ->
    ?ALL(
        {ClientInfo, SessInfo, Meta},
        {clientinfo(), sessioninfo(), request_meta()},
        begin
            ok = emqx_hooks:run('session.discarded', [ClientInfo, SessInfo]),
            {'on_session_discarded', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{clientinfo => from_clientinfo(ClientInfo), meta => Meta},
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_session_takenover() ->
    ?ALL(
        {ClientInfo, SessInfo, Meta},
        {clientinfo(), sessioninfo(), request_meta()},
        begin
            ok = emqx_hooks:run('session.takenover', [ClientInfo, SessInfo]),
            {'on_session_takenover', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{clientinfo => from_clientinfo(ClientInfo), meta => Meta},
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_session_terminated() ->
    ?ALL(
        {ClientInfo, Reason, SessInfo, Meta},
        {clientinfo(), shutdown_reason(), sessioninfo(), request_meta()},
        begin
            ok = emqx_hooks:run('session.terminated', [ClientInfo, Reason, SessInfo]),
            {'on_session_terminated', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{
                    reason => stringfy(Reason),
                    clientinfo => from_clientinfo(ClientInfo),
                    meta => Meta
                },
            ?assertEqual(Expected, Resp),
            true
        end
    ).

prop_message_publish() ->
    ?ALL(
        {Msg0, Meta},
        {message(), request_meta()},
        begin
            Msg = emqx_message:from_map(
                inject_magic_into(from, emqx_message:to_map(Msg0))
            ),
            OutMsg = emqx_hooks:run_fold('message.publish', [], Msg),
            case emqx_topic:match(emqx_message:topic(Msg), <<"$SYS/#">>) of
                true ->
                    ?assertEqual(Msg, OutMsg),
                    skip;
                _ ->
                    ExpectedOutMsg =
                        case emqx_message:from(Msg) of
                            <<"baduser">> ->
                                MsgMap =
                                    #{headers := Headers} =
                                    emqx_message:to_map(Msg),
                                emqx_message:from_map(
                                    MsgMap#{
                                        qos => 0,
                                        topic => <<"">>,
                                        payload => <<"">>,
                                        headers => maps:put(allow_publish, false, Headers)
                                    }
                                );
                            <<"gooduser">> = From ->
                                MsgMap =
                                    #{headers := Headers} =
                                    emqx_message:to_map(Msg),
                                emqx_message:from_map(
                                    MsgMap#{
                                        topic => From,
                                        payload => From,
                                        headers => maps:put(allow_publish, true, Headers)
                                    }
                                );
                            _ ->
                                Msg
                        end,
                    ?assertEqual(ExpectedOutMsg, OutMsg),

                    {'on_message_publish', Resp} = emqx_exhook_demo_svr:take(),
                    Expected =
                        #{
                            message => from_message(Msg),
                            meta => Meta,
                            props => [],
                            user_props => []
                        },
                    ?assertEqual(Expected, Resp)
            end,
            true
        end
    ).

prop_message_dropped() ->
    ?ALL(
        {Msg, By, Reason, Meta},
        {message(), hardcoded, shutdown_reason(), request_meta()},
        begin
            ok = emqx_hooks:run('message.dropped', [Msg, By, Reason]),
            case emqx_topic:match(emqx_message:topic(Msg), <<"$SYS/#">>) of
                true ->
                    skip;
                _ ->
                    {'on_message_dropped', Resp} = emqx_exhook_demo_svr:take(),
                    Expected =
                        #{
                            reason => stringfy(Reason),
                            message => from_message(Msg),
                            meta => Meta
                        },
                    ?assertEqual(Expected, Resp)
            end,
            true
        end
    ).

prop_message_delivered() ->
    ?ALL(
        {ClientInfo, Msg, Meta},
        {clientinfo(), message(), request_meta()},
        begin
            ok = emqx_hooks:run('message.delivered', [ClientInfo, Msg]),
            case emqx_topic:match(emqx_message:topic(Msg), <<"$SYS/#">>) of
                true ->
                    skip;
                _ ->
                    {'on_message_delivered', Resp} = emqx_exhook_demo_svr:take(),
                    Expected =
                        #{
                            clientinfo => from_clientinfo(ClientInfo),
                            message => from_message(Msg),
                            meta => Meta
                        },
                    ?assertEqual(Expected, Resp)
            end,
            true
        end
    ).

prop_message_acked() ->
    ?ALL(
        {ClientInfo, Msg, Meta},
        {clientinfo(), message(), request_meta()},
        begin
            ok = emqx_hooks:run('message.acked', [ClientInfo, Msg]),
            case emqx_topic:match(emqx_message:topic(Msg), <<"$SYS/#">>) of
                true ->
                    skip;
                _ ->
                    {'on_message_acked', Resp} = emqx_exhook_demo_svr:take(),
                    Expected =
                        #{
                            clientinfo => from_clientinfo(ClientInfo),
                            message => from_message(Msg),
                            meta => Meta
                        },
                    ?assertEqual(Expected, Resp)
            end,
            true
        end
    ).

nodestr() ->
    stringfy(node()).

peerhost(#{peername := {Host, _}}) ->
    ntoa(Host).

peerport(#{peername := {_, Port}}) ->
    Port.

sockport(#{sockname := {_, Port}}) ->
    Port.

%% copied from emqx_exhook

ntoa({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
    list_to_binary(inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256}));
ntoa(IP) ->
    list_to_binary(inet_parse:ntoa(IP)).

option(undefined) -> <<>>;
option(B) -> B.

properties(undefined) ->
    [];
properties(M) when is_map(M) ->
    maps:fold(
        fun(K, V, Acc) ->
            [
                #{
                    name => stringfy(K),
                    value => stringfy(V)
                }
                | Acc
            ]
        end,
        [],
        M
    ).

topicfilters(Tfs) when is_list(Tfs) ->
    [
        #{name => emqx_topic:maybe_format_share(Topic), subopts => subopts(SubOpts)}
     || {Topic, SubOpts} <- Tfs
    ].

%% @private
stringfy(Term) when is_binary(Term) ->
    Term;
stringfy(Term) when is_integer(Term) ->
    integer_to_binary(Term);
stringfy(Term) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
stringfy(Term) when is_list(Term) ->
    list_to_binary(Term);
stringfy(Term) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Term]))).

subopts(SubOpts) ->
    #{
        qos => maps:get(qos, SubOpts, 0),
        rh => maps:get(rh, SubOpts, 0),
        rap => maps:get(rap, SubOpts, 0),
        nl => maps:get(nl, SubOpts, 0)
    }.

authresult_to_bool(AuthResult) ->
    AuthResult == ok.

aclresult_to_bool(#{result := Result}) ->
    Result == allow.

pubsub_to_enum(?authz_action(publish)) -> 'PUBLISH';
pubsub_to_enum(?authz_action(subscribe)) -> 'SUBSCRIBE'.

from_conninfo(ConnInfo) ->
    #{
        node => nodestr(),
        clientid => maps:get(clientid, ConnInfo),
        username => option(maps:get(username, ConnInfo, <<>>)),
        peerhost => peerhost(ConnInfo),
        peerport => peerport(ConnInfo),
        sockport => sockport(ConnInfo),
        proto_name => maps:get(proto_name, ConnInfo),
        proto_ver => stringfy(maps:get(proto_ver, ConnInfo)),
        keepalive => maps:get(keepalive, ConnInfo)
    }.

from_clientinfo(ClientInfo) ->
    {PeerHost, PeerPort} = maps:get(peername, ClientInfo),
    #{
        node => nodestr(),
        clientid => maps:get(clientid, ClientInfo),
        username => option(maps:get(username, ClientInfo, <<>>)),
        password => option(maps:get(password, ClientInfo, <<>>)),
        peerhost => ntoa(PeerHost),
        peerport => PeerPort,
        sockport => maps:get(sockport, ClientInfo),
        protocol => stringfy(maps:get(protocol, ClientInfo)),
        mountpoint => option(maps:get(mountpoint, ClientInfo, <<>>)),
        is_superuser => maps:get(is_superuser, ClientInfo, false),
        anonymous => maps:get(anonymous, ClientInfo, true),
        cn => option(maps:get(cn, ClientInfo, <<>>)),
        dn => option(maps:get(dn, ClientInfo, <<>>))
    }.

from_message(Msg) ->
    #{
        node => nodestr(),
        id => emqx_guid:to_hexstr(emqx_message:id(Msg)),
        qos => emqx_message:qos(Msg),
        from => stringfy(emqx_message:from(Msg)),
        topic => emqx_message:topic(Msg),
        payload => emqx_message:payload(Msg),
        timestamp => emqx_message:timestamp(Msg),
        headers => emqx_exhook_handler:headers(
            emqx_message:get_headers(Msg)
        )
    }.

%%--------------------------------------------------------------------
%% Helper
%%--------------------------------------------------------------------

do_setup() ->
    logger:set_primary_config(#{level => warning}),
    ok = ekka:start(),
    application:set_env(ekka, cluster_name, ?DEFAULT_CLUSTER_NAME_ATOM),
    _ = emqx_exhook_demo_svr:start(),
    ok = emqx_config:init_load(emqx_exhook_schema, ?CONF_DEFAULT),
    emqx_common_test_helpers:start_apps([emqx_exhook]),
    %% waiting first loaded event
    {'on_provider_loaded', _} = emqx_exhook_demo_svr:take(),
    ok.

do_teardown(_) ->
    emqx_common_test_helpers:stop_apps([emqx_exhook]),
    %% waiting last unloaded event
    {'on_provider_unloaded', _} = emqx_exhook_demo_svr:take(),
    _ = emqx_exhook_demo_svr:stop(),
    logger:set_primary_config(#{level => notice}),
    timer:sleep(2000),
    ok.

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------

conn_properties() ->
    #{}.

ack_properties() ->
    #{}.

sub_properties() ->
    #{}.

unsub_properties() ->
    #{}.

shutdown_reason() ->
    oneof([utf8(), {shutdown, emqx_proper_types:limited_latin_atom()}]).

authresult() ->
    ?LET(
        RC,
        connack_return_code(),
        case RC of
            success -> ok;
            _ -> {error, RC}
        end
    ).

inject_magic_into(Key, Object) ->
    case castspell() of
        muggles -> Object;
        Spell -> Object#{Key => Spell}
    end.

castspell() ->
    L = [<<"baduser">>, <<"gooduser">>, <<"normaluser">>, muggles],
    lists:nth(rand:uniform(length(L)), L).

request_meta() ->
    #{
        node => nodestr(),
        version => stringfy(emqx_sys:version()),
        sysdescr => stringfy(emqx_sys:sysdescr()),
        cluster_name => ?DEFAULT_CLUSTER_NAME_BIN
    }.
