%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(prop_exhook_hooks).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(emqx_ct_proper_types,
        [ conninfo/0
        , clientinfo/0
        , sessioninfo/0
        , message/0
        , connack_return_code/0
        , topictab/0
        , topic/0
        , subopts/0
        ]).

-define(ALL(Vars, Types, Exprs),
        ?SETUP(fun() ->
            State = do_setup(),
            fun() -> do_teardown(State) end
         end, ?FORALL(Vars, Types, Exprs))).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_client_connect() ->
    ?ALL({ConnInfo, ConnProps},
         {conninfo(), conn_properties()},
       begin
           _OutConnProps = emqx_hooks:run_fold('client.connect', [ConnInfo], ConnProps),
           {'on_client_connect', Resp} = emqx_exhook_demo_svr:take(),
           Expected =
               #{props => properties(ConnProps),
                 conninfo =>
                   #{node => nodestr(),
                     clientid => maps:get(clientid, ConnInfo),
                     username => maybe(maps:get(username, ConnInfo, <<>>)),
                     peerhost => peerhost(ConnInfo),
                     sockport => sockport(ConnInfo),
                     proto_name => maps:get(proto_name, ConnInfo),
                     proto_ver => stringfy(maps:get(proto_ver, ConnInfo)),
                     keepalive => maps:get(keepalive, ConnInfo)
                    }
                },
           ?assertEqual(Expected, Resp),
           true
       end).

prop_client_connack() ->
    ?ALL({ConnInfo, Rc, AckProps},
         {conninfo(), connack_return_code(), ack_properties()},
        begin
            _OutAckProps = emqx_hooks:run_fold('client.connack', [ConnInfo, Rc], AckProps),
            {'on_client_connack', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{props => properties(AckProps),
                  result_code => atom_to_binary(Rc, utf8),
                  conninfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ConnInfo),
                      username => maybe(maps:get(username, ConnInfo, <<>>)),
                      peerhost => peerhost(ConnInfo),
                      sockport => sockport(ConnInfo),
                      proto_name => maps:get(proto_name, ConnInfo),
                      proto_ver => stringfy(maps:get(proto_ver, ConnInfo)),
                      keepalive => maps:get(keepalive, ConnInfo)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_client_authenticate() ->
    ?ALL({ClientInfo, AuthResult}, {clientinfo(), authresult()},
        begin
            _OutAuthResult = emqx_hooks:run_fold('client.authenticate', [ClientInfo], AuthResult),
            {'on_client_authenticate', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{result => authresult_to_bool(AuthResult),
                  clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_client_check_acl() ->
    ?ALL({ClientInfo, PubSub, Topic, Result},
         {clientinfo(), oneof([publish, subscribe]), topic(), oneof([allow, deny])},
        begin
            _OutResult = emqx_hooks:run_fold('client.check_acl', [ClientInfo, PubSub, Topic], Result),
            {'on_client_check_acl', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{result => aclresult_to_bool(Result),
                  type => pubsub_to_enum(PubSub),
                  topic => Topic,
                  clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).


prop_client_connected() ->
    ?ALL({ClientInfo, ConnInfo},
         {clientinfo(), conninfo()},
        begin
            ok = emqx_hooks:run('client.connected', [ClientInfo, ConnInfo]),
            {'on_client_connected', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_client_disconnected() ->
    ?ALL({ClientInfo, Reason, ConnInfo},
         {clientinfo(), shutdown_reason(), conninfo()},
        begin
            ok = emqx_hooks:run('client.disconnected', [ClientInfo, Reason, ConnInfo]),
            {'on_client_disconnected', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{reason => stringfy(Reason),
                  clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_client_subscribe() ->
    ?ALL({ClientInfo, SubProps, TopicTab},
         {clientinfo(), sub_properties(), topictab()},
        begin
            _OutTopicTab = emqx_hooks:run_fold('client.subscribe', [ClientInfo, SubProps], TopicTab),
            {'on_client_subscribe', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{props => properties(SubProps),
                  topic_filters => topicfilters(TopicTab),
                  clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_client_unsubscribe() ->
    ?ALL({ClientInfo, UnSubProps, TopicTab},
         {clientinfo(), unsub_properties(), topictab()},
        begin
            _OutTopicTab = emqx_hooks:run_fold('client.unsubscribe', [ClientInfo, UnSubProps], TopicTab),
            {'on_client_unsubscribe', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{props => properties(UnSubProps),
                  topic_filters => topicfilters(TopicTab),
                  clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_session_created() ->
    ?ALL({ClientInfo, SessInfo}, {clientinfo(), sessioninfo()},
        begin
            ok = emqx_hooks:run('session.created', [ClientInfo, SessInfo]),
            {'on_session_created', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
             ?assertEqual(Expected, Resp),
            true
        end).

prop_session_subscribed() ->
    ?ALL({ClientInfo, Topic, SubOpts},
         {clientinfo(), topic(), subopts()},
        begin
            ok = emqx_hooks:run('session.subscribed', [ClientInfo, Topic, SubOpts]),
            {'on_session_subscribed', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{topic => Topic,
                  subopts => subopts(SubOpts),
                  clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_session_unsubscribed() ->
    ?ALL({ClientInfo, Topic, SubOpts},
         {clientinfo(), topic(), subopts()},
        begin
            ok = emqx_hooks:run('session.unsubscribed', [ClientInfo, Topic, SubOpts]),
            {'on_session_unsubscribed', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{topic => Topic,
                  clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_session_resumed() ->
    ?ALL({ClientInfo, SessInfo}, {clientinfo(), sessioninfo()},
        begin
            ok = emqx_hooks:run('session.resumed', [ClientInfo, SessInfo]),
            {'on_session_resumed', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_session_discared() ->
    ?ALL({ClientInfo, SessInfo}, {clientinfo(), sessioninfo()},
        begin
            ok = emqx_hooks:run('session.discarded', [ClientInfo, SessInfo]),
            {'on_session_discarded', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_session_takeovered() ->
    ?ALL({ClientInfo, SessInfo}, {clientinfo(), sessioninfo()},
        begin
            ok = emqx_hooks:run('session.takeovered', [ClientInfo, SessInfo]),
            {'on_session_takeovered', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),
            true
        end).

prop_session_terminated() ->
    ?ALL({ClientInfo, Reason, SessInfo},
         {clientinfo(), shutdown_reason(), sessioninfo()},
        begin
            ok = emqx_hooks:run('session.terminated', [ClientInfo, Reason, SessInfo]),
            {'on_session_terminated', Resp} = emqx_exhook_demo_svr:take(),
            Expected =
                #{reason => stringfy(Reason),
                  clientinfo =>
                    #{node => nodestr(),
                      clientid => maps:get(clientid, ClientInfo),
                      username => maybe(maps:get(username, ClientInfo, <<>>)),
                      password => maybe(maps:get(password, ClientInfo, <<>>)),
                      peerhost => ntoa(maps:get(peerhost, ClientInfo)),
                      sockport => maps:get(sockport, ClientInfo),
                      protocol => stringfy(maps:get(protocol, ClientInfo)),
                      mountpoint => maybe(maps:get(mountpoint, ClientInfo, <<>>)),
                      is_superuser => maps:get(is_superuser, ClientInfo, false),
                      anonymous => maps:get(anonymous, ClientInfo, true)
                     }
                 },
            ?assertEqual(Expected, Resp),

            true
        end).

nodestr() ->
    stringfy(node()).

peerhost(#{peername := {Host, _}}) ->
    ntoa(Host).

sockport(#{sockname := {_, Port}}) ->
    Port.

%% copied from emqx_exhook

ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    list_to_binary(inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256}));
ntoa(IP) ->
    list_to_binary(inet_parse:ntoa(IP)).

maybe(undefined) -> <<>>;
maybe(B) -> B.

properties(undefined) -> [];
properties(M) when is_map(M) ->
    maps:fold(fun(K, V, Acc) ->
        [#{name => stringfy(K),
           value => stringfy(V)} | Acc]
    end, [], M).

topicfilters(Tfs) when is_list(Tfs) ->
    [#{name => Topic, qos => Qos} || {Topic, #{qos := Qos}} <- Tfs].

%% @private
stringfy(Term) when is_binary(Term) ->
    Term;
stringfy(Term) when is_integer(Term) ->
    integer_to_binary(Term);
stringfy(Term) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
stringfy(Term) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Term]))).

subopts(SubOpts) ->
    #{qos => maps:get(qos, SubOpts, 0),
      rh => maps:get(rh, SubOpts, 0),
      rap => maps:get(rap, SubOpts, 0),
      nl => maps:get(nl, SubOpts, 0),
      share => maps:get(share, SubOpts, <<>>)
     }.

authresult_to_bool(AuthResult) ->
    maps:get(auth_result, AuthResult, undefined) == success.

aclresult_to_bool(Result) ->
    Result == allow.

pubsub_to_enum(publish) -> 'PUBLISH';
pubsub_to_enum(subscribe) -> 'SUBSCRIBE'.

%prop_message_publish() ->
%    ?ALL({Msg, Env, Encode}, {message(), topic_filter_env()},
%        begin
%            true
%        end).
%
%prop_message_delivered() ->
%    ?ALL({ClientInfo, Msg, Env, Encode}, {clientinfo(), message(), topic_filter_env()},
%        begin
%            true
%        end).
%
%prop_message_acked() ->
%    ?ALL({ClientInfo, Msg, Env, Encode}, {clientinfo(), message()},
%        begin
%            true
%        end).

%%--------------------------------------------------------------------
%% Helper
%%--------------------------------------------------------------------

do_setup() ->
    _ = emqx_exhook_demo_svr:start(),
    emqx_ct_helpers:start_apps([emqx_exhook], fun set_special_cfgs/1),
    emqx_logger:set_log_level(warning),
    %% waiting first loaded event
    {'on_provider_loaded', _} = emqx_exhook_demo_svr:take(),
    ok.

do_teardown(_) ->
    emqx_ct_helpers:stop_apps([emqx_exhook]),
    %% waiting last unloaded event
    {'on_provider_unloaded', _} = emqx_exhook_demo_svr:take(),
    _ = emqx_exhook_demo_svr:stop(),
    timer:sleep(2000),
    ok.

set_special_cfgs(emqx) ->
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins"));
set_special_cfgs(emqx_exhook) ->
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
    oneof([utf8(), {shutdown, atom()}]).

authresult() ->
    #{auth_result => connack_return_code()}.

%topic_filter_env() ->
%    oneof([{<<"#">>}, {undefined}, {topic()}]).
