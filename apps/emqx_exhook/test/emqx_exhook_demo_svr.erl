%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_demo_svr).

-behavior(emqx_exhook_v_1_hook_provider_bhvr).

%%
-export([ start/0
        , stop/0
        , take/0
        , in/1
        ]).

%% gRPC server HookProvider callbacks
-export([ on_provider_loaded/2
        , on_provider_unloaded/2
        , on_client_connect/2
        , on_client_connack/2
        , on_client_connected/2
        , on_client_disconnected/2
        , on_client_authenticate/2
        , on_client_check_acl/2
        , on_client_subscribe/2
        , on_client_unsubscribe/2
        , on_session_created/2
        , on_session_subscribed/2
        , on_session_unsubscribed/2
        , on_session_resumed/2
        , on_session_discarded/2
        , on_session_takeovered/2
        , on_session_terminated/2
        , on_message_publish/2
        , on_message_delivered/2
        , on_message_dropped/2
        , on_message_acked/2
        ]).

-define(PORT, 9000).
-define(NAME, ?MODULE).
-define(DEFAULT_CLUSTER_NAME, <<"emqxcl">>).
-define(OTHER_CLUSTER_NAME_BIN, <<"test_emqx_cluster">>).
-define(TEST_PUBLISH_FILTERS_BIN, <<"test_message_filters">>).
-define(AFTER_HARDCODED_PAYLOAD, <<"after_hardcoded">>).

%%--------------------------------------------------------------------
%% Server APIs
%%--------------------------------------------------------------------

start() ->
    Pid = spawn(fun mngr_main/0),
    register(?MODULE, Pid),
    {ok, Pid}.

stop() ->
    grpc:stop_server(?NAME),
    ?MODULE ! stop.

take() ->
    ?MODULE ! {take, self()},
    receive {value, V} -> V
    after 5000 -> error(timeout) end.

in({FunName, Req}) ->
    ?MODULE ! {in, FunName, Req}.

mngr_main() ->
    application:ensure_all_started(grpc),
    Services = #{protos => [emqx_exhook_pb],
                 services => #{'emqx.exhook.v1.HookProvider' => emqx_exhook_demo_svr}
                },
    Options = [],
    Svr = grpc:start_server(?NAME, ?PORT, Services, Options),
    mngr_loop([Svr, queue:new(), queue:new()]).

mngr_loop([Svr, Q, Takes]) ->
    receive
        {in, FunName, Req} ->
            {NQ1, NQ2} = reply(queue:in({FunName, Req}, Q), Takes),
            mngr_loop([Svr, NQ1, NQ2]);
        {take, From} ->
            {NQ1, NQ2} = reply(Q, queue:in(From, Takes)),
            mngr_loop([Svr, NQ1, NQ2]);
        stop ->
            exit(normal)
    end.

reply(Q1, Q2) ->
    case queue:len(Q1) =:= 0 orelse
         queue:len(Q2) =:= 0 of
        true -> {Q1, Q2};
        _ ->
            {{value, {Name, V}}, NQ1} = queue:out(Q1),
            {{value, From}, NQ2} = queue:out(Q2),
            From ! {value, {Name, V}},
            {NQ1, NQ2}
    end.

%%--------------------------------------------------------------------
%% callbacks
%%--------------------------------------------------------------------

-spec on_provider_loaded(emqx_exhook_pb:provider_loaded_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:loaded_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_provider_loaded(#{meta := #{cluster_name := Name}} = Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    HooksClient =
        [#{name => <<"client.connect">>},
         #{name => <<"client.connack">>},
         #{name => <<"client.connected">>},
         #{name => <<"client.disconnected">>},
         #{name => <<"client.authenticate">>},
         #{name => <<"client.check_acl">>},
         #{name => <<"client.subscribe">>},
         #{name => <<"client.unsubscribe">>}],
    HooksSession =
        [#{name => <<"session.created">>},
         #{name => <<"session.subscribed">>},
         #{name => <<"session.unsubscribed">>},
         #{name => <<"session.resumed">>},
         #{name => <<"session.discarded">>},
         #{name => <<"session.takeovered">>},
         #{name => <<"session.terminated">>}],
    PublishWithFilter =
        [#{name => <<"message.publish">>, topics => [<<"t/1">>, <<"a/#">>, <<"b/+">>]}],
    PublishWithOutFilter =
        [#{name => <<"message.publish">>}],
    HooksMessage =
        [#{name => <<"message.delivered">>},
         #{name => <<"message.acked">>},
         #{name => <<"message.dropped">>}],
    case Name of
        ?DEFAULT_CLUSTER_NAME ->
            {ok, #{hooks => HooksClient ++ HooksSession ++ PublishWithOutFilter ++ HooksMessage}, Md};
        ?OTHER_CLUSTER_NAME_BIN ->
            {ok, #{hooks => HooksClient}, Md};
        ?TEST_PUBLISH_FILTERS_BIN ->
            {ok, #{hooks => HooksClient ++ HooksSession ++ PublishWithFilter ++ HooksMessage}, Md}
    end.
-spec on_provider_unloaded(emqx_exhook_pb:provider_unloaded_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_provider_unloaded(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_client_connect(emqx_exhook_pb:client_connect_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_client_connect(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_client_connack(emqx_exhook_pb:client_connack_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_client_connack(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_client_connected(emqx_exhook_pb:client_connected_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_client_connected(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_client_disconnected(emqx_exhook_pb:client_disconnected_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_client_disconnected(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_client_authenticate(emqx_exhook_pb:client_authenticate_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_client_authenticate(#{clientinfo := #{username := Username}} = Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    %% some cases for testing
    case Username of
        <<"baduser">> ->
            {ok, #{type => 'STOP_AND_RETURN',
                   value => {bool_result, false}}, Md};
        <<"gooduser">> ->
            {ok, #{type => 'STOP_AND_RETURN',
                   value => {bool_result, true}}, Md};
        <<"normaluser">> ->
            {ok, #{type => 'CONTINUE',
                   value => {bool_result, true}}, Md};
        _ ->
            {ok, #{type => 'IGNORE'}, Md}
    end.

-spec on_client_check_acl(emqx_exhook_pb:client_check_acl_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_client_check_acl(#{clientinfo := #{username := Username}} = Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    %% some cases for testing
    case Username of
        <<"baduser">> ->
            {ok, #{type => 'STOP_AND_RETURN',
                   value => {bool_result, false}}, Md};
        <<"gooduser">> ->
            {ok, #{type => 'STOP_AND_RETURN',
                   value => {bool_result, true}}, Md};
        <<"normaluser">> ->
            {ok, #{type => 'CONTINUE',
                   value => {bool_result, true}}, Md};
        _ ->
            {ok, #{type => 'IGNORE'}, Md}
    end.

-spec on_client_subscribe(emqx_exhook_pb:client_subscribe_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_client_subscribe(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_client_unsubscribe(emqx_exhook_pb:client_unsubscribe_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_client_unsubscribe(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_session_created(emqx_exhook_pb:session_created_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_session_created(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_session_subscribed(emqx_exhook_pb:session_subscribed_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_session_subscribed(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_session_unsubscribed(emqx_exhook_pb:session_unsubscribed_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_session_unsubscribed(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_session_resumed(emqx_exhook_pb:session_resumed_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_session_resumed(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_session_discarded(emqx_exhook_pb:session_discarded_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_session_discarded(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_session_takeovered(emqx_exhook_pb:session_takeovered_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_session_takeovered(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_session_terminated(emqx_exhook_pb:session_terminated_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_session_terminated(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_message_publish(emqx_exhook_pb:message_publish_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_message_publish(#{message := #{from := From} = Msg} = Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    %% some cases for testing
    case From of
        <<"baduser">> ->
            NMsg = deny(Msg#{qos => 0,
                        topic => <<"">>,
                        payload => <<"">>
                       }),
            {ok, #{type => 'STOP_AND_RETURN',
                   value => {message, NMsg}}, Md};
        <<"gooduser">> ->
            NMsg = allow(Msg#{topic => From,
                              payload => From}),
            {ok, #{type => 'STOP_AND_RETURN',
                   value => {message, NMsg}}, Md};
        <<"test_filter_client">> ->
            %% rewrite topic and payload
            NMsg = Msg#{topic => <<"exhook/hardcoded">>,
                        payload => ?AFTER_HARDCODED_PAYLOAD},
            {ok, #{type => 'STOP_AND_RETURN',
                   value => {message, NMsg}}, Md};
        _ ->
            {ok, #{type => 'IGNORE'}, Md}
    end.

deny(Msg) ->
    NHeader = maps:put(<<"allow_publish">>, <<"false">>,
                       maps:get(headers, Msg, #{})),
    maps:put(headers, NHeader, Msg).

allow(Msg) ->
    NHeader = maps:put(<<"allow_publish">>, <<"true">>,
                       maps:get(headers, Msg, #{})),
    maps:put(headers, NHeader, Msg).

-spec on_message_delivered(emqx_exhook_pb:message_delivered_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_message_delivered(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_message_dropped(emqx_exhook_pb:message_dropped_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_message_dropped(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.

-spec on_message_acked(emqx_exhook_pb:message_acked_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
on_message_acked(Req, Md) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Md}.
