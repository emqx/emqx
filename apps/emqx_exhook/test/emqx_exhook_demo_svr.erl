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

-define(HTTP, #{grpc_opts => #{service_protos => [emqx_exhook_pb],
                               services => #{'emqx.exhook.v1.HookProvider' => emqx_exhook_demo_svr}},
                listen_opts => #{port => ?PORT,
                                 socket_options => [{reuseaddr, true}]},
                pool_opts => #{size => 8},
                transport_opts => #{ssl => false}}).

%%--------------------------------------------------------------------
%% Server APIs
%%--------------------------------------------------------------------

start() ->
    Pid = spawn(fun mngr_main/0),
    register(?MODULE, Pid),
    {ok, Pid}.

stop() ->
    ?MODULE ! stop.

take() ->
    ?MODULE ! {take, self()},
    receive {value, V} -> V
    after 5000 -> error(timeout) end.

in({FunName, Req}) ->
    ?MODULE ! {in, FunName, Req}.

mngr_main() ->
    application:ensure_all_started(grpcbox),
    Svr = grpcbox:start_server(?HTTP),
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
            supervisor:terminate_child(grpcbox_services_simple_sup, Svr),
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

-spec on_provider_loaded(ctx:ctx(), emqx_exhook_pb:on_provider_loadedial_request())
  -> {ok, emqx_exhook_pb:on_provider_loaded_response(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_provider_loaded(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{hooks => [
                     #{name => <<"client.connect">>},
                     #{name => <<"client.connack">>},
                     #{name => <<"client.connected">>},
                     #{name => <<"client.disconnected">>},
                     #{name => <<"client.authenticate">>},
                     #{name => <<"client.check_acl">>},
                     #{name => <<"client.subscribe">>},
                     #{name => <<"client.unsubscribe">>},
                     #{name => <<"session.created">>},
                     #{name => <<"session.subscribed">>},
                     #{name => <<"session.unsubscribed">>},
                     #{name => <<"session.resumed">>},
                     #{name => <<"session.discarded">>},
                     #{name => <<"session.takeovered">>},
                     #{name => <<"session.terminated">>},
                     #{name => <<"message.publish">>},
                     #{name => <<"message.delivered">>},
                     #{name => <<"message.acked">>},
                     #{name => <<"message.dropped">>}]}, Ctx}.

-spec on_provider_unloaded(ctx:ctx(), emqx_exhook_pb:on_provider_unloadedial_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_provider_unloaded(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_client_connect(ctx:ctx(), emqx_exhook_pb:client_connect_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_client_connect(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_client_connack(ctx:ctx(), emqx_exhook_pb:client_connack_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_client_connack(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_client_connected(ctx:ctx(), emqx_exhook_pb:client_connected_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_client_connected(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_client_disconnected(ctx:ctx(), emqx_exhook_pb:client_disconnected_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_client_disconnected(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_client_authenticate(ctx:ctx(), emqx_exhook_pb:client_authenticate_request())
  -> {ok, emqx_exhook_pb:bool_result(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_client_authenticate(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{type => 'IGNORE'}, Ctx}.

-spec on_client_check_acl(ctx:ctx(), emqx_exhook_pb:client_check_acl_request())
  -> {ok, emqx_exhook_pb:bool_result(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_client_check_acl(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{type => 'STOP_AND_RETURN', value => {bool_result, true}}, Ctx}.

-spec on_client_subscribe(ctx:ctx(), emqx_exhook_pb:client_subscribe_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_client_subscribe(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_client_unsubscribe(ctx:ctx(), emqx_exhook_pb:client_unsubscribe_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_client_unsubscribe(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_session_created(ctx:ctx(), emqx_exhook_pb:session_created_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_session_created(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_session_subscribed(ctx:ctx(), emqx_exhook_pb:session_subscribed_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_session_subscribed(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_session_unsubscribed(ctx:ctx(), emqx_exhook_pb:session_unsubscribed_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_session_unsubscribed(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_session_resumed(ctx:ctx(), emqx_exhook_pb:session_resumed_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_session_resumed(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_session_discarded(ctx:ctx(), emqx_exhook_pb:session_discarded_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_session_discarded(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_session_takeovered(ctx:ctx(), emqx_exhook_pb:session_takeovered_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_session_takeovered(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_session_terminated(ctx:ctx(), emqx_exhook_pb:session_terminated_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_session_terminated(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_message_publish(ctx:ctx(), emqx_exhook_pb:message_publish_request())
  -> {ok, emqx_exhook_pb:valued_response(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_message_publish(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_message_delivered(ctx:ctx(), emqx_exhook_pb:message_delivered_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_message_delivered(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_message_dropped(ctx:ctx(), emqx_exhook_pb:message_dropped_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_message_dropped(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_message_acked(ctx:ctx(), emqx_exhook_pb:message_acked_request())
  -> {ok, emqx_exhook_pb:empty_success(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
on_message_acked(Ctx, Req) ->
    ?MODULE:in({?FUNCTION_NAME, Req}),
    %io:format("fun: ~p, req: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.
