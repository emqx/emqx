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

%% The gRPC server
-module(emqx_exproto_conn_svr).

-behavior(emqx_exproto_v_1_connection_entity_bhvr).

%% ConnectionEntity callbacks
-export([ send/2
        , close/2
        , authenticate/2
        , publish/2
        , subscribe/2
        , unsubscribe/2
        ]).

%%--------------------------------------------------------------------
%% gRPC ConnectionEntity service
%%--------------------------------------------------------------------

-spec send(ctx:ctx(), emqx_exproto_pb:send_bytes_request()) ->
    {ok, emqx_exproto_pb:bool_result(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
send(Ctx, #{conn := ConnStr, bytes := Bytes}) ->
    emqx_exproto_conn:call(to_pid(ConnStr), {send, Bytes}),
    {ok, #{result => true}, Ctx}.

-spec close(ctx:ctx(), emqx_exproto_pb:close_socket_request()) ->
    {ok, emqx_exproto_pb:bool_result(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
close(Ctx, #{conn := ConnStr}) ->
    emqx_exproto_conn:call(to_pid(ConnStr), close),
    {ok, #{result => true}, Ctx}.

-spec authenticate(ctx:ctx(), emqx_exproto_pb:authenticate_request()) ->
    {ok, emqx_exproto_pb:bool_result(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
authenticate(Ctx, #{conn := ConnStr, password := Password, clientinfo := ClientInfo0}) ->
    case emqx_exproto_types:parse(clientinfo, ClientInfo0) of
        {error, Reason} -> {error, Reason};
        ClientInfo ->
            %% FIXME: Return the auth-result
            emqx_exproto_conn:call(to_pid(ConnStr), {register, ClientInfo, Password}),
            {ok, #{result => true}, Ctx}
    end.

-spec publish(ctx:ctx(), emqx_exproto_pb:publish_request()) ->
    {ok, emqx_exproto_pb:bool_result(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
publish(Ctx, #{conn := ConnStr, topic := Topic, qos := Qos, payload := Payload})
  when is_binary(Topic), is_binary(Payload),
       (Qos =:= 0 orelse Qos =:= 1 orelse Qos =:= 2) ->
    emqx_exproto_conn:call(to_pid(ConnStr), {publish, Topic, Qos, Payload}),
    {ok, #{result => true}, Ctx}.

-spec subscribe(ctx:ctx(), emqx_exproto_pb:subscribe_request()) ->
    {ok, emqx_exproto_pb:bool_result(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
subscribe(Ctx, #{conn := ConnStr, topic := Topic, qos := Qos})
  when is_binary(Topic),
       (Qos =:= 0 orelse Qos =:= 1 orelse Qos =:= 2) ->
    emqx_exproto_conn:call(to_pid(ConnStr), {subscribe, Topic, Qos}),
    {ok, #{result => true}, Ctx}.

-spec unsubscribe(ctx:ctx(), emqx_exproto_pb:unsubscribe_request()) ->
    {ok, emqx_exproto_pb:bool_result(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
unsubscribe(Ctx, #{conn := Conn, topic := Topic})
  when is_binary(Topic) ->
    emqx_exproto_conn:call(to_pid(Conn), {unsubscribe, Topic}),
    {ok, #{result => true}, Ctx}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

to_pid(ConnStr) ->
    list_to_pid(binary_to_list(ConnStr)).
