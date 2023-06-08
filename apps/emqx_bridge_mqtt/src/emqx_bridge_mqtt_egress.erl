%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_mqtt_egress).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-behaviour(ecpool_worker).

%% ecpool
-export([connect/1]).

-export([
    config/1,
    send/3,
    send_async/4
]).

%% management APIs
-export([
    status/1,
    info/1
]).

-type name() :: term().
-type message() :: emqx_types:message() | map().
-type callback() :: {function(), [_Arg]} | {module(), atom(), [_Arg]}.
-type remote_message() :: #mqtt_msg{}.

-type option() ::
    {name, name()}
    %% see `emqtt:option()`
    | {client_opts, map()}.

-type egress() :: #{
    local => #{
        topic => emqx_types:topic()
    },
    remote := emqx_bridge_mqtt_msg:msgvars()
}.

%% @doc Start an ingress bridge worker.
-spec connect([option() | {ecpool_worker_id, pos_integer()}]) ->
    {ok, pid()} | {error, _Reason}.
connect(Options) ->
    ?SLOG(debug, #{
        msg => "egress_client_starting",
        options => emqx_utils:redact(Options)
    }),
    Name = proplists:get_value(name, Options),
    WorkerId = proplists:get_value(ecpool_worker_id, Options),
    ClientOpts = proplists:get_value(client_opts, Options),
    case emqtt:start_link(mk_client_opts(WorkerId, ClientOpts)) of
        {ok, Pid} ->
            connect(Pid, Name);
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "egress_client_start_failed",
                config => emqx_utils:redact(ClientOpts),
                reason => Reason
            }),
            Error
    end.

mk_client_opts(WorkerId, ClientOpts = #{clientid := ClientId}) ->
    ClientOpts#{clientid := mk_clientid(WorkerId, ClientId)}.

mk_clientid(WorkerId, ClientId) ->
    iolist_to_binary([ClientId, $: | integer_to_list(WorkerId)]).

connect(Pid, Name) ->
    case emqtt:connect(Pid) of
        {ok, _Props} ->
            {ok, Pid};
        {error, Reason} = Error ->
            ?SLOG(warning, #{
                msg => "egress_client_connect_failed",
                reason => Reason,
                name => Name
            }),
            _ = catch emqtt:stop(Pid),
            Error
    end.

%%

-spec config(map()) ->
    egress().
config(#{remote := RC = #{}} = Conf) ->
    Conf#{remote => emqx_bridge_mqtt_msg:parse(RC)}.

-spec send(pid(), message(), egress()) ->
    ok.
send(Pid, MsgIn, Egress) ->
    emqtt:publish(Pid, export_msg(MsgIn, Egress)).

-spec send_async(pid(), message(), callback(), egress()) ->
    ok | {ok, pid()}.
send_async(Pid, MsgIn, Callback, Egress) ->
    ok = emqtt:publish_async(Pid, export_msg(MsgIn, Egress), _Timeout = infinity, Callback),
    {ok, Pid}.

export_msg(Msg, #{remote := Remote}) ->
    to_remote_msg(Msg, Remote).

-spec to_remote_msg(message(), emqx_bridge_mqtt_msg:msgvars()) ->
    remote_message().
to_remote_msg(#message{flags = Flags} = Msg, Vars) ->
    {EventMsg, _} = emqx_rule_events:eventmsg_publish(Msg),
    to_remote_msg(EventMsg#{retain => maps:get(retain, Flags, false)}, Vars);
to_remote_msg(Msg = #{}, Remote) ->
    #{
        topic := Topic,
        payload := Payload,
        qos := QoS,
        retain := Retain
    } = emqx_bridge_mqtt_msg:render(Msg, Remote),
    PubProps = maps:get(pub_props, Msg, #{}),
    #mqtt_msg{
        qos = QoS,
        retain = Retain,
        topic = Topic,
        props = emqx_utils:pub_props_to_packet(PubProps),
        payload = Payload
    }.

%%

-spec info(pid()) ->
    [{atom(), term()}].
info(Pid) ->
    emqtt:info(Pid).

-spec status(pid()) ->
    emqx_resource:resource_status().
status(Pid) ->
    try
        case proplists:get_value(socket, info(Pid)) of
            Socket when Socket /= undefined ->
                connected;
            undefined ->
                connecting
        end
    catch
        exit:{noproc, _} ->
            disconnected
    end.
