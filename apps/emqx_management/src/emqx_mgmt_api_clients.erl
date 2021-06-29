%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_clients).

-include_lib("emqx/include/emqx.hrl").

-include_lib("emqx/include/logger.hrl").


%% API
-export([rest_schema/0, rest_api/0]).

-export([ handle_list/1
        , handle_get/1
        , handle_delete/1
        , handle_get_acl_cache/1
        , handle_clean_acl_cache/1
        , handle_publish/1
        , handle_publish_batch/1
        , handle_subscribe/1
        , handle_subscribe_batch/1]).

-export([format_channel_info/1]).

%% for batch operation
-export([do_publish/5, do_subscribe/3]).

-define(CLIENT_QS_SCHEMA, {emqx_channel_info,
    [{<<"clientid">>, binary},
        {<<"username">>, binary},
        {<<"zone">>, atom},
        {<<"ip_address">>, ip},
        {<<"conn_state">>, atom},
        {<<"clean_start">>, atom},
        {<<"proto_name">>, binary},
        {<<"proto_ver">>, integer},
        {<<"_like_clientid">>, binary},
        {<<"_like_username">>, binary},
        {<<"_gte_created_at">>, timestamp},
        {<<"_lte_created_at">>, timestamp},
        {<<"_gte_connected_at">>, timestamp},
        {<<"_lte_connected_at">>, timestamp}]}).

-define(query_fun, {?MODULE, query}).
-define(format_fun, {?MODULE, format_channel_info}).

-define(CLIENT_ID_NOT_FOUND, <<"{\"code\": \"RESOURCE_NOT_FOUND\", \"reason\": \"Client id not found\"}">>).

rest_schema() ->
    ClientDef = #{
        <<"node">> =>
        #{ type => <<"string">>, description => <<"Name of the node to which the client is connected">>},
        <<"clientid">> =>
        #{ type => <<"string">>, description => <<"Client identifier">>},
        <<"username">> =>
        #{ type => <<"string">>, description => <<"User name of client when connecting">>},
        <<"proto_name">> =>
        #{ type => <<"string">>, description => <<"Client protocol name">>},
        <<"proto_ver">> =>
        #{ type => <<"integer">>, description => <<"Protocol version used by the client">>},
        <<"ip_address">> =>
        #{ type => <<"string">>, description => <<"Client's IP address">>},
        <<"is_bridge">> =>
        #{ type => <<"boolean">>, description => <<"Indicates whether the client is connected via bridge">>},
        <<"connected_at">> =>
        #{ type => <<"string">>, description => <<"Client connection time">>},
        <<"disconnected_at">> =>
        #{ type => <<"string">>, description => <<"Client offline time, This field is only valid and returned when connected is false">>},
        <<"connected">> =>
        #{ type => <<"boolean">>, description => <<"Whether the client is connected">>},
        <<"will_msg">> =>
        #{ type => <<"string">>, description => <<"Client will message">>},
        <<"zone">> =>
        #{ type => <<"string">>, description => <<"Indicate the configuration group used by the client">>},
        <<"keepalive">> =>
        #{ type => <<"integer">>, description => <<"keepalive time, with the unit of second">>},
        <<"clean_start">> =>
        #{ type => <<"boolean">>, description => <<"Indicate whether the client is using a brand new session">>},
        <<"expiry_interval">> =>
        #{ type => <<"integer">>, description => <<"Session expiration interval, with the unit of second">>},
        <<"created_at">> =>
        #{ type => <<"string">>, description => <<"Session creation time">>},
        <<"subscriptions_cnt">> =>
        #{ type => <<"integer">>, description => <<"Number of subscriptions established by this client">>},
        <<"subscriptions_max">> =>
        #{ type => <<"integer">>, description => <<"Maximum number of subscriptions allowed by this client">>},
        <<"inflight_cnt">> =>
        #{ type => <<"integer">>, description => <<"Current length of inflight">>},
        <<"inflight_max">> =>
        #{ type => <<"integer">>, description => <<"Maximum length of inflight">>},
        <<"mqueue_len">> =>
        #{ type => <<"integer">>, description => <<"Current length of message queue">>},
        <<"mqueue_max">> =>
        #{ type => <<"integer">>, description => <<"Maximum length of message queue">>},
        <<"mqueue_dropped">> =>
        #{ type => <<"integer">>, description => <<"Number of messages dropped by the message queue due to exceeding the length">>},
        <<"awaiting_rel_cnt">> =>
        #{ type => <<"integer">>, description => <<"Number of awaiting PUBREC packet">>},
        <<"awaiting_rel_max">> =>
        #{ type => <<"integer">>, description => <<"Maximum allowed number of awaiting PUBREC packet">>},
        <<"recv_oct">> =>
        #{ type => <<"integer">>, description => <<"Number of bytes received by EMQ X Broker (the same below)">>},
        <<"recv_cnt">> =>
        #{ type => <<"integer">>, description => <<"Number of TCP packets received">>},
        <<"recv_pkt">> =>
        #{ type => <<"integer">>, description => <<"Number of MQTT packets received">>},
        <<"recv_msg">> =>
        #{ type => <<"integer">>, description => <<"Number of PUBLISH packets received">>},
        <<"send_oct">> =>
        #{ type => <<"integer">>, description => <<"Number of bytes sent">>},
        <<"send_cnt">> =>
        #{ type => <<"integer">>, description => <<"Number of TCP packets sent">>},
        <<"send_pkt">> =>
        #{ type => <<"integer">>, description => <<"Number of MQTT packets sent">>},
        <<"send_msg">> =>
        #{ type => <<"integer">>, description => <<"Number of PUBLISH packets sent">>},
        <<"mailbox_len">> =>
        #{ type => <<"integer">>, description => <<"Process mailbox size">>},
        <<"heap_size">> =>
        #{ type => <<"integer">>, description => <<"Process heap size with the unit of byte">>},
        <<"reductions">> =>
        #{ type => <<"integer">>, description => <<"Erlang reduction">>}
    },
    ACLCacheDefinitionProperties = #{
        <<"topic">> =>
        #{ type => <<"string">>, description => <<"Topic name">>},
        <<"access">> =>
        #{ type => <<"string">>
        , enum => [<<"subscribe">>, <<"subscribe">>]
        , description => <<"Access type">>},
        <<"result">> =>
        #{ type => <<"string">>
        , enum => [<<"allow">>, <<"deny">>]
        , default => <<"allow">>
        , description => <<"Allow or deny">>},
        <<"updated_time">> =>
        #{ type => <<"integer">>, description => <<"Update time">>}
    },
    MessageDef = #{
        <<"topic">> =>
        #{ type => <<"string">>},
        <<"qos">> =>
        #{type => <<"integer">>, enum => [0, 1, 2], default => 0},
        <<"payload">> =>
        #{type => <<"string">>},
        <<"retain">> =>
        #{type => boolean, required => false, default => false}
    },
    [ {<<"client">>, ClientDef}
    , {<<"acl_cache">>, ACLCacheDefinitionProperties}
    , {<<"message">>, MessageDef}].

rest_api() ->
    [ clients_api()
    , client_api()
    , clients_acl_cache_api()
    , publish_api()
    , subscribe_api()
    , publish_batch_api()].

clients_api() ->
    Metadata = #{
        get =>
        #{tags => ["client"],
            description => "List clients",
            operationId => handle_list,
            responses => #{
                <<"200">> => #{
                    description => <<"List clients 200 OK">>,
                    content => #{
                        'application/json' =>
                        #{schema =>
                        #{type => array,
                            items => cowboy_swagger:schema(<<"client">>)}}}}}}},
    {"/clients", Metadata}.

client_api() ->
    Metadata = #{
        get =>
        #{tags => ["client"],
            description => "Get clients info by client ID",
            operationId => handle_get,
            parameters => [#{ name => clientid
                , in => path
                , required => false
                , schema =>
                #{type => string, example => 123456}
            }],
            responses => #{
                <<"404">> => emqx_mgmt_util:not_found_schema(<<"Client id not found">>),
                <<"200">> => #{
                    description => <<"Get clients 200 OK">>,
                    content => #{
                        'application/json' =>
                        #{schema => cowboy_swagger:schema(<<"client">>)}}}
            }},
        delete =>
        #{tags => ["client"],
            description => "Kick out client by client ID",
            operationId => handle_delete,
            parameters => [#{ name => clientid
                , in => path
                , required => false
                , schema =>
                #{type => string, example => 123456}
            }],
            responses => #{
                <<"404">> => emqx_mgmt_util:not_found_schema(<<"Client id not found">>),
                <<"200">> => #{description => <<"Kick out clients OK">>}
            }}
    },
    {"/clients/:clientid", Metadata}.

clients_acl_cache_api() ->
    Metadata = #{
        get =>
        #{tags => ["client"],
            description => "Get client acl cache",
            operationId => handle_get_acl_cache,
            parameters => [#{ name => clientid
                , in => path
                , required => true
                , schema =>
                #{type => string, example => <<"123456">>}
            }],
            responses => #{
                <<"404">> => emqx_mgmt_util:not_found_schema(<<"Client id not found">>),
                <<"200">> => #{
                    description => <<"List 200 OK">>,
                    content => #{
                        'application/json' =>
                        #{schema =>
                        #{type => array,
                            items => cowboy_swagger:schema(<<"client">>)}}}}}},
        delete =>
        #{tags => ["client"],
            description => "Clean client acl cache",
            operationId => handle_clean_acl_cache,
            parameters => [#{ name => clientid
                , in => path
                , required => true
                , schema =>
                #{type => string, example => <<"123456">>}
            }],
            responses => #{
                <<"404">> => emqx_mgmt_util:not_found_schema(<<"client id not found">>),
                <<"200">> => #{description => <<"Clean acl cache 200 OK">>}
            }}
    },
    {"/clients/:clientid/acl_cache", Metadata}.

publish_api() ->
    Path = "/clients/:clientid/publish",
    Metadata = #{
        post =>
        #{tags => ["client"],
            description => "publish message",
            operationId => handle_publish,
            parameters => [#{ name => clientid
                , in => path
                , required => true
                , schema =>
                #{type => string, example => <<"123456">>}}],
            requestBody => #{
                content => #{
                    'application/json' => #{schema => cowboy_swagger:schema(<<"message">>)}}},
            responses => #{
                <<"200">> => #{
                    description => <<"publish ok">>}}}},
    {Path, Metadata}.

publish_batch_api() ->
    Path = "/clients/:clientid/publish_batch",
    Metadata = #{
        post =>
        #{tags => ["client"],
            description => "publish messages",
            parameters => [
                #{name => clientid
                , in => path
                , required => true
                , schema =>
                #{type => string, example => <<"123456">>}}],
            requestBody => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => array,
                            items =>  cowboy_swagger:schema(<<"message">>)}}}},
            operationId => handle_publish_batch,
            responses => #{
                <<"200">> => #{
                    description => <<"publish ok">>}}}},
    {Path, Metadata}.

subscribe_api() ->
    Path = "/clients/:clientid/subscribe",
    Metadata = #{
        post =>
        #{tags => ["client"],
            description => "subscribe",
            operationId => handle_subscribe,
            parameters => [
                #{name => clientid
                , in => path
                , required => true
                , schema =>
                #{type => string, example => <<"123456">>}}],
            requestBody => #{
                content => #{
                    'application/json' =>
                    #{schema => #{
                        type => object,
                        properties => #{
                            <<"topic">> =>
                            #{type => <<"string">>
                                , example => <<"topic_1">>
                                , description => <<"Topic">>},
                            <<"qos">> =>
                            #{ type => <<"integer">>
                                , enum => [0, 1, 2]
                                , example => 0
                                , description => <<"QOS">>}}}}}},
            responses => #{
                <<"404">> => emqx_mgmt_util:not_found_schema(<<"Client id not found">>),
                <<"200">> => #{
                    description => <<"publish ok">>}}}},
    {Path, Metadata}.

%%%==============================================================================================
%% parameters trans
handle_list(_Request) ->
%%    TODO: list
    list(#{}).

handle_get(Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    lookup(#{clientid => ClientID}).

handle_delete(Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    kickout(#{clientid => ClientID}).

handle_get_acl_cache(Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    get_acl_cache(#{clientid => ClientID}).

handle_clean_acl_cache(Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    clean_acl_cache(#{clientid => ClientID}).

handle_publish(Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    Message = emqx_json:decode(Body, [return_maps]),
    Qos = maps:get(<<"qos">>, Message, 0),
    Topic = maps:get(<<"topic">>, Message),
    Payload = maps:get(<<"payload">>, Message),
    Retain = maps:get(retain, Message, false),
    publish(#{clientid => ClientID, qos => Qos, topic => Topic, payload => Payload, retain => Retain}).

handle_publish_batch(Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    Messages0 = emqx_json:decode(Body, [return_maps]),
    Messages =
        [begin
             Topic = maps:get(<<"topic">>, Message0),
             Qos = maps:get(<<"qos">>, Message0, 0),
             Payload = maps:get(<<"payload">>, Message0),
             Retain = maps:get(retain, Message0, false),
             #{clientid => ClientID, topic => Topic, qos => Qos, payload => Payload, retain => Retain}
         end || Message0 <- Messages0],
    publish_batch(#{clientid => ClientID, messages => Messages}).

handle_subscribe(Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    TopicInfo = emqx_json:decode(Body, [return_maps]),
    Topic = maps:get(<<"topic">>, TopicInfo),
    Qos = maps:get(<<"qos">>, TopicInfo, 0),
    subscribe(#{clientid => ClientID, topic => Topic, qos => Qos}).

handle_subscribe_batch(Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    TopicInfos = emqx_json:decode(Body, [return_maps]),
    Topics =
        [begin
            Topic = maps:get(<<"topic">>, TopicInfo),
            Qos = maps:get(<<"qos">>, TopicInfo, 0),
            #{topic => Topic, qos => Qos}
        end || TopicInfo <- TopicInfos],
    subscribe_batch(#{clientid => ClientID, topics => Topics}).

%%%==============================================================================================
%% api apply

list(Params) ->
    emqx_mgmt_api:cluster_query(Params, ?CLIENT_QS_SCHEMA, ?query_fun).

lookup(#{clientid := ClientID}) ->
    case emqx_mgmt:lookup_client({clientid, ClientID}, ?format_fun) of
        [] ->
            {404, ?CLIENT_ID_NOT_FOUND};
        ClientInfo ->
            Response = emqx_json:encode(hd(ClientInfo)),
            {ok, Response}
    end.

kickout(#{clientid := ClientID}) ->
    emqx_mgmt:kickout_client(ClientID),
    {ok}.

get_acl_cache(#{clientid := ClientID})->
    case emqx_mgmt:list_acl_cache(ClientID) of
        {error, not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            {500, #{code => <<"UNKNOW_ERROR">>, reason => io_lib:format("~p", [Reason])}};
        Caches ->
            Response = emqx_json:encode([format_acl_cache(Cache) || Cache <- Caches]),
            {ok, Response}
    end.

clean_acl_cache(#{clientid := ClientID}) ->
    case emqx_mgmt:clean_acl_cache(ClientID) of
        ok ->
            ok;
        {error, not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            {500, #{code => <<"UNKNOW_ERROR">>, reason => io_lib:format("~p", [Reason])}}
    end.

publish(#{clientid := ClientID, qos := Qos, topic := Topic, payload := Payload, retain := Retain}) ->
    case do_publish(ClientID, Qos, Topic, Payload, Retain) of
        ok ->
            {ok};
        {error ,Reason} ->
            Body = emqx_json:encode(#{code => "UN_KNOW_ERROR", reason => io_lib:format("~p", [Reason])}),
            {500, Body}
    end.

publish_batch(#{clientid := ClientID, messages := Messages}) ->
    ArgsList = [[ClientID, Qos, Topic,  Payload, Retain]
        || #{qos := Qos, topic := Topic, payload := Payload, retain := Retain} <- Messages],
    Data = emqx_mgmt_util:batch_operation(?MODULE, do_publish, ArgsList),
    Body = emqx_json:encode(Data),
    {ok, Body}.

subscribe(#{clientid := ClientID, topic := Topic, qos := Qos}) ->
    case do_subscribe(ClientID, Topic, Qos) of
        {error, channel_not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            Body = emqx_json:encode(#{code => <<"UNKNOW_ERROR">>, reason => io_lib:format("~p", [Reason])}),
            {ok, Body};
        ok ->
            {ok}
    end.

subscribe_batch(#{clientid := ClientID, topics := Topics}) ->
    ArgList = [[ClientID, Topic, Qos]|| #{topic := Topic, qos := Qos} <- Topics],
    emqx_mgmt_util:batch_operation(?MODULE, do_subscribe, ArgList).

%%%==============================================================================================
%% util function
format_channel_info({_, ClientInfo, ClientStats}) ->
    Fun =
        fun
            (_Key, Value, Current) when is_map(Value) ->
                maps:merge(Current, Value);
            (Key, Value, Current) ->
                maps:put(Key, Value, Current)
        end,
    StatsMap = maps:without([memory, next_pkt_id, total_heap_size],
        maps:from_list(ClientStats)),
    ClientInfoMap0 = maps:fold(Fun, #{}, ClientInfo),
    IpAddress      = peer_to_binary(maps:get(peername, ClientInfoMap0)),
    Connected      = maps:get(conn_state, ClientInfoMap0) =:= connected,
    ClientInfoMap1 = maps:merge(StatsMap, ClientInfoMap0),
    ClientInfoMap2 = maps:put(node, node(), ClientInfoMap1),
    ClientInfoMap3 = maps:put(ip_address, IpAddress, ClientInfoMap2),
    ClientInfoMap  = maps:put(connected, Connected, ClientInfoMap3),
    RemoveList = [auth_result, peername, sockname, peerhost
                , conn_state, send_pend, conn_props, peercert
                , sockstate, receive_maximum, protocol
                , is_superuser, sockport, anonymous, mountpoint
                , socktype, active_n, await_rel_timeout
                , conn_mod, sockname, retry_interval ,upgrade_qos],
    maps:without(RemoveList, ClientInfoMap).

peer_to_binary({Addr, Port}) ->
    AddrBinary = list_to_binary(inet:ntoa(Addr)),
    PortBinary = integer_to_binary(Port),
    <<AddrBinary/binary, ":", PortBinary/binary>>;
peer_to_binary(Addr) ->
    list_to_binary(inet:ntoa(Addr)).

format_acl_cache({{PubSub, Topic}, {AclResult, Timestamp}}) ->
    #{access => PubSub,
        topic => Topic,
        result => AclResult,
        updated_time => Timestamp}.

do_publish(ClientID, Qos, Topic, Payload, Retain) ->
    Message = emqx_message:make(ClientID, Qos, Topic, Payload),
    PublishResult = emqx_mgmt:publish(Message#message{flags = #{retain => Retain}}),
    case PublishResult of
        [] ->
            ok;
        [{_, _ , {ok, _}} | _] ->
            ok;
        [{_, _ , {error, Reason}} | _] ->
            {error, Reason}
    end.

do_subscribe(ClientID, Topic0, Qos) ->
    {Topic, Opts} = emqx_topic:parse(Topic0),
    TopicTable = [{Topic, Opts#{qos => Qos}}],
    emqx_mgmt:subscribe(ClientID, TopicTable),
    case emqx_mgmt:subscribe(ClientID, TopicTable) of
        {error, Reason} ->
            {error, Reason};
        {subscribe, Subscriptions} ->
            case proplists:is_defined(Topic, Subscriptions) of
                true ->
                    ok;
                false ->
                    {error, unknow_error}
            end
    end.
