%%--------------------------------------------------------------------
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
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_clients).

-behavior(minirest_api).

-include_lib("emqx/include/emqx.hrl").

-include_lib("emqx/include/logger.hrl").

-include("emqx_mgmt.hrl").

%% API
-export([api_spec/0]).

-export([ clients/2
        , client/2
        , acl_cache/2
        , subscribe/2
        , subscribe_batch/2]).

-export([ query/3
        , format_channel_info/1]).

%% for batch operation
-export([do_subscribe/3]).

-define(CLIENT_QS_SCHEMA, {emqx_channel_info,
    [ {<<"clientid">>, binary}
    , {<<"username">>, binary}
    , {<<"zone">>, atom}
    , {<<"ip_address">>, ip}
    , {<<"conn_state">>, atom}
    , {<<"clean_start">>, atom}
    , {<<"proto_name">>, binary}
    , {<<"proto_ver">>, integer}
    , {<<"like_clientid">>, binary}
    , {<<"like_username">>, binary}
    , {<<"gte_created_at">>, timestamp}
    , {<<"lte_created_at">>, timestamp}
    , {<<"gte_connected_at">>, timestamp}
    , {<<"lte_connected_at">>, timestamp}]}).

-define(query_fun, {?MODULE, query}).
-define(format_fun, {?MODULE, format_channel_info}).

-define(CLIENT_ID_NOT_FOUND,
    <<"{\"code\": \"RESOURCE_NOT_FOUND\", \"reason\": \"Client id not found\"}">>).

api_spec() ->
    {apis(), schemas()}.

apis() ->
    [ clients_api()
    , client_api()
    , clients_acl_cache_api()
    , subscribe_api()].

schemas() ->
    Client = #{
        client => #{
            type => object,
            properties => #{
                node => #{
                    type => string,
                    description => <<"Name of the node to which the client is connected">>},
                clientid => #{
                    type => string,
                    description => <<"Client identifier">>},
                username => #{
                    type => string,
                    description => <<"User name of client when connecting">>},
                proto_name => #{
                    type => string,
                    description => <<"Client protocol name">>},
                proto_ver => #{
                    type => integer,
                    description => <<"Protocol version used by the client">>},
                ip_address => #{
                    type => string,
                    description => <<"Client's IP address">>},
                is_bridge => #{
                    type => boolean,
                    description => <<"Indicates whether the client is connectedvia bridge">>},
                connected_at => #{
                    type => string,
                    description => <<"Client connection time">>},
                disconnected_at => #{
                    type => string,
                    description => <<"Client offline time, This field is only valid and returned when connected is false">>},
                connected => #{
                    type => boolean,
                    description => <<"Whether the client is connected">>},
                will_msg => #{
                    type => string,
                    description => <<"Client will message">>},
                zone => #{
                    type => string,
                    description => <<"Indicate the configuration group used by the client">>},
                keepalive => #{
                    type => integer,
                    description => <<"keepalive time, with the unit of second">>},
                clean_start => #{
                    type => boolean,
                    description => <<"Indicate whether the client is using a brand new session">>},
                expiry_interval => #{
                    type => integer,
                    description => <<"Session expiration interval, with the unit of second">>},
                created_at => #{
                    type => string,
                    description => <<"Session creation time">>},
                subscriptions_cnt => #{
                    type => integer,
                    description => <<"Number of subscriptions established by this client.">>},
                subscriptions_max => #{
                    type => integer,
                    description => <<"v4 api name [max_subscriptions] Maximum number of subscriptions allowed by this client">>},
                inflight_cnt => #{
                    type => integer,
                    description => <<"Current length of inflight">>},
                inflight_max => #{
                    type => integer,
                    description => <<"v4 api name [max_inflight]. Maximum length of inflight">>},
                mqueue_len => #{
                    type => integer,
                    description => <<"Current length of message queue">>},
                mqueue_max => #{
                    type => integer,
                    description => <<"v4 api name [max_mqueue]. Maximum length of message queue">>},
                mqueue_dropped => #{
                    type => integer,
                    description => <<"Number of messages dropped by the message queue due to exceeding the length">>},
                awaiting_rel_cnt => #{
                    type => integer,
                    description => <<"v4 api name [awaiting_rel] Number of awaiting PUBREC packet">>},
                awaiting_rel_max => #{
                    type => integer,
                    description => <<"v4 api name [max_awaiting_rel]. Maximum allowed number of awaiting PUBREC packet">>},
                recv_oct => #{
                    type => integer,
                    description => <<"Number of bytes received by EMQ X Broker (the same below)">>},
                recv_cnt => #{
                    type => integer,
                    description => <<"Number of TCP packets received">>},
                recv_pkt => #{
                    type => integer,
                    description => <<"Number of MQTT packets received">>},
                recv_msg => #{
                    type => integer,
                    description => <<"Number of PUBLISH packets received">>},
                send_oct => #{
                    type => integer,
                    description => <<"Number of bytes sent">>},
                send_cnt => #{
                    type => integer,
                    description => <<"Number of TCP packets sent">>},
                send_pkt => #{
                    type => integer,
                    description => <<"Number of MQTT packets sent">>},
                send_msg => #{
                    type => integer,
                    description => <<"Number of PUBLISH packets sent">>},
                mailbox_len => #{
                    type => integer,
                    description => <<"Process mailbox size">>},
                heap_size => #{
                    type => integer,
                    description => <<"Process heap size with the unit of byte">>
                },
                reductions => #{
                    type => integer,
                    description => <<"Erlang reduction">>}
            }
        }
       },
    AclCache = #{
        acl_cache => #{
            type => object,
            properties => #{
                topic => #{
                    type => string,
                    description => <<"Topic name">>},
                access => #{
                    type => string,
                    enum => [<<"subscribe">>, <<"publish">>],
                    description => <<"Access type">>},
                result => #{
                    type => string,
                    enum => [<<"allow">>, <<"deny">>],
                    default => <<"allow">>,
                    description => <<"Allow or deny">>},
                updated_time => #{
                    type => integer,
                    description => <<"Update time">>}
            }
        }
    },
    [Client, AclCache].

clients_api() ->
    Metadata = #{
        get => #{
            description => "List clients",
            responses => #{
                <<"200">> => emqx_mgmt_util:response_array_schema(<<"List clients 200 OK">>, <<"client">>)}}},
    {"/clients", Metadata, clients}.

client_api() ->
    Metadata = #{
        get => #{
            description => "Get clients info by client ID",
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true,
                example => 123456}],
            responses => #{
                <<"404">> => emqx_mgmt_util:response_error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:response_schema(<<"List clients 200 OK">>, <<"client">>)}},
        delete => #{
            description => "Kick out client by client ID",
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true,
                example => 123456}],
            responses => #{
                <<"404">> => emqx_mgmt_util:response_error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:response_schema(<<"List clients 200 OK">>, <<"client">>)}}},
    {"/clients/:clientid", Metadata, client}.

clients_acl_cache_api() ->
    Metadata = #{
        get => #{
            description => "Get client acl cache",
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true,
                example => 123456}],
            responses => #{
                <<"404">> => emqx_mgmt_util:response_error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:response_schema(<<"List clients 200 OK">>, <<"acl_cache">>)}},
        delete => #{
            description => "Clean client acl cache",
            parameters => [#{
                name => clientid,
                in => path,
                schema => #{type => string},
                required => true,
                example => 123456}],
            responses => #{
                <<"404">> => emqx_mgmt_util:response_error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:response_schema(<<"Delete clients 200 OK">>)}}},
    {"/clients/:clientid/acl_cache", Metadata, acl_cache}.

subscribe_api() ->
    Metadata = #{
        post => #{
            description => "subscribe",
            parameters => [
                #{
                    name => clientid,
                    in => path,
                    schema => #{type => string},
                    required => true,
                    example => 123456
                }
            ],
            'requestBody' => emqx_mgmt_util:request_body_schema(#{
                type => object,
                properties => #{
                    <<"topic">> => #{
                        type => string,
                        example => <<"topic_1">>,
                        description => <<"Topic">>},
                    <<"qos">> => #{
                        type => integer,
                        enum => [0, 1, 2],
                        example => 0,
                        description => <<"QoS">>}}}),
            responses => #{
                <<"404">> => emqx_mgmt_util:response_error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:response_schema(<<"subscribe ok">>)}},
        delete => #{
            description => "unsubscribe",
            parameters => [
                #{
                    name => clientid,
                    in => path,
                    schema => #{type => string},
                    required => true,
                    example => 123456
                },
                #{
                    name => topic,
                    in => query,
                    schema => #{type => string},
                    required => true,
                    example => <<"topic_1">>
                }
            ],
            responses => #{
                <<"404">> => emqx_mgmt_util:response_error_schema(<<"Client id not found">>),
                <<"200">> => emqx_mgmt_util:response_schema(<<"unsubscribe ok">>)}}},
    {"/clients/:clientid/subscribe", Metadata, subscribe}.

%%%==============================================================================================
%% parameters trans
clients(get, _Request) ->
    list(#{}).

client(get, Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    lookup(#{clientid => ClientID});

client(delete, Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    kickout(#{clientid => ClientID}).

acl_cache(get, Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    get_acl_cache(#{clientid => ClientID});

acl_cache(delete, Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    clean_acl_cache(#{clientid => ClientID}).

subscribe(post, Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    TopicInfo = emqx_json:decode(Body, [return_maps]),
    Topic = maps:get(<<"topic">>, TopicInfo),
    Qos = maps:get(<<"qos">>, TopicInfo, 0),
    subscribe(#{clientid => ClientID, topic => Topic, qos => Qos});

subscribe(delete, Request) ->
    ClientID = cowboy_req:binding(clientid, Request),
    #{topic := Topic} = cowboy_req:match_qs([topic], Request),
    unsubscribe(#{clientid => ClientID, topic => Topic}).

%% TODO: batch
subscribe_batch(post, Request) ->
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
    Data = emqx_mgmt_api:cluster_query(maps:to_list(Params), ?CLIENT_QS_SCHEMA, ?query_fun),
    Body = emqx_json:encode(Data),
    {200, Body}.

lookup(#{clientid := ClientID}) ->
    case emqx_mgmt:lookup_client({clientid, ClientID}, ?format_fun) of
        [] ->
            {404, ?CLIENT_ID_NOT_FOUND};
        ClientInfo ->
            Response = emqx_json:encode(hd(ClientInfo)),
            {200, Response}
    end.

kickout(#{clientid := ClientID}) ->
    emqx_mgmt:kickout_client(ClientID),
    {200}.

get_acl_cache(#{clientid := ClientID})->
    case emqx_mgmt:list_acl_cache(ClientID) of
        {error, not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            {500, #{code => <<"UNKNOW_ERROR">>, reason => io_lib:format("~p", [Reason])}};
        Caches ->
            Response = emqx_json:encode([format_acl_cache(Cache) || Cache <- Caches]),
            {200, Response}
    end.

clean_acl_cache(#{clientid := ClientID}) ->
    case emqx_mgmt:clean_acl_cache(ClientID) of
        ok ->
            {200};
        {error, not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            {500, #{code => <<"UNKNOW_ERROR">>, reason => io_lib:format("~p", [Reason])}}
    end.

subscribe(#{clientid := ClientID, topic := Topic, qos := Qos}) ->
    case do_subscribe(ClientID, Topic, Qos) of
        {error, channel_not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            Body = emqx_json:encode(#{code => <<"UNKNOW_ERROR">>, reason => io_lib:format("~p", [Reason])}),
            {500, Body};
        ok ->
            {200}
    end.

unsubscribe(#{clientid := ClientID, topic := Topic}) ->
    case do_unsubscribe(ClientID, Topic) of
        {error, channel_not_found} ->
            {404, ?CLIENT_ID_NOT_FOUND};
        {error, Reason} ->
            Body = emqx_json:encode(#{code => <<"UNKNOW_ERROR">>, reason => io_lib:format("~p", [Reason])}),
            {500, Body};
        {unsubscribe, [{Topic, #{}}]} ->
            {200}
    end.

subscribe_batch(#{clientid := ClientID, topics := Topics}) ->
    ArgList = [[ClientID, Topic, Qos]|| #{topic := Topic, qos := Qos} <- Topics],
    emqx_mgmt_util:batch_operation(?MODULE, do_subscribe, ArgList).

%%%==============================================================================================
%% internal function
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
    RemoveList = [
        auth_result
        , peername
        , sockname
        , peerhost
        , conn_state
        , send_pend
        , conn_props
        , peercert
        , sockstate
        , receive_maximum
        , protocol
        , is_superuser
        , sockport
        , anonymous
        , mountpoint
        , socktype
        , active_n
        , await_rel_timeout
        , conn_mod
        , sockname
        , retry_interval
        , upgrade_qos
    ],
    maps:without(RemoveList, ClientInfoMap).

peer_to_binary({Addr, Port}) ->
    AddrBinary = list_to_binary(inet:ntoa(Addr)),
    PortBinary = integer_to_binary(Port),
    <<AddrBinary/binary, ":", PortBinary/binary>>;
peer_to_binary(Addr) ->
    list_to_binary(inet:ntoa(Addr)).

format_acl_cache({{PubSub, Topic}, {AclResult, Timestamp}}) ->
    #{
        access => PubSub,
        topic => Topic,
        result => AclResult,
        updated_time => Timestamp
    }.

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

do_unsubscribe(ClientID, Topic) ->
    case emqx_mgmt:unsubscribe(ClientID, Topic) of
        {error, Reason} ->
            {error, Reason};
        Res ->
            Res
    end.
%%%==============================================================================================
%% Query Functions

query({Qs, []}, Start, Limit) ->
    Ms = qs2ms(Qs),
    emqx_mgmt_api:select_table(emqx_channel_info, Ms, Start, Limit, fun format_channel_info/1);

query({Qs, Fuzzy}, Start, Limit) ->
    Ms = qs2ms(Qs),
    MatchFun = match_fun(Ms, Fuzzy),
    emqx_mgmt_api:traverse_table(emqx_channel_info, MatchFun, Start, Limit, fun format_channel_info/1).

%%%==============================================================================================
%% QueryString to Match Spec
-spec qs2ms(list()) -> ets:match_spec().
qs2ms(Qs) ->
    {MtchHead, Conds} = qs2ms(Qs, 2, {#{}, []}),
    [{{'$1', MtchHead, '_'}, Conds, ['$_']}].

qs2ms([], _, {MtchHead, Conds}) ->
    {MtchHead, lists:reverse(Conds)};

qs2ms([{Key, '=:=', Value} | Rest], N, {MtchHead, Conds}) ->
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(Key, Value)),
    qs2ms(Rest, N, {NMtchHead, Conds});
qs2ms([Qs | Rest], N, {MtchHead, Conds}) ->
    Holder = binary_to_atom(iolist_to_binary(["$", integer_to_list(N)]), utf8),
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(element(1, Qs), Holder)),
    NConds = put_conds(Qs, Holder, Conds),
    qs2ms(Rest, N+1, {NMtchHead, NConds}).

put_conds({_, Op, V}, Holder, Conds) ->
    [{Op, Holder, V} | Conds];
put_conds({_, Op1, V1, Op2, V2}, Holder, Conds) ->
    [{Op2, Holder, V2},
        {Op1, Holder, V1} | Conds].

ms(clientid, X) ->
    #{clientinfo => #{clientid => X}};
ms(username, X) ->
    #{clientinfo => #{username => X}};
ms(zone, X) ->
    #{clientinfo => #{zone => X}};
ms(ip_address, X) ->
    #{clientinfo => #{peerhost => X}};
ms(conn_state, X) ->
    #{conn_state => X};
ms(clean_start, X) ->
    #{conninfo => #{clean_start => X}};
ms(proto_name, X) ->
    #{conninfo => #{proto_name => X}};
ms(proto_ver, X) ->
    #{conninfo => #{proto_ver => X}};
ms(connected_at, X) ->
    #{conninfo => #{connected_at => X}};
ms(created_at, X) ->
    #{session => #{created_at => X}}.

%%%==============================================================================================
%% Match funcs
match_fun(Ms, Fuzzy) ->
    MsC = ets:match_spec_compile(Ms),
    REFuzzy = lists:map(fun({K, like, S}) ->
        {ok, RE} = re:compile(S),
        {K, like, RE}
                        end, Fuzzy),
    fun(Rows) ->
        case ets:match_spec_run(Rows, MsC) of
            [] -> [];
            Ls ->
                lists:filter(fun(E) ->
                    run_fuzzy_match(E, REFuzzy)
                             end, Ls)
        end
    end.

run_fuzzy_match(_, []) ->
    true;
run_fuzzy_match(E = {_, #{clientinfo := ClientInfo}, _}, [{Key, _, RE}|Fuzzy]) ->
    Val = case maps:get(Key, ClientInfo, "") of
              undefined -> "";
              V -> V
          end,
    re:run(Val, RE, [{capture, none}]) == match andalso run_fuzzy_match(E, Fuzzy).
