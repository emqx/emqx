%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
-module(emqx_gateway_api_client).

-behaviour(minirest_api).

%% minirest behaviour callbacks
-export([api_spec/0]).

%% http handlers
-export([ clients/2
        , clients_insta/2
        , subscriptions/2
        ]).

%% internal exports (for client query)
-export([ query/4
        , format_channel_info/1
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

api_spec() ->
    {metadata(apis()), []}.

apis() ->
    [ {"/gateway/:name/clients", clients}
    , {"/gateway/:name/clients/:clientid", clients_insta}
    , {"/gateway/:name/clients/:clientid/subscriptions", subscriptions}
    , {"/gateway/:name/clients/:clientid/subscriptions/:topic", subscriptions}
    ].


-define(CLIENT_QS_SCHEMA,
    [ {<<"node">>, atom}
    , {<<"clientid">>, binary}
    , {<<"username">>, binary}
    %%, {<<"zone">>, atom}
    , {<<"ip_address">>, ip}
    , {<<"conn_state">>, atom}
    , {<<"clean_start">>, atom}
    %%, {<<"proto_name">>, binary}
    %%, {<<"proto_ver">>, integer}
    , {<<"like_clientid">>, binary}
    , {<<"like_username">>, binary}
    , {<<"gte_created_at">>, timestamp}
    , {<<"lte_created_at">>, timestamp}
    , {<<"gte_connected_at">>, timestamp}
    , {<<"lte_connected_at">>, timestamp}
    ]).

-define(query_fun, {?MODULE, query}).
-define(format_fun, {?MODULE, format_channel_info}).

clients(get, #{ bindings := #{name := GwName0}
              , query_string := Qs
              }) ->
    GwName = binary_to_existing_atom(GwName0),
    TabName = emqx_gateway_cm:tabname(info, GwName),
    case maps:get(<<"node">>, Qs, undefined) of
        undefined ->
            Response = emqx_mgmt_api:cluster_query(
                         Qs, TabName,
                         ?CLIENT_QS_SCHEMA, ?query_fun
                        ),
            {200, Response};
        Node1 ->
            Node = binary_to_atom(Node1, utf8),
            ParamsWithoutNode = maps:without([<<"node">>], Qs),
            Response = emqx_mgmt_api:node_query(
                         Node, ParamsWithoutNode,
                         TabName, ?CLIENT_QS_SCHEMA, ?query_fun
                        ),
            {200, Response}
    end.

clients_insta(get, _Req) ->
    {200, <<"{}">>};
clients_insta(delete, _Req) ->
    {200}.

subscriptions(get, _Req) ->
    {200, []};
subscriptions(delete, _Req) ->
    {200}.

%%--------------------------------------------------------------------
%% query funcs

query(Tab, {Qs, []}, Start, Limit) ->
    Ms = qs2ms(Qs),
    emqx_mgmt_api:select_table(Tab, Ms, Start, Limit,
                               fun format_channel_info/1);

query(Tab, {Qs, Fuzzy}, Start, Limit) ->
    Ms = qs2ms(Qs),
    MatchFun = match_fun(Ms, Fuzzy),
    emqx_mgmt_api:traverse_table(Tab, MatchFun, Start, Limit,
                                 fun format_channel_info/1).

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

%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% format funcs

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
        , subscriptions
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

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

metadata(APIs) ->
    metadata(APIs, []).
metadata([], APIAcc) ->
    lists:reverse(APIAcc);
metadata([{Path, Fun}|More], APIAcc) ->
    Methods = [get, post, put, delete, patch],
    Mds = lists:foldl(fun(M, Acc) ->
              try
                  Acc#{M => swagger(Path, M)}
              catch
                  error : function_clause ->
                      Acc
              end
          end, #{}, Methods),
    metadata(More, [{Path, Mds, Fun} | APIAcc]).

swagger("/gateway/:name/clients", get) ->
    #{ description => <<"Get the gateway clients">>
     , parameters => params_client_query()
     , responses =>
        #{ <<"404">> => schema_not_found()
         , <<"200">> => schema_clients_list()
       }
     };
swagger("/gateway/:name/clients/:clientid", get) ->
    #{ description => <<"Get the gateway client infomation">>
     , parameters => params_client_insta()
     , responses =>
        #{ <<"404">> => schema_not_found()
         , <<"200">> => schema_client()
       }
     };
swagger("/gateway/:name/clients/:clientid", delete) ->
    #{ description => <<"Kick out the gateway client">>
     , parameters => params_client_insta()
     , responses =>
        #{ <<"404">> => schema_not_found()
         , <<"204">> => schema_no_content()
       }
     };
swagger("/gateway/:name/clients/:clientid/subscriptions", get) ->
    #{ description => <<"Get the gateway client subscriptions">>
     , parameters => params_client_insta()
     , responses =>
        #{ <<"404">> => schema_not_found()
         , <<"200">> => schema_subscription_list()
       }
     };
swagger("/gateway/:name/clients/:clientid/subscriptions", post) ->
    #{ description => <<"Get the gateway client subscriptions">>
     , parameters => params_client_insta()
     , requestBody => schema_subscription()
     , responses =>
        #{ <<"404">> => schema_not_found()
         , <<"200">> => schema_no_content()
       }
     };
swagger("/gateway/:name/clients/:clientid/subscriptions/:topic", delete) ->
    #{ description => <<"Unsubscribe the topic for client">>
     , parameters => params_topic_name_in_path() ++ params_client_insta()
     , responses =>
        #{ <<"404">> => schema_not_found()
         , <<"204">> => schema_no_content()
       }
     }.

params_client_query() ->
    params_gateway_name_in_path()
    ++ params_client_searching_in_qs()
    ++ emqx_mgmt_util:page_params().

params_client_insta() ->
    params_clientid_in_path()
    ++ params_gateway_name_in_path().

params_client_searching_in_qs() ->
    queries(
      [ {node, string}
      , {clientid, string}
      , {username, string}
      , {ip_address, string}
      , {conn_state, string}
      , {clean_start, boolean}
      , {like_clientid, string}
      , {like_username, string}
      , {gte_created_at, string}
      , {lte_created_at, string}
      , {gte_connected_at, string}
      , {lte_connected_at, string}
    ]).

params_gateway_name_in_path() ->
    [#{ name => name
      , in => path
      , schema => #{type => string}
      , required => true
      }].

params_clientid_in_path() ->
    [#{ name => clientid
      , in => path
      , schema => #{type => string}
      , required => true
      }].

params_topic_name_in_path() ->
    [#{ name => topic
      , in => path
      , schema => #{type => string}
      , required => true
      }].

queries(Ls) ->
    lists:map(fun({K, Type}) ->
        #{name => K, in => query,
          schema => #{type => Type},
          required => false
         }
    end, Ls).

%%--------------------------------------------------------------------
%% Schemas

schema_not_found() ->
    emqx_mgmt_util:error_schema(<<"Gateway not found or unloaded">>).

schema_no_content() ->
    #{description => <<"No Content">>}.

schema_clients_list() ->
    emqx_mgmt_util:page_schema(
      #{ type => object
       , properties => properties_client()
       }
     ).

schema_client() ->
    emqx_mgmt_util:schema(
    #{ type => object
     , properties => properties_client()
     }).

schema_subscription_list() ->
    emqx_mgmt_util:array_schema(
      #{ type => object
       , properties => properties_subscription()
       },
      <<"Client subscriptions">>
     ).

schema_subscription() ->
    emqx_mgmt_util:schema(
      #{ type => object
       , properties => properties_subscription()
       }
     ).

%%--------------------------------------------------------------------
%% Object properties def

properties_client() ->
    emqx_mgmt_util:properties(
      [ {node, string,
         <<"Name of the node to which the client is connected">>}
      , {clientid, string,
         <<"Client identifier">>}
      , {username, string,
         <<"Username of client when connecting">>}
      , {proto_name, string,
         <<"Client protocol name">>}
      , {proto_ver, string,
         <<"Protocol version used by the client">>}
      , {ip_address, string,
         <<"Client's IP address">>}
      , {is_bridge, boolean,
         <<"Indicates whether the client is connectedvia bridge">>}
      , {connected_at, string,
         <<"Client connection time">>}
      , {disconnected_at, string,
         <<"Client offline time, This field is only valid and returned "
            "when connected is false">>}
      , {connected, boolean,
         <<"Whether the client is connected">>}
      %, {will_msg, string,
      %   <<"Client will message">>}
      %, {zone, string,
      %   <<"Indicate the configuration group used by the client">>}
      , {keepalive, integer,
         <<"keepalive time, with the unit of second">>}
      , {clean_start, boolean,
         <<"Indicate whether the client is using a brand new session">>}
      , {expiry_interval, integer,
         <<"Session expiration interval, with the unit of second">>}
      , {created_at, string,
         <<"Session creation time">>}
      , {subscriptions_cnt, integer,
         <<"Number of subscriptions established by this client">>}
      , {subscriptions_max, integer,
         <<"v4 api name [max_subscriptions] Maximum number of "
           "subscriptions allowed by this client">>}
      , {inflight_cnt, integer,
         <<"Current length of inflight">>}
      , {inflight_max, integer,
         <<"v4 api name [max_inflight]. Maximum length of inflight">>}
      , {mqueue_len, integer,
         <<"Current length of message queue">>}
      , {mqueue_max, integer,
         <<"v4 api name [max_mqueue]. Maximum length of message queue">>}
      , {mqueue_dropped, integer,
         <<"Number of messages dropped by the message queue due to "
           "exceeding the length">>}
      , {awaiting_rel_cnt, integer,
         <<"v4 api name [awaiting_rel] Number of awaiting PUBREC packet">>}
      , {awaiting_rel_max, integer,
         <<"v4 api name [max_awaiting_rel]. Maximum allowed number of "
           "awaiting PUBREC packet">>}
      , {recv_oct, integer,
         <<"Number of bytes received by EMQ X Broker (the same below)">>}
      , {recv_cnt, integer,
         <<"Number of TCP packets received">>}
      , {recv_pkt, integer,
         <<"Number of MQTT packets received">>}
      , {recv_msg, integer,
         <<"Number of PUBLISH packets received">>}
      , {send_oct, integer,
         <<"Number of bytes sent">>}
      , {send_cnt, integer,
         <<"Number of TCP packets sent">>}
      , {send_pkt, integer,
         <<"Number of MQTT packets sent">>}
      , {send_msg, integer,
         <<"Number of PUBLISH packets sent">>}
      , {mailbox_len, integer,
         <<"Process mailbox size">>}
      , {heap_size, integer,
         <<"Process heap size with the unit of byte">>}
      , {reductions, integer,
         <<"Erlang reduction">>}
      ]).

properties_subscription() ->
    emqx_mgmt_util:properties(
     [ {topic, string,
        <<"Topic Fillter">>}
     , {qos, integer,
        <<"QoS level">>}
     ]).
