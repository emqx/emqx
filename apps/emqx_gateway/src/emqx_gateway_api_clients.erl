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

-module(emqx_gateway_api_clients).

-behaviour(minirest_api).

-include_lib("emqx/include/logger.hrl").

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

-import(emqx_gateway_http,
        [ return_http_error/2
        , with_gateway/2
        , schema_bad_request/0
        , schema_not_found/0
        , schema_internal_error/0
        , schema_no_content/0
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
    , {<<"ip_address">>, ip}
    , {<<"conn_state">>, atom}
    , {<<"clean_start">>, atom}
    , {<<"proto_ver">>, integer}
    , {<<"like_clientid">>, binary}
    , {<<"like_username">>, binary}
    , {<<"gte_created_at">>, timestamp}
    , {<<"lte_created_at">>, timestamp}
    , {<<"gte_connected_at">>, timestamp}
    , {<<"lte_connected_at">>, timestamp}
    %% special keys for lwm2m protocol
    , {<<"endpoint_name">>, binary}
    , {<<"like_endpoint_name">>, binary}
    , {<<"gte_lifetime">>, timestamp}
    , {<<"lte_lifetime">>, timestamp}
    ]).

-define(query_fun, {?MODULE, query}).
-define(format_fun, {?MODULE, format_channel_info}).

clients(get, #{ bindings := #{name := Name0}
              , query_string := Params
              }) ->
    with_gateway(Name0, fun(GwName, _) ->
        TabName = emqx_gateway_cm:tabname(info, GwName),
        case maps:get(<<"node">>, Params, undefined) of
            undefined ->
                Response = emqx_mgmt_api:cluster_query(Params, TabName,
                                                       ?CLIENT_QS_SCHEMA, ?query_fun),
                emqx_mgmt_util:generate_response(Response);
            Node1 ->
                Node = binary_to_atom(Node1, utf8),
                ParamsWithoutNode = maps:without([<<"node">>], Params),
                Response = emqx_mgmt_api:node_query(Node, ParamsWithoutNode,
                                                    TabName, ?CLIENT_QS_SCHEMA, ?query_fun),
                emqx_mgmt_util:generate_response(Response)
        end
    end).

clients_insta(get, #{ bindings := #{name := Name0,
                                    clientid := ClientId0}
                    }) ->
    ClientId = emqx_mgmt_util:urldecode(ClientId0),
    with_gateway(Name0, fun(GwName, _) ->
        case emqx_gateway_http:lookup_client(GwName, ClientId,
                                             {?MODULE, format_channel_info}) of
            [ClientInfo] ->
                {200, ClientInfo};
            [ClientInfo | _More] ->
                ?LOG(warning, "More than one client info was returned on ~ts",
                              [ClientId]),
                {200, ClientInfo};
            [] ->
                return_http_error(404, "Client not found")
        end
    end);
clients_insta(delete, #{ bindings := #{name := Name0,
                                       clientid := ClientId0}
                       }) ->
    ClientId = emqx_mgmt_util:urldecode(ClientId0),
    with_gateway(Name0, fun(GwName, _) ->
        _ = emqx_gateway_http:kickout_client(GwName, ClientId),
        {204}
    end).

%% FIXME:
%% List the subscription without mountpoint, but has SubOpts,
%% for example, share group ...
subscriptions(get, #{ bindings := #{name := Name0,
                                    clientid := ClientId0}
                    }) ->
    ClientId = emqx_mgmt_util:urldecode(ClientId0),
    with_gateway(Name0, fun(GwName, _) ->
        case emqx_gateway_http:list_client_subscriptions(GwName, ClientId) of
            {error, Reason} ->
                return_http_error(500, Reason);
            {ok, Subs} ->
                {200, Subs}
        end
    end);

%% Create the subscription without mountpoint
subscriptions(post, #{ bindings := #{name := Name0,
                                     clientid := ClientId0},
                       body := Body
                    }) ->
    ClientId = emqx_mgmt_util:urldecode(ClientId0),
    with_gateway(Name0, fun(GwName, _) ->
        case {maps:get(<<"topic">>, Body, undefined), subopts(Body)} of
            {undefined, _} ->
                return_http_error(400, "Miss topic property");
            {Topic, QoS} ->
                case emqx_gateway_http:client_subscribe(GwName, ClientId, Topic, QoS) of
                    {error, Reason} ->
                        return_http_error(404, Reason);
                    ok ->
                        {204}
                end
        end
    end);

%% Remove the subscription without mountpoint
subscriptions(delete, #{ bindings := #{name := Name0,
                                       clientid := ClientId0,
                                       topic := Topic0
                                      }
                       }) ->
    ClientId = emqx_mgmt_util:urldecode(ClientId0),
    Topic = emqx_mgmt_util:urldecode(Topic0),
    with_gateway(Name0, fun(GwName, _) ->
        _ = emqx_gateway_http:client_unsubscribe(GwName, ClientId, Topic),
        {204}
    end).

%%--------------------------------------------------------------------
%% Utils

subopts(Req) ->
    #{ qos => maps:get(<<"qos">>, Req, 0)
     , rap => maps:get(<<"rap">>, Req, 0)
     , nl => maps:get(<<"nl">>, Req, 0)
     , rh => maps:get(<<"rh">>, Req, 0)
     , sub_props => extra_sub_props(maps:get(<<"sub_props">>, Req, #{}))
     }.

extra_sub_props(Props) ->
    maps:filter(
      fun(_, V) -> V =/= undefined end,
      #{subid => maps:get(<<"subid">>, Props, undefined)}
     ).

%%--------------------------------------------------------------------
%% query funcs

query(Tab, {Qs, []}, Continuation, Limit) ->
    Ms = qs2ms(Qs),
    emqx_mgmt_api:select_table_with_count(Tab, Ms, Continuation, Limit,
                                          fun format_channel_info/1);

query(Tab, {Qs, Fuzzy}, Continuation, Limit) ->
    Ms = qs2ms(Qs),
    FuzzyFilterFun = fuzzy_filter_fun(Fuzzy),
    emqx_mgmt_api:select_table_with_count(Tab, {Ms, FuzzyFilterFun}, Continuation, Limit,
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
    #{clientinfo => #{peername => {X, '_'}}};
ms(conn_state, X) ->
    #{conn_state => X};
ms(clean_start, X) ->
    #{conninfo => #{clean_start => X}};
ms(proto_ver, X) ->
    #{conninfo => #{proto_ver => X}};
ms(connected_at, X) ->
    #{conninfo => #{connected_at => X}};
ms(created_at, X) ->
    #{session => #{created_at => X}};
%% lwm2m fields
ms(endpoint_name, X) ->
    #{clientinfo => #{endpoint_name => X}};
ms(lifetime, X) ->
    #{clientinfo => #{lifetime => X}}.

%%--------------------------------------------------------------------
%% Fuzzy filter funcs

fuzzy_filter_fun(Fuzzy) ->
    REFuzzy = lists:map(fun({K, like, S}) ->
        {ok, RE} = re:compile(S),
        {K, like, RE}
                        end, Fuzzy),
    fun(MsRaws) when is_list(MsRaws) ->
            lists:filter( fun(E) -> run_fuzzy_filter(E, REFuzzy) end
                        , MsRaws)
    end.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(E = {_, #{clientinfo := ClientInfo}, _}, [{Key, _, RE} | Fuzzy]) ->
    Val = case maps:get(Key, ClientInfo, "") of
              undefined -> "";
              V -> V
          end,
    re:run(Val, RE, [{capture, none}]) == match andalso run_fuzzy_filter(E, Fuzzy).

%%--------------------------------------------------------------------
%% format funcs

format_channel_info({_, Infos, Stats} = R) ->
    ClientInfo = maps:get(clientinfo, Infos, #{}),
    ConnInfo = maps:get(conninfo, Infos, #{}),
    SessInfo = maps:get(session, Infos, #{}),
    FetchX = [ {node, ClientInfo, node()}
             , {clientid, ClientInfo}
             , {username, ClientInfo}
             , {proto_name, ConnInfo}
             , {proto_ver, ConnInfo}
             , {ip_address, {peername, ConnInfo, fun peer_to_binary_addr/1}}
             , {port, {peername, ConnInfo, fun peer_to_port/1}}
             , {is_bridge, ClientInfo, false}
             , {connected_at,
                {connected_at, ConnInfo, fun emqx_gateway_utils:unix_ts_to_rfc3339/1}}
             , {disconnected_at,
                {disconnected_at, ConnInfo, fun emqx_gateway_utils:unix_ts_to_rfc3339/1}}
             , {connected, {conn_state, Infos, fun conn_state_to_connected/1}}
             , {keepalive, ClientInfo, 0}
             , {clean_start, ConnInfo, true}
             , {expiry_interval, ConnInfo, 0}
             , {created_at,
                {created_at, SessInfo, fun emqx_gateway_utils:unix_ts_to_rfc3339/1}}
             , {subscriptions_cnt, Stats, 0}
             , {subscriptions_max, Stats, infinity}
             , {inflight_cnt, Stats, 0}
             , {inflight_max, Stats, infinity}
             , {mqueue_len, Stats, 0}
             , {mqueue_max, Stats, infinity}
             , {mqueue_dropped, Stats, 0}
             , {awaiting_rel_cnt, Stats, 0}
             , {awaiting_rel_max, Stats, infinity}
             , {recv_oct, Stats, 0}
             , {recv_cnt, Stats, 0}
             , {recv_pkt, Stats, 0}
             , {recv_msg, Stats, 0}
             , {send_oct, Stats, 0}
             , {send_cnt, Stats, 0}
             , {send_pkt, Stats, 0}
             , {send_msg, Stats, 0}
             , {mailbox_len, Stats, 0}
             , {heap_size, Stats, 0}
             , {reductions, Stats, 0}
             ],
    eval(FetchX ++ extra_feilds(R)).

extra_feilds({_, Infos, _Stats} = R) ->
    extra_feilds(
      maps:get(protocol, maps:get(clientinfo, Infos)),
      R).

extra_feilds(lwm2m, {_, Infos, _Stats}) ->
    ClientInfo = maps:get(clientinfo, Infos, #{}),
    [ {endpoint_name, ClientInfo}
    , {lifetime, ClientInfo}
    ];
extra_feilds(_, _) ->
    [].

eval(Ls) ->
    eval(Ls, #{}).
eval([], AccMap) ->
    AccMap;
eval([{K, Vx} | More], AccMap) ->
    case valuex_get(K, Vx) of
        undefined -> eval(More, AccMap#{K => null});
        Value -> eval(More, AccMap#{K => Value})
    end;
eval([{K, Vx, Default} | More], AccMap) ->
    case valuex_get(K, Vx) of
        undefined -> eval(More, AccMap#{K => Default});
        Value -> eval(More, AccMap#{K => Value})
    end.

valuex_get(K, Vx) when is_map(Vx); is_list(Vx) ->
    key_get(K, Vx);
valuex_get(_K, {InKey, Obj}) when is_map(Obj); is_list(Obj) ->
    key_get(InKey, Obj);
valuex_get(_K, {InKey, Obj, MappingFun}) when is_map(Obj); is_list(Obj) ->
    case key_get(InKey, Obj) of
        undefined -> undefined;
        Val -> MappingFun(Val)
    end.

key_get(K, M) when is_map(M) ->
    maps:get(K, M, undefined);
key_get(K, L) when is_list(L) ->
    proplists:get_value(K, L).

-spec(peer_to_binary_addr(emqx_types:peername()) -> binary()).
peer_to_binary_addr({Addr, _}) ->
    list_to_binary(inet:ntoa(Addr)).

-spec(peer_to_port(emqx_types:peername()) -> inet:port_number()).
peer_to_port({_, Port}) ->
    Port.

conn_state_to_connected(connected) -> true;
conn_state_to_connected(_) -> false.

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

metadata(APIs) ->
    metadata(APIs, []).
metadata([], APIAcc) ->
    lists:reverse(APIAcc);
metadata([{Path, Fun} | More], APIAcc) ->
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
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_clients_list()
       }
     };
swagger("/gateway/:name/clients/:clientid", get) ->
    #{ description => <<"Get the gateway client infomation">>
     , parameters => params_client_insta()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_client()
       }
     };
swagger("/gateway/:name/clients/:clientid", delete) ->
    #{ description => <<"Kick out the gateway client">>
     , parameters => params_client_insta()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
       }
     };
swagger("/gateway/:name/clients/:clientid/subscriptions", get) ->
    #{ description => <<"Get the gateway client subscriptions">>
     , parameters => params_client_insta()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_subscription_list()
       }
     };
swagger("/gateway/:name/clients/:clientid/subscriptions", post) ->
    #{ description => <<"Get the gateway client subscriptions">>
     , parameters => params_client_insta()
     , requestBody => schema_subscription()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
       }
     };
swagger("/gateway/:name/clients/:clientid/subscriptions/:topic", delete) ->
    #{ description => <<"Unsubscribe the topic for client">>
     , parameters => params_topic_name_in_path() ++ params_client_insta()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
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
      , {proto_ver, string}
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
%% schemas

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
%% properties defines

properties_client() ->
    %% FIXME: enum for every protocol's client
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
      , {port, integer,
         <<"Client's port">>}
      , {is_bridge, boolean,
         <<"Indicates whether the client is connectedvia bridge">>}
      , {connected_at, string,
         <<"Client connection time">>}
      , {disconnected_at, string,
         <<"Client offline time, This field is only valid and returned "
            "when connected is false">>}
      , {connected, boolean,
         <<"Whether the client is connected">>}
      %% FIXME: the will_msg attribute is not a general attribute
      %% for every protocol. But it should be returned to frontend if someone
      %% want it
      %%
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
    ExtraProps = [ {subid, string,
                    <<"Only stomp protocol, an uniquely identity for "
                      "the subscription. range: 1-65535.">>}
                 ],
    emqx_mgmt_util:properties(
     [ {topic, string,
        <<"Topic Fillter">>}
     , {qos, integer,
        <<"QoS level, enum: 0, 1, 2">>}
     , {nl, integer,     %% FIXME: why not boolean?
        <<"No Local option, enum: 0, 1">>}
     , {rap, integer,
        <<"Retain as Published option, enum: 0, 1">>}
     , {rh, integer,
        <<"Retain Handling option, enum: 0, 1, 2">>}
     , {sub_props, object, ExtraProps}
     ]).
