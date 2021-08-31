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

-export([ clients/2
        , clients_insta/2
        , subscriptions/2
        ]).

api_spec() ->
    {metadata(apis()), []}.

apis() ->
    [ {"/gateway/:name/clients", clients}
    , {"/gateway/:name/clients/:clientid", clients_insta}
    , {"/gateway/:name/clients/:clientid/subscriptions", subscriptions}
    , {"/gateway/:name/clients/:clientid/subscriptions/:topic", subscriptions}
    ].

clients(get, _Req) ->
    {200, []}.

clients_insta(get, _Req) ->
    {200, <<"{}">>};
clients_insta(delete, _Req) ->
    {200}.

subscriptions(get, _Req) ->
    {200, []};
subscriptions(delete, _Req) ->
    {200}.

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
     , parameters => params_client_insta() ++ params_topic_name_in_path()
     , responses =>
        #{ <<"404">> => schema_not_found()
         , <<"204">> => schema_no_content()
       }
     }.

params_client_query() ->
    params_client_searching_in_qs()
    ++ emqx_mgmt_util:page_params()
    ++ params_gateway_name_in_path().

params_client_insta() ->
    params_gateway_name_in_path()
    ++ params_clientid_in_path().

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
    emqx_mgmt_util:array_schema(
      #{ type => object
       , properties => properties_client()
       },
      <<"Client lists">>
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
