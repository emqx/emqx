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
-module(emqx_gateway_api).

-include_lib("emqx/include/emqx_placeholder.hrl").

-behaviour(minirest_api).

-import(emqx_gateway_http,
        [ return_http_error/2
        , with_gateway/2
        , schema_bad_request/0
        , schema_not_found/0
        , schema_internal_error/0
        , schema_no_content/0
        ]).

%% minirest behaviour callbacks
-export([api_spec/0]).

%% http handlers
-export([ gateway/2
        , gateway_insta/2
        , gateway_insta_stats/2
        ]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    {metadata(apis()), []}.

apis() ->
    [ {"/gateway", gateway}
    , {"/gateway/:name", gateway_insta}
    , {"/gateway/:name/stats", gateway_insta_stats}
    ].

%%--------------------------------------------------------------------
%% http handlers

gateway(get, Request) ->
    Params = maps:get(query_string, Request, #{}),
    Status = case maps:get(<<"status">>, Params, undefined) of
                 undefined -> all;
                 S0 -> binary_to_existing_atom(S0, utf8)
             end,
    {200, emqx_gateway_http:gateways(Status)};
gateway(post, Request) ->
    Body = maps:get(body, Request, #{}),
    try
        Name0 = maps:get(<<"name">>, Body),
        GwName = binary_to_existing_atom(Name0),
        case emqx_gateway_registry:lookup(GwName) of
            undefined -> error(badarg);
            _ ->
                GwConf = maps:without([<<"name">>], Body),
                case emqx_gateway_conf:load_gateway(GwName,  GwConf) of
                    ok ->
                        {204};
                    {error, Reason} ->
                        return_http_error(500, Reason)
                end
        end
    catch
        error : {badkey, K} ->
            return_http_error(400, [K, " is required"]);
        error : badarg ->
            return_http_error(404, "Bad gateway name")
    end.

gateway_insta(delete, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        case emqx_gateway_conf:unload_gateway(GwName) of
            ok ->
                {204};
            {error, Reason} ->
                return_http_error(400, Reason)
        end
    end);
gateway_insta(get, #{bindings := #{name := Name0}}) ->
    try
        binary_to_existing_atom(Name0)
    of
        GwName ->
            case emqx_gateway:lookup(GwName) of
                undefined ->
                    {200, #{name => GwName, status => unloaded}};
                Gateway ->
                    GwConf = emqx_gateway_conf:gateway(Name0),
                    GwInfo0 = emqx_gateway_utils:unix_ts_to_rfc3339(
                                [created_at, started_at, stopped_at],
                                Gateway),
                    GwInfo1 = maps:with([name,
                                         status,
                                         created_at,
                                         started_at,
                                         stopped_at], GwInfo0),
                    {200, maps:merge(GwConf, GwInfo1)}
            end
    catch
        error : badarg ->
            return_http_error(400, "Bad gateway name")
    end;
gateway_insta(put, #{body := GwConf,
                     bindings := #{name := Name0}
                    }) ->
    with_gateway(Name0, fun(GwName, _) ->
        case emqx_gateway_conf:update_gateway(GwName, GwConf) of
            ok ->
                {204};
            {error, Reason} ->
                return_http_error(500, Reason)
        end
    end).

gateway_insta_stats(get, _Req) ->
    return_http_error(401, "Implement it later (maybe 5.1)").

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

swagger("/gateway", get) ->
    #{ description => <<"Get gateway list">>
     , parameters => params_gateway_status_in_qs()
     , responses =>
        #{ <<"200">> => schema_gateway_overview_list() }
     };
swagger("/gateway", post) ->
    #{ description => <<"Load a gateway">>
     , requestBody => schema_gateway_conf()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
         }
     };
swagger("/gateway/:name", get) ->
    #{ description => <<"Get the gateway configurations">>
     , parameters => params_gateway_name_in_path()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_gateway_conf()
         }
      };
swagger("/gateway/:name", delete) ->
    #{ description => <<"Delete/Unload the gateway">>
     , parameters => params_gateway_name_in_path()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
         }
      };
swagger("/gateway/:name", put) ->
    #{ description => <<"Update the gateway configurations/status">>
     , parameters => params_gateway_name_in_path()
     , requestBody => schema_gateway_conf()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_no_content()
         }
     };
swagger("/gateway/:name/stats", get) ->
    #{ description => <<"Get gateway Statistic">>
     , parameters => params_gateway_name_in_path()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_gateway_stats()
         }
     }.

%%--------------------------------------------------------------------
%% params defines

params_gateway_name_in_path() ->
    [#{ name => name
      , in => path
      , schema => #{type => string}
      , required => true
      }].

params_gateway_status_in_qs() ->
    [#{ name => status
      , in => query
      , schema => #{type => string}
      , required => false
      }].

%%--------------------------------------------------------------------
%% schemas

schema_gateway_overview_list() ->
    emqx_mgmt_util:array_schema(
      #{ type => object
       , properties => properties_gateway_overview()
       },
      <<"Gateway list">>
     ).

%% XXX: This is whole confs for all type gateways. It is used to fill the
%% default configurations and generate the swagger-schema
%%
%% NOTE: It is a temporary measure to generate swagger-schema
-define(COAP_GATEWAY_CONFS,
#{<<"authentication">> =>
      #{<<"mechanism">> => <<"password-based">>,
        <<"name">> => <<"authenticator1">>,
        <<"server_type">> => <<"built-in-database">>,
        <<"user_id_type">> => <<"clientid">>},
  <<"name">> => <<"coap">>,
  <<"enable">> => true,
  <<"enable_stats">> => true,<<"heartbeat">> => <<"30s">>,
  <<"idle_timeout">> => <<"30s">>,
  <<"listeners">> => [
      #{<<"id">> => <<"coap:udp:default">>,
        <<"type">> => <<"udp">>,
        <<"running">> => true,
        <<"acceptors">> => 8,<<"bind">> => 5683,
        <<"max_conn_rate">> => 1000,
        <<"max_connections">> => 10240}],
  <<"mountpoint">> => <<>>,<<"notify_type">> => <<"qos">>,
  <<"publish_qos">> => <<"qos1">>,
  <<"subscribe_qos">> => <<"qos0">>}
).

-define(EXPROTO_GATEWAY_CONFS,
#{<<"enable">> => true,
  <<"name">> => <<"exproto">>,
  <<"enable_stats">> => true,
  <<"handler">> =>
      #{<<"address">> => <<"http://127.0.0.1:9001">>},
  <<"idle_timeout">> => <<"30s">>,
  <<"listeners">> => [
      #{<<"id">> => <<"exproto:tcp:default">>,
        <<"type">> => <<"tcp">>,
        <<"running">> => true,
        <<"acceptors">> => 8,<<"bind">> => 7993,
        <<"max_conn_rate">> => 1000,
        <<"max_connections">> => 10240}],
  <<"mountpoint">> => <<>>,
  <<"server">> => #{<<"bind">> => 9100}}
).

-define(LWM2M_GATEWAY_CONFS,
#{<<"auto_observe">> => false,
  <<"name">> => <<"lwm2m">>,
  <<"enable">> => true,
  <<"enable_stats">> => true,
  <<"idle_timeout">> => <<"30s">>,
  <<"lifetime_max">> => <<"86400s">>,
  <<"lifetime_min">> => <<"1s">>,
  <<"listeners">> => [
      #{<<"id">> => <<"lwm2m:udp:default">>,
        <<"type">> => <<"udp">>,
        <<"running">> => true,
        <<"bind">> => 5783}],
  <<"mountpoint">> => <<"lwm2m/", ?PH_S_ENDPOINT_NAME, "/">>,
  <<"qmode_time_windonw">> => 22,
  <<"translators">> =>
      #{<<"command">> => <<"dn/#">>,<<"notify">> => <<"up/notify">>,
        <<"register">> => <<"up/resp">>,
        <<"response">> => <<"up/resp">>,
        <<"update">> => <<"up/resp">>},
  <<"update_msg_publish_condition">> =>
      <<"contains_object_list">>,
  <<"xml_dir">> => <<"etc/lwm2m_xml">>}
).

-define(MQTTSN_GATEWAY_CONFS,
#{<<"broadcast">> => true,
  <<"clientinfo_override">> =>
      #{<<"password">> => <<"abc">>,
        <<"username">> => <<"mqtt_sn_user">>},
  <<"enable">> => true,
  <<"name">> => <<"mqtt-sn">>,
  <<"enable_qos3">> => true,<<"enable_stats">> => true,
  <<"gateway_id">> => 1,<<"idle_timeout">> => <<"30s">>,
  <<"listeners">> => [
      #{<<"id">> => <<"mqttsn:udp:default">>,
        <<"type">> => <<"udp">>,
        <<"running">> => true,
        <<"bind">> => 1884,<<"max_conn_rate">> => 1000,
                    <<"max_connections">> => 10240000}],
  <<"mountpoint">> => <<>>,
  <<"predefined">> =>
      [#{<<"id">> => 1,
         <<"topic">> => <<"/predefined/topic/name/hello">>},
       #{<<"id">> => 2,
         <<"topic">> => <<"/predefined/topic/name/nice">>}]}
).

-define(STOMP_GATEWAY_CONFS,
#{<<"authentication">> =>
      #{<<"mechanism">> => <<"password-based">>,
        <<"name">> => <<"authenticator1">>,
        <<"server_type">> => <<"built-in-database">>,
        <<"user_id_type">> => <<"clientid">>},
  <<"clientinfo_override">> =>
      #{<<"password">> => <<"${Packet.headers.passcode}">>,
        <<"username">> => <<"${Packet.headers.login}">>},
  <<"enable">> => true,
  <<"name">> => <<"stomp">>,
  <<"enable_stats">> => true,
  <<"frame">> =>
      #{<<"max_body_length">> => 8192,<<"max_headers">> => 10,
        <<"max_headers_length">> => 1024},
  <<"idle_timeout">> => <<"30s">>,
  <<"listeners">> => [
      #{<<"id">> => <<"stomp:tcp:default">>,
        <<"type">> => <<"tcp">>,
        <<"running">> => true,
        <<"acceptors">> => 16,<<"active_n">> => 100,
        <<"bind">> => 61613,<<"max_conn_rate">> => 1000,
        <<"max_connections">> => 1024000}],
  <<"mountpoint">> => <<>>}
).

%% --- END

schema_gateway_conf() ->
    emqx_mgmt_util:schema(
      #{oneOf =>
        [ emqx_mgmt_api_configs:gen_schema(?STOMP_GATEWAY_CONFS)
        , emqx_mgmt_api_configs:gen_schema(?MQTTSN_GATEWAY_CONFS)
        , emqx_mgmt_api_configs:gen_schema(?COAP_GATEWAY_CONFS)
        , emqx_mgmt_api_configs:gen_schema(?LWM2M_GATEWAY_CONFS)
        , emqx_mgmt_api_configs:gen_schema(?EXPROTO_GATEWAY_CONFS)
        ]}).

schema_gateway_stats() ->
    emqx_mgmt_util:schema(
      #{ type => object
       , properties =>
        #{ a_key => #{type => string}
       }}).

%%--------------------------------------------------------------------
%% properties

properties_gateway_overview() ->
    ListenerProps =
        [ {id, string,
           <<"Listener ID">>}
        , {running, boolean,
           <<"Listener Running status">>}
        , {type, string,
           <<"Listener Type">>, [<<"tcp">>, <<"ssl">>, <<"udp">>, <<"dtls">>]}
        ],
    emqx_mgmt_util:properties(
      [ {name, string,
         <<"Gateway Name">>}
      , {status, string,
         <<"Gateway Status">>,
         [<<"running">>, <<"stopped">>, <<"unloaded">>]}
      , {created_at, string,
         <<>>}
      , {started_at, string,
         <<>>}
      , {stopped_at, string,
         <<>>}
      , {max_connections, integer, <<>>}
      , {current_connections, integer, <<>>}
      , {listeners, {array, object}, ListenerProps}
      ]).
