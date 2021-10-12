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

-module(emqx_gateway_api_listeners).

-behaviour(minirest_api).

-import(emqx_gateway_http,
        [ return_http_error/2
        , schema_bad_request/0
        , schema_not_found/0
        , schema_internal_error/0
        , schema_no_content/0
        , with_gateway/2
        , checks/2
        ]).

-import(emqx_gateway_api_authn, [schema_authn/0]).

%% minirest behaviour callbacks
-export([api_spec/0]).

%% http handlers
-export([ listeners/2
        , listeners_insta/2
        , listeners_insta_authn/2
        ]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    {metadata(apis()), []}.

apis() ->
    [ {"/gateway/:name/listeners", listeners}
    , {"/gateway/:name/listeners/:id", listeners_insta}
    , {"/gateway/:name/listeners/:id/authentication", listeners_insta_authn}
    ].

%%--------------------------------------------------------------------
%% http handlers

listeners(get, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        {200, emqx_gateway_conf:listeners(GwName)}
    end);

listeners(post, #{bindings := #{name := Name0}, body := LConf}) ->
    with_gateway(Name0, fun(GwName, Gateway) ->
        RunningConf = maps:get(config, Gateway),
        %% XXX: check params miss? check badly data tpye??
        _ = checks([<<"type">>, <<"name">>, <<"bind">>], LConf),

        Type = binary_to_existing_atom(maps:get(<<"type">>, LConf)),
        LName = binary_to_atom(maps:get(<<"name">>, LConf)),

        Path = [listeners, Type, LName],
        case emqx_map_lib:deep_get(Path, RunningConf, undefined) of
            undefined ->
                ListenerId = emqx_gateway_utils:listener_id(
                               GwName, Type, LName),
                ok = emqx_gateway_http:add_listener(ListenerId, LConf),
                {204};
            _ ->
                return_http_error(400, "Listener name has occupied")
        end
    end).

listeners_insta(delete, #{bindings := #{name := Name0, id := ListenerId0}}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(_GwName, _) ->
        ok = emqx_gateway_http:remove_listener(ListenerId),
        {204}
    end);
listeners_insta(get, #{bindings := #{name := Name0, id := ListenerId0}}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(_GwName, _) ->
        case emqx_gateway_conf:listener(ListenerId) of
            {ok, Listener} ->
                {200, Listener};
            {error, not_found} ->
                return_http_error(404, "Listener not found");
            {error, Reason} ->
                return_http_error(500, Reason)
        end
    end);
listeners_insta(put, #{body := LConf,
                       bindings := #{name := Name0, id := ListenerId0}
                      }) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(_GwName, _) ->
        ok = emqx_gateway_http:update_listener(ListenerId, LConf),
        {204}
    end).

listeners_insta_authn(get, #{bindings := #{name := Name0,
                                           id := ListenerId0}}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(GwName, _) ->
        {200, emqx_gateway_http:authn(GwName, ListenerId)}
    end);
listeners_insta_authn(post, #{body := Conf,
                              bindings := #{name := Name0,
                                            id := ListenerId0}}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(GwName, _) ->
        ok = emqx_gateway_http:add_authn(GwName, ListenerId, Conf),
        {204}
    end);
listeners_insta_authn(put, #{body := Conf,
                             bindings := #{name := Name0,
                                           id := ListenerId0}}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(GwName, _) ->
        ok = emqx_gateway_http:update_authn(GwName, ListenerId, Conf),
        {204}
    end);
listeners_insta_authn(delete, #{bindings := #{name := Name0,
                                              id := ListenerId0}}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(GwName, _) ->
        ok = emqx_gateway_http:remove_authn(GwName, ListenerId),
        {204}
    end).

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

swagger("/gateway/:name/listeners", get) ->
    #{ description => <<"Get the gateway listeners">>
     , parameters => params_gateway_name_in_path()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_listener_list()
         }
     };
swagger("/gateway/:name/listeners", post) ->
    #{ description => <<"Create the gateway listener">>
     , parameters => params_gateway_name_in_path()
     , requestBody => schema_listener()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_listener_list()
         }
     };
swagger("/gateway/:name/listeners/:id", get) ->
    #{ description => <<"Get the gateway listener configurations">>
     , parameters => params_gateway_name_in_path()
                     ++ params_listener_id_in_path()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_listener()
         }
      };
swagger("/gateway/:name/listeners/:id", delete) ->
    #{ description => <<"Delete the gateway listener">>
     , parameters => params_gateway_name_in_path()
                     ++ params_listener_id_in_path()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
         }
      };
swagger("/gateway/:name/listeners/:id", put) ->
    #{ description => <<"Update the gateway listener">>
     , parameters => params_gateway_name_in_path()
                     ++ params_listener_id_in_path()
     , requestBody => schema_listener()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_no_content()
         }
     };
swagger("/gateway/:name/listeners/:id/authentication", get) ->
    #{ description => <<"Get the listener's authentication info">>
     , parameters => params_gateway_name_in_path()
                     ++ params_listener_id_in_path()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_authn()
         }
     };
swagger("/gateway/:name/listeners/:id/authentication", post) ->
    #{ description => <<"Add authentication for the listener">>
     , parameters => params_gateway_name_in_path()
                     ++ params_listener_id_in_path()
     , requestBody => schema_authn()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
         }
     };
swagger("/gateway/:name/listeners/:id/authentication", put) ->
    #{ description => <<"Update authentication for the listener">>
     , parameters => params_gateway_name_in_path()
                     ++ params_listener_id_in_path()
     , requestBody => schema_authn()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
         }
     };
swagger("/gateway/:name/listeners/:id/authentication", delete) ->
    #{ description => <<"Remove authentication for the listener">>
     , parameters => params_gateway_name_in_path()
                     ++ params_listener_id_in_path()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
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

params_listener_id_in_path() ->
    [#{ name => id
      , in => path
      , schema => #{type => string}
      , required => true
      }].

%%--------------------------------------------------------------------
%% schemas

schema_listener_list() ->
    emqx_mgmt_util:array_schema(
      #{ type => object
       , properties => properties_listener()
       },
      <<"Listener list">>
     ).

schema_listener() ->
    emqx_mgmt_util:schema(
      #{ type => object
       , properties => properties_listener()
       }
     ).

%%--------------------------------------------------------------------
%% properties

properties_listener() ->
    emqx_mgmt_util:properties(
      raw_properties_common_listener() ++
      [ {tcp, object, raw_properties_tcp_opts()}
      , {ssl, object, raw_properties_ssl_opts()}
      , {udp, object, raw_properties_udp_opts()}
      , {dtls, object, raw_properties_dtls_opts()}
      ]).

raw_properties_tcp_opts() ->
    [ {active_n, integer, <<>>}
    , {backlog, integer, <<>>}
    , {buffer, string, <<>>}
    , {recbuf, string, <<>>}
    , {sndbuf, string, <<>>}
    , {high_watermark, string, <<>>}
    , {nodelay, boolean, <<>>}
    , {reuseaddr, boolean, <<>>}
    , {send_timeout, string, <<>>}
    , {send_timeout_close, boolean, <<>>}
    ].

raw_properties_ssl_opts() ->
    [ {cacertfile, string, <<>>}
    , {certfile, string, <<>>}
    , {keyfile, string, <<>>}
    , {verify, string, <<>>}
    , {fail_if_no_peer_cert, boolean, <<>>}
    , {server_name_indication, boolean, <<>>}
    , {depth, integer, <<>>}
    , {password, string, <<>>}
    , {handshake_timeout, string, <<>>}
    , {versions, {array, string}, <<>>}
    , {ciphers, {array, string}, <<>>}
    , {user_lookup_fun, string, <<>>}
    , {reuse_sessions, boolean, <<>>}
    , {secure_renegotiate, boolean, <<>>}
    , {honor_cipher_order, boolean, <<>>}
    , {dhfile, string, <<>>}
    ].

raw_properties_udp_opts() ->
    [ {active_n, integer, <<>>}
    , {buffer, string, <<>>}
    , {recbuf, string, <<>>}
    , {sndbuf, string, <<>>}
    , {reuseaddr, boolean, <<>>}
    ].

raw_properties_dtls_opts() ->
    Ls = lists_key_without(
      [versions,ciphers,handshake_timeout], 1,
      raw_properties_ssl_opts()
     ),
    [ {versions, {array, string}, <<>>}
    , {ciphers, {array, string}, <<>>}
    | Ls].

lists_key_without([], _N, L) ->
    L;
lists_key_without([K|Ks], N, L) ->
    lists_key_without(Ks, N, lists:keydelete(K, N, L)).

raw_properties_common_listener() ->
    [ {enable, boolean, <<"Whether to enable this listener">>}
    , {id, string, <<"Listener Id">>}
    , {name, string, <<"Listener name">>}
    , {type, string,
       <<"Listener type. Enum: tcp, udp, ssl, dtls">>,
       [<<"tcp">>, <<"ssl">>, <<"udp">>, <<"dtls">>]}
    , {running, boolean, <<"Listener running status">>}
    , {bind, string, <<"Listener bind address or port">>}
    , {acceptors, integer, <<"Listener acceptors number">>}
    , {access_rules, {array, string}, <<"Listener Access rules for client">>}
    , {max_conn_rate, integer, <<"Max connection rate for the listener">>}
    , {max_connections, integer, <<"Max connections for the listener">>}
    , {mountpoint, string,
       <<"The Mounpoint for clients of the listener. "
         "The gateway-level mountpoint configuration can be overloaded "
         "when it is not null or empty string">>}
    %% FIXME:
    , {authentication, string, <<"NOT-SUPPORTED-NOW">>}
   ].
