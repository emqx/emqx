%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ct_broker_helpers).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx).

-define(MQTT_SSL_TWOWAY, [{cacertfile, "certs/cacert.pem"},
                          {verify, verify_peer},
                          {fail_if_no_peer_cert, true}]).

-define(MQTT_SSL_CLIENT, [{keyfile, "certs/client-key.pem"},
                          {cacertfile, "certs/cacert.pem"},
                          {certfile, "certs/client-cert.pem"}]).

-define(CIPHERS,    [{ciphers,
                        ["ECDHE-ECDSA-AES256-GCM-SHA384",
                         "ECDHE-RSA-AES256-GCM-SHA384",
                         "ECDHE-ECDSA-AES256-SHA384",
                         "ECDHE-RSA-AES256-SHA384","ECDHE-ECDSA-DES-CBC3-SHA",
                         "ECDH-ECDSA-AES256-GCM-SHA384",
                         "ECDH-RSA-AES256-GCM-SHA384",
                         "ECDH-ECDSA-AES256-SHA384","ECDH-RSA-AES256-SHA384",
                         "DHE-DSS-AES256-GCM-SHA384","DHE-DSS-AES256-SHA256",
                         "AES256-GCM-SHA384","AES256-SHA256",
                         "ECDHE-ECDSA-AES128-GCM-SHA256",
                         "ECDHE-RSA-AES128-GCM-SHA256",
                         "ECDHE-ECDSA-AES128-SHA256",
                         "ECDHE-RSA-AES128-SHA256",
                         "ECDH-ECDSA-AES128-GCM-SHA256",
                         "ECDH-RSA-AES128-GCM-SHA256",
                         "ECDH-ECDSA-AES128-SHA256","ECDH-RSA-AES128-SHA256",
                         "DHE-DSS-AES128-GCM-SHA256","DHE-DSS-AES128-SHA256",
                         "AES128-GCM-SHA256","AES128-SHA256",
                         "ECDHE-ECDSA-AES256-SHA","ECDHE-RSA-AES256-SHA",
                         "DHE-DSS-AES256-SHA","ECDH-ECDSA-AES256-SHA",
                         "ECDH-RSA-AES256-SHA","AES256-SHA",
                         "ECDHE-ECDSA-AES128-SHA","ECDHE-RSA-AES128-SHA",
                         "DHE-DSS-AES128-SHA","ECDH-ECDSA-AES128-SHA",
                         "ECDH-RSA-AES128-SHA","AES128-SHA"]}]).

run_setup_steps() ->
    NewConfig = generate_config(),
    lists:foreach(fun set_app_env/1, NewConfig),
    set_bridge_env(),
    application:ensure_all_started(?APP).

run_teardown_steps() ->
    ?APP:shutdown().

generate_config() ->
    Schema = cuttlefish_schema:files([local_path(["priv", "emqx.schema"])]),
    Conf = conf_parse:file([local_path(["etc", "gen.emqx.conf"])]),
    cuttlefish_generator:map(Schema, Conf).

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

get_base_dir() ->
    get_base_dir(?MODULE).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

local_path(Components) ->
    local_path(Components, ?MODULE).

set_app_env({App, Lists}) ->
    lists:foreach(fun({acl_file, _Var}) ->
                      application:set_env(App, acl_file, local_path(["etc", "acl.conf"]));
                     ({plugins_loaded_file, _Var}) ->
                      application:set_env(App, plugins_loaded_file, local_path(["test", "emqx_SUITE_data","loaded_plugins"]));
                     ({Par, Var}) ->
                      application:set_env(App, Par, Var)
                  end, Lists).

set_bridge_env() ->
    BridgeEnvs = bridge_conf(),
    application:set_env(?APP, bridges, BridgeEnvs).

change_opts(SslType) ->
    {ok, Listeners} = application:get_env(?APP, listeners),
    NewListeners =
    lists:foldl(fun({Protocol, Port, Opts} = Listener, Acc) ->
    case Protocol of
    ssl ->
            SslOpts = proplists:get_value(ssl_options, Opts),
            Keyfile = local_path(["etc/certs", "key.pem"]),
            Certfile = local_path(["etc/certs", "cert.pem"]),
            TupleList1 = lists:keyreplace(keyfile, 1, SslOpts, {keyfile, Keyfile}),
            TupleList2 = lists:keyreplace(certfile, 1, TupleList1, {certfile, Certfile}),
            TupleList3 =
            case SslType of
            ssl_twoway->
                CAfile = local_path(["etc", proplists:get_value(cacertfile, ?MQTT_SSL_TWOWAY)]),
                MutSslList = lists:keyreplace(cacertfile, 1, ?MQTT_SSL_TWOWAY, {cacertfile, CAfile}),
                lists:merge(TupleList2, MutSslList);
            _ ->
                lists:filter(fun ({cacertfile, _}) -> false;
                                 ({verify, _}) -> false;
                                 ({fail_if_no_peer_cert, _}) -> false;
                                 (_) -> true
                             end, TupleList2)
            end,
            [{Protocol, Port, lists:keyreplace(ssl_options, 1, Opts, {ssl_options, TupleList3})} | Acc];
        _ ->
            [Listener | Acc]
    end
    end, [], Listeners),
    application:set_env(?APP, listeners, NewListeners).

client_ssl_twoway() ->
    [{Key, local_path(["etc", File])} || {Key, File} <- ?MQTT_SSL_CLIENT] ++ ?CIPHERS.

client_ssl() ->
    ?CIPHERS ++ [{reuse_sessions, true}].

wait_mqtt_payload(Payload) ->
    receive
        {publish, #{payload := Payload}} ->
            ct:pal("OK - received msg: ~p~n", [Payload])
    after 1000 ->
        ct:fail({timeout, Payload, {msg_box, flush()}})
    end.

not_wait_mqtt_payload(Payload) ->
    receive
        {publish, #{payload := Payload}} ->
            ct:fail({received, Payload})
    after 1000 ->
        ct:pal("OK - msg ~p is not received", [Payload])
    end.

flush() ->
    flush([]).
flush(Msgs) ->
    receive
        M -> flush([M|Msgs])
    after
        0 -> lists:reverse(Msgs)
    end.

bridge_conf() ->
    [ {local_rpc,
        [{connect_module, emqx_portal_rpc},
         {address, node()},
         {forwards, ["portal-1/#", "portal-2/#"]}
        ]}
    ].
    % [{aws,
    %   [{connect_module, emqx_portal_mqtt},
    %   {username,"user"},
    %    {address,"127.0.0.1:1883"},
    %    {clean_start,true},
    %    {client_id,"bridge_aws"},
    %    {forwards,["topic1/#","topic2/#"]},
    %    {keepalive,60000},
    %    {max_inflight,32},
    %    {mountpoint,"bridge/aws/${node}/"},
    %    {password,"passwd"},
    %    {proto_ver,mqttv4},
    %    {queue,
    %     #{batch_coun t_limit => 1000,
    %       replayq_dir => "data/emqx_aws_bridge/",
    %       replayq_seg_bytes => 10485760}},
    %    {reconnect_delay_ms,30000},
    %    {ssl,false},
    %    {ssl_opts,[{versions,[tlsv1,'tlsv1.1','tlsv1.2']}]},
    %    {start_type,manual},
    %    {subscriptions,[{"cmd/topic1",1},{"cmd/topic2",1}]}]}].
