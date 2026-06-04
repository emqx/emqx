-module(emqx_acme_api_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        t_put_config_returns_400_when_parser_rejects_avro_valid_input
    ].

-doc "Models the contract behind the HTTP PUT /plugins/:name/config 400 "
"response when a body passes the avsc schema but the plugin's own parser "
"rejects it. The HTTP layer's two validation hops (see "
"emqx_mgmt_api_plugins:put_plugin_config/2) are exercised independently: "
"emqx_plugins_serde:decode/2 runs the avsc validation, and "
"emqx_acme_config:parse/1 runs the plugin-specific parse. A bad domain "
"(\"admin@example\") is the canonical case -- the avsc accepts any "
"string for `domains` but the parser refuses entries that idna would "
"choke on.".
t_put_config_returns_400_when_parser_rejects_avro_valid_input(_Config) ->
    BadBody = #{
        <<"dir_url">> => <<"https://acme-v02.api.letsencrypt.org/directory">>,
        <<"domains">> => <<"mqtt.example.com,admin@example">>,
        <<"contact">> => <<"">>,
        <<"cert_bundle_name">> => <<"acme">>,
        <<"listener_ids">> => <<"ssl:default,wss:default">>,
        <<"enable_dashboard_https">> => true,
        <<"dashboard_https_port">> => 18084,
        <<"cert_type">> => <<"ec">>,
        <<"challenge_port">> => 80,
        <<"renew_before_expiry_days">> => 30,
        <<"check_interval_hours">> => 24,
        <<"acc_key">> => <<"">>,
        <<"acc_key_password">> => <<"">>
    },
    BadJson = emqx_utils_json:encode(BadBody),

    %% Layer 1 (avsc validation): emqx_plugins_serde:decode/2 mirrors the
    %% server-side avsc check that runs before the BPAPI fan-out. It must
    %% accept the body -- only then is the parse-fail case distinguishable
    %% from the avsc-fail case (which would already return 400 with a
    %% different error before reaching the parser).
    Store = build_avro_store(),
    Opts = avro:make_decoder_options(
        [
            {map_type, map},
            {record_type, map},
            {encoding, avro_json},
            {is_wrapped, false}
        ]
    ),
    ?assertMatch(
        #{<<"domains">> := <<"mqtt.example.com,admin@example">>},
        avro_json_decoder:decode_value(BadJson, schema_name(), Store, Opts)
    ),

    %% Layer 2 (plugin parse): emqx_acme_config:parse/1 is what runs inside
    %% emqx_acme_app:on_config_changed/2 on every node during the BPAPI
    %% fan-out. Its {error, Reason} return is what
    %% emqx_mgmt_api_plugins:return_config_update_result/1 maps to a
    %% 400 BAD_CONFIG response.
    ?assertEqual(
        {error, {invalid_domain, <<"admin@example">>}},
        emqx_acme_config:parse(BadBody)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

schema_name() ->
    <<"emqx_acme_config_schema_test">>.

build_avro_store() ->
    AvscPath = filename:join([code:lib_dir(emqx_acme), "priv", "config_schema.avsc"]),
    {ok, AvscBin} = file:read_file(AvscPath),
    Store0 = avro_schema_store:new([map]),
    avro_schema_store:import_schema_json(schema_name(), AvscBin, Store0).
