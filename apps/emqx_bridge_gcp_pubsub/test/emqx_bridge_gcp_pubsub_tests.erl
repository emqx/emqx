%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_tests).

-include_lib("eunit/include/eunit.hrl").

%%===========================================================================
%% Data section
%%===========================================================================

%% erlfmt-ignore
gcp_pubsub_producer_hocon() ->
"
bridges.gcp_pubsub.my_producer {
  attributes_template = [
    {key = \"${payload.key}\", value = fixed_value}
    {key = \"${payload.key}2\", value = \"${.payload.value}\"}
    {key = fixed_key, value = fixed_value}
    {key = fixed_key2, value = \"${.payload.value}\"}
  ]
  connect_timeout = 15s
  enable = false
  local_topic = \"t/gcp/produ\"
  max_retries = 2
  ordering_key_template = \"${.payload.ok}\"
  payload_template = \"${.}\"
  pipelining = 100
  pool_size = 8
  pubsub_topic = my-topic
  resource_opts {
    batch_size = 1
    batch_time = 0ms
    health_check_interval = 15s
    inflight_window = 100
    max_buffer_bytes = 256MB
    query_mode = async
    request_ttl = 15s
    start_after_created = true
    start_timeout = 5s
    worker_pool_size = 16
  }
  service_account_json {
    auth_provider_x509_cert_url = \"https://www.googleapis.com/oauth2/v1/certs\"
    auth_uri = \"https://accounts.google.com/o/oauth2/auth\"
    client_email = \"test@myproject.iam.gserviceaccount.com\"
    client_id = \"123812831923812319190\"
    client_x509_cert_url = \"https://www.googleapis.com/robot/v1/metadata/x509/...\"
    private_key = \"-----BEGIN PRIVATE KEY-----...\"
    private_key_id = \"kid\"
    project_id = myproject
    token_uri = \"https://oauth2.googleapis.com/token\"
    type = service_account
  }
}
".

%%===========================================================================
%% Helper functions
%%===========================================================================

parse(Hocon) ->
    {ok, Conf} = hocon:binary(Hocon),
    Conf.

check(Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(emqx_bridge_schema, Conf).

-define(validation_error(Reason, Value),
    {emqx_bridge_schema, [
        #{
            kind := validation_error,
            reason := Reason,
            value := Value
        }
    ]}
).

-define(ok_config(Cfg), #{
    <<"bridges">> :=
        #{
            <<"gcp_pubsub">> :=
                #{
                    <<"my_producer">> :=
                        Cfg
                }
        }
}).

%%===========================================================================
%% Test cases
%%===========================================================================

producer_attributes_validator_test_() ->
    %% ensure this module is loaded when testing only this file
    _ = emqx_bridge_enterprise:module_info(),
    BaseConf = parse(gcp_pubsub_producer_hocon()),
    Override = fun(Cfg) ->
        emqx_utils_maps:deep_merge(
            BaseConf,
            #{
                <<"bridges">> =>
                    #{
                        <<"gcp_pubsub">> =>
                            #{<<"my_producer">> => Cfg}
                    }
            }
        )
    end,
    [
        {"base config",
            ?_assertMatch(
                ?ok_config(#{
                    <<"attributes_template">> := [_, _, _, _]
                }),
                check(BaseConf)
            )},
        {"empty key template",
            ?_assertThrow(
                ?validation_error("Key templates must not be empty", _),
                check(
                    Override(#{
                        <<"attributes_template">> => [
                            #{
                                <<"key">> => <<>>,
                                <<"value">> => <<"some_value">>
                            }
                        ]
                    })
                )
            )},
        {"empty value template",
            ?_assertMatch(
                ?ok_config(#{
                    <<"attributes_template">> := [_]
                }),
                check(
                    Override(#{
                        <<"attributes_template">> => [
                            #{
                                <<"key">> => <<"some_key">>,
                                <<"value">> => <<>>
                            }
                        ]
                    })
                )
            )}
    ].
