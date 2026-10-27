%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_tablestore_tests).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

-define(CONNECTOR_TYPE_BIN, <<"tablestore">>).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        ~"endpoint" => ~"some.endpoint",
        ~"instance_name" => ~"some_instance",
        ~"access_key_id" => ~"aki",
        ~"access_key_secret" => ~"aks"
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

default_ssl_opts_test_() ->
    [
        {"disabled by default",
            ?_assertMatch(
                #{
                    ~"ssl" := #{
                        ~"enable" := false,
                        ~"verify" := ~"verify_peer"
                    }
                },
                connector_config(#{})
            )},
        {"enabled",
            ?_assertMatch(
                #{
                    ~"ssl" := #{
                        ~"enable" := true,
                        ~"verify" := ~"verify_peer"
                    }
                },
                connector_config(#{~"ssl" => #{~"enable" => true}})
            )},
        {"override verify_peer",
            ?_assertMatch(
                #{
                    ~"ssl" := #{
                        ~"enable" := true,
                        ~"verify" := ~"verify_none"
                    }
                },
                connector_config(#{~"ssl" => #{~"enable" => true, ~"verify" => ~"verify_none"}})
            )}
    ].
