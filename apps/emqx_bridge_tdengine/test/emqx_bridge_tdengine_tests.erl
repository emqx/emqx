%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_tdengine_tests).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

-define(CONNECTOR_TYPE_BIN, <<"tdengine">>).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        ~"server" => ~"toxiproxy:6041",
        ~"username" => ~"root",
        ~"password" => ~"somesecret"
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
                        ~"verify" := ~"verify_peer",
                        ~"cacertfile" := ~"system_default"
                    }
                },
                connector_config(#{})
            )},
        {"enabled",
            ?_assertMatch(
                #{
                    ~"ssl" := #{
                        ~"enable" := true,
                        ~"verify" := ~"verify_peer",
                        ~"cacertfile" := ~"system_default"
                    }
                },
                connector_config(#{~"ssl" => #{~"enable" => true}})
            )},
        {"override verify_peer",
            ?_assertMatch(
                #{
                    ~"ssl" := #{
                        ~"enable" := true,
                        ~"verify" := ~"verify_none",
                        ~"cacertfile" := ~"/some/file"
                    }
                },
                connector_config(#{
                    ~"ssl" => #{
                        ~"enable" => true,
                        ~"verify" => ~"verify_none",
                        ~"cacertfile" => ~"/some/file"
                    }
                })
            )}
    ].
