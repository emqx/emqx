%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_grpc_tests).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("../src/emqx_bridge_gcp_pubsub_consumer_grpc.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

fmt(Fmt, Ctx) -> emqx_bridge_v2_testlib:fmt(Fmt, Ctx).

connector_config(Overrides) ->
    Defaults = #{
        ~"enable" => true,
        ~"description" => ~"my connector",
        ~"tags" => [~"some", ~"tags"],
        ~"authentication" => #{~"type" => ~"attached_service_account"},
        ~"url" => ~"please override",
        ~"pool_size" => 1,
        ~"connect_timeout" => ~"5s",
        ~"ssl" => #{~"enable" => false},
        ~"resource_opts" =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, ~"x", InnerConfigMap).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Any URL accepted by the schema must be also accepted by `grpc_client_sup:spec/3`.

They must have scheme, host and port.
""".
url_test_() ->
    Title = fun(Case) ->
        #{url := URL} = Case,
        IsValid =
            case maps:get(valid, Case, true) of
                true -> ~"valid";
                false -> ~"invalid"
            end,
        fmt(~"${u} (${v})", #{u => URL, v => IsValid})
    end,
    CheckSchema = fun(URL) -> connector_config(#{~"url" => URL}) end,
    Cases = [
        #{url => ~"just-host", valid => false},
        #{url => ~"no-scheme:443", valid => false},
        #{url => ~"http://no-port", valid => false},
        #{url => ~"https://no-port", valid => false},
        %% no commas
        #{url => ~"https://server1:443,", valid => false},
        %% single server
        #{url => ~"https://server1:443,https://server2:443", valid => false},
        #{url => ~"pulsar://bad-scheme:443", valid => false},
        #{url => ~"http://plain-http:8080", valid => true},
        #{url => ~"https://uses-tls:8181", valid => true}
    ],
    CheckSpec = fun(URL) -> grpc_client_sup:spec(~"child name", URL, _Opts = #{}) end,
    Test =
        fun
            (#{valid := false, url := URL}) ->
                ?_assertThrow(
                    {_SchemaMod, [
                        #{
                            kind := validation_error,
                            path := "connectors.gcp_pubsub_consumer_grpc.x.url"
                        }
                    ]},
                    CheckSchema(URL)
                );
            (#{url := URL}) ->
                ?_test(begin
                    ?assertMatch(#{}, CheckSchema(URL)),
                    ?assertMatch({ok, _}, CheckSpec(URL))
                end)
        end,
    [
        {Title(Case), Test(Case)}
     || Case <- Cases
    ].
