%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_resource_tests).

-include_lib("eunit/include/eunit.hrl").

%% Logger handler callback
-export([log/2]).

setup(Enable) ->
    persistent_term:put(
        {emqx_utils_ssrf, cache},
        #{
            enable => Enable,
            allow_cidrs => [],
            deny_cidrs => emqx_utils_ssrf:compile_cidrs(
                emqx_utils_ssrf:default_deny_cidrs()
            ),
            deny_hosts => []
        }
    ).

teardown() ->
    persistent_term:erase({emqx_utils_ssrf, cache}).

with_ssrf(Enable, Fun) ->
    setup(Enable),
    try
        Fun()
    after
        teardown()
    end.

parse_url_allows_public_host_test() ->
    with_ssrf(true, fun() ->
        ?assertMatch(
            {_, _},
            emqx_connector_resource:parse_url(<<"http://example.com:8080/path">>)
        )
    end).

parse_url_rejects_imds_test() ->
    with_ssrf(true, fun() ->
        ?assertThrow(
            #{kind := validation_error, reason := <<"Address resolves", _/binary>>},
            emqx_connector_resource:parse_url(<<"http://169.254.169.254/">>)
        )
    end).

parse_url_rejects_rfc1918_test() ->
    with_ssrf(true, fun() ->
        ?assertThrow(
            #{kind := validation_error},
            emqx_connector_resource:parse_url(<<"http://10.0.0.1:8080/">>)
        )
    end).

parse_url_allows_private_when_disabled_test() ->
    with_ssrf(false, fun() ->
        ?assertMatch(
            {_, _},
            emqx_connector_resource:parse_url(<<"http://10.0.0.1:8080/">>)
        )
    end).

%%--------------------------------------------------------------------
%% `file://' secrets in a namespace
%%--------------------------------------------------------------------

deny_secret_files_test_() ->
    Conf = #{
        server => <<"localhost:1883">>,
        username => <<"user">>,
        password => emqx_schema_secret:convert_secret(<<"file:///run/secrets/password">>, #{})
    },
    PlainConf = Conf#{password := emqx_schema_secret:convert_secret(<<"plain">>, #{})},
    LogMeta = #{config_root => connectors, type => <<"mqtt">>, name => <<"c1">>},
    %% `local': the log handler sends the reports to the process that runs the setup.
    {setup, local,
        fun() ->
            ok = logger:add_handler(?MODULE, ?MODULE, #{config => #{pid => self()}})
        end,
        fun(_) -> logger:remove_handler(?MODULE) end, [
            {"the global namespace keeps the secret", fun() ->
                ?assertEqual(
                    Conf, emqx_connector_resource:deny_secret_files(global, LogMeta, Conf)
                ),
                ?assertEqual([], denied_logs())
            end},
            {"a namespace gets an empty secret, and the log names the file", fun() ->
                NewConf = emqx_connector_resource:deny_secret_files(<<"ns1">>, LogMeta, Conf),
                ?assertEqual(<<>>, emqx_secret:unwrap(maps:get(password, NewConf))),
                ?assertEqual(maps:remove(password, Conf), maps:remove(password, NewConf)),
                ?assertMatch(
                    [
                        #{
                            namespace := <<"ns1">>,
                            paths := [<<"/run/secrets/password">>],
                            config_root := connectors,
                            type := <<"mqtt">>,
                            name := <<"c1">>
                        }
                    ],
                    denied_logs()
                )
            end},
            {"a namespace keeps a secret that is not a file", fun() ->
                ?assertEqual(
                    PlainConf,
                    emqx_connector_resource:deny_secret_files(<<"ns1">>, LogMeta, PlainConf)
                ),
                ?assertEqual([], denied_logs())
            end}
        ]}.

log(
    #{msg := {report, #{msg := "namespaced_secret_file_read_denied"} = Report}, level := error},
    #{config := #{pid := Pid}}
) ->
    Pid ! {namespaced_denied_log, Report},
    ok;
log(_Event, _Config) ->
    ok.

denied_logs() ->
    receive
        {namespaced_denied_log, Report} -> [Report | denied_logs()]
    after 0 ->
        []
    end.
