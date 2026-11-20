%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_schema_secret_tests).

-include_lib("eunit/include/eunit.hrl").

secret(Value) ->
    emqx_schema_secret:convert_secret(Value, #{}).

deny_file_sources_test() ->
    Plain = secret(<<"plain">>),
    NotASecret = fun() -> {file, <<"file:///not/a/secret">>} end,
    Conf = #{
        server => <<"localhost:1883">>,
        username => "file://not-a-secret-field",
        password => secret(<<"file:///run/secrets/password">>),
        pool_size => 8,
        ssl => #{enable => true, password => secret("file://${EMQX_ETC_DIR}/key-password")},
        authentication => [
            #{mechanism => plain, password => Plain},
            #{mechanism => plain, password => secret(<<"file://relative/path">>)}
        ],
        callback => NotASecret
    },
    {NewConf, Sources} = emqx_schema_secret:deny_file_sources(Conf),
    ?assertEqual(Sources, emqx_schema_secret:file_sources(Conf)),
    ?assertEqual(
        [
            <<"file://${EMQX_ETC_DIR}/key-password">>,
            <<"file:///run/secrets/password">>,
            <<"file://relative/path">>
        ],
        lists:sort(Sources)
    ),
    #{
        password := Password,
        ssl := #{password := KeyPassword},
        authentication := [#{password := Password1}, #{password := Password2}]
    } = NewConf,
    %% Each secret that loads from a file is now an empty secret.
    ?assertEqual(<<>>, emqx_secret:unwrap(Password)),
    ?assertEqual(<<>>, emqx_secret:unwrap(KeyPassword)),
    ?assertEqual(<<>>, emqx_secret:unwrap(Password2)),
    %% Nothing else is changed.
    ?assertEqual(Plain, Password1),
    ?assertEqual(
        maps:without([password, ssl, authentication], Conf),
        maps:without([password, ssl, authentication], NewConf)
    ).

deny_file_sources_no_change_test() ->
    Conf = #{
        password => secret(<<"plain">>),
        list => [1, "string", <<"binary">>, atom, {tuple, 1}],
        improper => [a | b]
    },
    ?assertEqual({Conf, []}, emqx_schema_secret:deny_file_sources(Conf)),
    ?assertEqual([], emqx_schema_secret:file_sources(Conf)),
    ?assertEqual({undefined, []}, emqx_schema_secret:deny_file_sources(undefined)).

%% A validator reads the file of a `file://' secret only in a check of the global config.
unwrap_in_validator_test() ->
    File = filename:join("/tmp", ?MODULE_STRING ++ "-" ++ os:getpid()),
    ok = file:write_file(File, <<"from-file\n">>),
    try
        FileSecret = secret(iolist_to_binary(["file://", File])),
        Unwrap = fun(Secret, Opts) -> emqx_schema_secret:unwrap_in_validator(Secret, Opts) end,
        Global = emqx_schema_secret:check_opts(global),
        Namespace = emqx_schema_secret:check_opts(<<"ns1">>),
        %% A check with no options: at boot, of an API request body, in a dry run.
        NoReads = emqx_common_test_helpers:file_reads(fun() ->
            file = Unwrap(FileSecret, #{}),
            file = Unwrap(FileSecret, #{maybe_obfuscated => true}),
            file = Unwrap(FileSecret, Namespace)
        end),
        ?assertEqual([], NoReads),
        Reads = emqx_common_test_helpers:file_reads(fun() ->
            {value, <<"from-file">>} = Unwrap(FileSecret, Global)
        end),
        ?assertEqual([File], Reads),
        %% A secret that is not from a file has its value in each check.
        Plain = secret(<<"plain">>),
        ?assertEqual({value, <<"plain">>}, Unwrap(Plain, #{})),
        ?assertEqual({value, <<"plain">>}, Unwrap(Plain, Global)),
        ?assertEqual({value, <<"raw">>}, Unwrap(<<"raw">>, #{}))
    after
        file:delete(File)
    end.
