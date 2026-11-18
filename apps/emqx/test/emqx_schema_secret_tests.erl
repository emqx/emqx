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
