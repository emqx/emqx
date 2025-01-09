%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_test_lib).

-compile(nowarn_export_all).
-compile(export_all).

private_key() ->
    test_key("pvt.key").

public_key() ->
    test_key("pub.pem").

public_key_pem() ->
    test_key("pub.pem", pem).

test_key(Filename) ->
    test_key(Filename, decoded).

test_key(Filename, Format) ->
    Dir = code:lib_dir(emqx_license),
    Path = filename:join([Dir, "test", "data", Filename]),
    {ok, KeyData} = file:read_file(Path),
    case Format of
        pem ->
            KeyData;
        decoded ->
            [PemEntry] = public_key:pem_decode(KeyData),
            public_key:pem_entry_decode(PemEntry)
    end.

make_license(Values0 = #{}) ->
    Defaults = #{
        license_format => "220111",
        license_type => "0",
        customer_type => "10",
        name => "Foo",
        email => "contact@foo.com",
        deployment => "bar-deployment",
        start_date => "20220111",
        days => "100000",
        max_connections => "10"
    },
    Values1 = maps:merge(Defaults, Values0),
    Keys = [
        license_format,
        license_type,
        customer_type,
        name,
        email,
        deployment,
        start_date,
        days,
        max_connections
    ],
    Values = lists:map(fun(K) -> maps:get(K, Values1) end, Keys),
    make_license(Values);
make_license(Values) ->
    Key = private_key(),
    Text = string:join(Values, "\n"),
    EncodedText = base64:encode(Text),
    Signature = public_key:sign(Text, sha256, Key),
    EncodedSignature = base64:encode(Signature),
    iolist_to_binary([EncodedText, ".", EncodedSignature]).

default_test_license() ->
    make_license(#{}).

default_license() ->
    emqx_license_schema:default_license().

mock_parser() ->
    meck:new(emqx_license_parser, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_license_parser, pubkey, fun() -> public_key_pem() end),
    meck:expect(emqx_license_parser, default, fun() -> default_test_license() end),
    ok.

unmock_parser() ->
    meck:unload(emqx_license_parser),
    ok.
