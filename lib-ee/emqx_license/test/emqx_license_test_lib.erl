%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_test_lib).

-compile(nowarn_export_all).
-compile(export_all).

-define(DEFAULT_LICENSE_VALUES,
        ["220111",
         "0",
         "10",
         "Foo",
         "contact@foo.com",
         "20220111",
         "100000",
         "10"]).

-define(DEFAULT_LICENSE_FILE, "emqx.lic").

private_key() ->
    test_key("pvt.key").

public_key() ->
    test_key("pub.pem").

public_key_encoded() ->
    public_key:der_encode('RSAPublicKey', public_key()).

test_key(Filename) ->
    Dir = code:lib_dir(emqx_license, test),
    Path = filename:join([Dir, "data", Filename]),
    {ok, KeyData} = file:read_file(Path),
    [PemEntry] = public_key:pem_decode(KeyData),
    public_key:pem_entry_decode(PemEntry).

make_license(Values) ->
    Key = private_key(),
    Text = string:join(Values, "\n"),
    EncodedText = base64:encode(Text),
    Signature = public_key:sign(Text, sha256, Key),
    EncodedSignature = base64:encode(Signature),
    iolist_to_binary([EncodedText, ".", EncodedSignature]).

default_license() ->
    License = make_license(?DEFAULT_LICENSE_VALUES),
    ok = file:write_file(?DEFAULT_LICENSE_FILE, License),
    ?DEFAULT_LICENSE_FILE.
