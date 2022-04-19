%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_test_lib).

-compile(nowarn_export_all).
-compile(export_all).

-define(DEFAULT_LICENSE_VALUES, [
    "220111",
    "0",
    "10",
    "Foo",
    "contact@foo.com",
    "20220111",
    "100000",
    "10"
]).

-define(DEFAULT_LICENSE_FILE, "emqx.lic").

private_key() ->
    test_key("pvt.key").

public_key() ->
    test_key("pub.pem").

public_key_pem() ->
    test_key("pub.pem", pem).

test_key(Filename) ->
    test_key(Filename, decoded).

legacy_license() ->
    test_key("emqx.lic", pem).

test_key(Filename, Format) ->
    Dir = code:lib_dir(emqx_license, test),
    Path = filename:join([Dir, "data", Filename]),
    {ok, KeyData} = file:read_file(Path),
    case Format of
        pem ->
            KeyData;
        decoded ->
            [PemEntry] = public_key:pem_decode(KeyData),
            public_key:pem_entry_decode(PemEntry)
    end.

make_license(Values) ->
    Key = private_key(),
    Text = string:join(Values, "\n"),
    EncodedText = base64:encode(Text),
    Signature = public_key:sign(Text, sha256, Key),
    EncodedSignature = base64:encode(Signature),
    iolist_to_binary([EncodedText, ".", EncodedSignature]).

default_license() ->
    %% keep it the same as in etc/emqx_license.conf
    License =
        "MjIwMTExCjAKMTAKRXZhbHVhdGlvbgpjb250YWN0QGVtcXguaW8KZGVmYXVsdAoyMDIyMDQxOQoxODI1CjEwMDAK."
        "MEQCICbgRVijCQov2hrvZXR1mk9Oa+tyV1F5oJ6iOZeSHjnQAiB9dUiVeaZekDOjztk+NCWjhk4PG8tWfw2uFZWruSzD6g==",
    ok = file:write_file(?DEFAULT_LICENSE_FILE, License),
    ?DEFAULT_LICENSE_FILE.
