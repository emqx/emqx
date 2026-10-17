%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_revocation).
-moduledoc """
Hard-coded list of revoked licenses.

Each entry is the lowercase hex SHA-1 of a license payload.
The payload is the base64-decoded part of the license key before the `.`.
It is not the base64 text itself.
""".

-export([check/1, revoked/0]).

-doc """
Check a decoded license payload against the revocation list.
Return `ok` when the payload is not revoked.
Return `{revoked, Sha1Hex}` when the payload is revoked.
""".
-spec check(binary()) -> ok | {revoked, binary()}.
check(Payload) when is_binary(Payload) ->
    Hash = binary:encode_hex(crypto:hash(sha, Payload), lowercase),
    case lists:member(Hash, revoked()) of
        true -> {revoked, Hash};
        false -> ok
    end.

-doc "Return the SHA-1 hashes (lowercase hex) of the revoked license payloads.".
-spec revoked() -> [binary()].
revoked() ->
    [
        %% Revoked on 2026-10-16.
        <<"7ea1f7381e077d3c5ad1727ada9382b85b33c196">>
    ].
