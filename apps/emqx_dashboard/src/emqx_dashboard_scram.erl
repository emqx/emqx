%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc The fixed SCRAM-SHA-256 profile used by Dashboard login.
%%
%% The HTTP layer transports the fields as JSON, but the values below are
%% deliberately constructed as the canonical SCRAM message fragments.  JSON
%% serialization must never become part of AuthMessage.

-module(emqx_dashboard_scram).

-export([
    from_pwdhash/1,
    client_proof/6,
    verify/5,
    server_first/4
]).

-define(SHA256_BYTES, 32).
-define(MIN_ITERATIONS, 4096).
-define(MAX_ITERATIONS, 10000000).

-type verifier() :: #{
    iterations := pos_integer(),
    salt := binary(),
    salted_password := binary(),
    stored_key := binary(),
    server_key := binary()
}.

-spec from_pwdhash(binary()) -> {ok, verifier()} | {error, unsupported | malformed}.
from_pwdhash(<<"$1$", Rest/binary>>) ->
    case binary:split(Rest, <<"$">>, [global]) of
        [IterationBin, SaltBin, SaltedPasswordBin] ->
            try
                Iterations = binary_to_integer(IterationBin),
                Salt = base64:decode(SaltBin, #{padding => false}),
                SaltedPassword = base64:decode(SaltedPasswordBin, #{padding => false}),
                true = valid_iterations(Iterations),
                true = byte_size(Salt) =:= 16,
                true = byte_size(SaltedPassword) =:= ?SHA256_BYTES,
                {ok, verifier_from_salted_password(Iterations, Salt, SaltedPassword)}
            catch
                _:_ -> {error, malformed}
            end;
        _ ->
            {error, malformed}
    end;
from_pwdhash(<<_:36/binary>>) ->
    %% The pre-6.4 Dashboard hash cannot be used as SCRAM SaltedPassword.
    {error, unsupported};
from_pwdhash(_) ->
    {error, malformed}.

-spec server_first(binary(), binary(), binary(), pos_integer()) -> binary().
server_first(ClientNonce, ServerNonce, Salt, Iterations) ->
    CombinedNonce = <<ClientNonce/binary, ServerNonce/binary>>,
    iolist_to_binary([
        <<"r=">>,
        CombinedNonce,
        <<",s=">>,
        base64:encode(Salt),
        <<",i=">>,
        integer_to_binary(Iterations)
    ]).

-spec client_proof(binary(), binary(), binary(), binary(), pos_integer(), binary()) -> binary().
client_proof(Username, ClientNonce, ServerNonce, Salt, Iterations, Password) ->
    SaltedPassword = crypto:pbkdf2_hmac(sha256, Password, Salt, Iterations, ?SHA256_BYTES),
    ClientKey = hmac(SaltedPassword, <<"Client Key">>),
    StoredKey = crypto:hash(sha256, ClientKey),
    AuthMessage = auth_message(Username, ClientNonce, ServerNonce, Salt, Iterations),
    ClientSignature = hmac(StoredKey, AuthMessage),
    crypto:exor(ClientKey, ClientSignature).

-spec verify(verifier(), binary(), binary(), binary(), binary()) ->
    {ok, binary()} | {error, invalid_proof}.
verify(
    #{
        stored_key := StoredKey,
        server_key := ServerKey,
        salt := Salt,
        iterations := Iterations
    },
    Username,
    ClientNonce,
    ServerNonce,
    ClientProof
) when byte_size(ClientProof) =:= ?SHA256_BYTES ->
    AuthMessage = auth_message(Username, ClientNonce, ServerNonce, Salt, Iterations),
    ClientSignature = hmac(StoredKey, AuthMessage),
    RecoveredClientKey = crypto:exor(ClientProof, ClientSignature),
    case crypto:hash_equals(StoredKey, crypto:hash(sha256, RecoveredClientKey)) of
        true ->
            {ok, hmac(ServerKey, AuthMessage)};
        false ->
            {error, invalid_proof}
    end;
verify(_, _, _, _, _) ->
    {error, invalid_proof}.

verifier_from_salted_password(Iterations, Salt, SaltedPassword) ->
    ClientKey = hmac(SaltedPassword, <<"Client Key">>),
    #{
        iterations => Iterations,
        salt => Salt,
        salted_password => SaltedPassword,
        stored_key => crypto:hash(sha256, ClientKey),
        server_key => hmac(SaltedPassword, <<"Server Key">>)
    }.

auth_message(Username, ClientNonce, ServerNonce, Salt, Iterations) ->
    %% The username is escaped only for the SCRAM client-first-message-bare.
    %% Credential lookup and the username stored in the challenge remain raw.
    EscapedUsername = escape_username(Username),
    CombinedNonce = <<ClientNonce/binary, ServerNonce/binary>>,
    ClientFirstBare = <<"n=", EscapedUsername/binary, ",r=", ClientNonce/binary>>,
    ServerFirst = server_first(ClientNonce, ServerNonce, Salt, Iterations),
    ClientFinalWithoutProof = <<"c=biws,r=", CombinedNonce/binary>>,
    iolist_to_binary([
        ClientFirstBare,
        <<",">>,
        ServerFirst,
        <<",">>,
        ClientFinalWithoutProof
    ]).

escape_username(Username) ->
    %% RFC 5802 saslname escaping.  Replace '=' before ',' so the '='
    %% introduced by comma escaping is not escaped again.
    EscapedEquals = binary:replace(Username, <<"=">>, <<"=3D">>, [global]),
    binary:replace(EscapedEquals, <<",">>, <<"=2C">>, [global]).

hmac(Key, Data) ->
    crypto:mac(hmac, sha256, Key, Data).

valid_iterations(Iterations) ->
    Iterations >= ?MIN_ITERATIONS andalso Iterations =< ?MAX_ITERATIONS.
