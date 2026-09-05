%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Dashboard login protocol orchestration.
%%
%% The HTTP module is intentionally a thin adapter.  This module owns the
%% challenge lifecycle and keeps SCRAM, MFA, lockout, password policy and
%% token issuance in one login seam.

-module(emqx_dashboard_login).

-behaviour(gen_server).

-include("emqx_dashboard.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    create_tables/0,
    start_link/0,
    scram_challenge/2,
    scram_verify/4,
    password_login_enabled/0,
    owner/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TAB, emqx_dashboard_scram_challenge).
-define(META_TAB, emqx_dashboard_scram_meta).
-define(MOCK_SALT_SECRET_KEY, mock_salt_secret).
-define(MOCK_SALT_CONTEXT, <<"dashboard-scram-mock-salt:">>).
-define(CHALLENGE_TTL_MS, 60 * 1000).
-define(SCRAM_ITERATIONS, 600000).
-define(CLEANUP_MS, 30 * 1000).
-define(MAX_CHALLENGES, 100000).
-define(MAX_CHALLENGES_PER_USERNAME, 16).
-define(MIN_NONCE_BYTES, 20).
-define(MAX_CLIENT_NONCE_BYTES, 128).
-define(MAX_COMBINED_NONCE_BYTES, 160).

-type challenge() :: #emqx_dashboard_scram_challenge{}.

-spec create_tables() -> [?TAB | ?META_TAB].
create_tables() ->
    ok = mria:create_table(?TAB, [
        {type, set},
        {rlog_shard, ?DASHBOARD_SHARD},
        {storage, ram_copies},
        {record_name, emqx_dashboard_scram_challenge},
        {attributes, record_info(fields, emqx_dashboard_scram_challenge)},
        {storage_properties, [{ets, [{read_concurrency, true}, {write_concurrency, true}]}]}
    ]),
    ok = mria:create_table(?META_TAB, [
        {type, set},
        {rlog_shard, ?DASHBOARD_SHARD},
        {storage, disc_copies},
        {record_name, emqx_dashboard_scram_meta},
        {attributes, record_info(fields, emqx_dashboard_scram_meta)}
    ]),
    [?TAB, ?META_TAB].

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec password_login_enabled() -> boolean().
password_login_enabled() ->
    password_login_mode() =:= both.

password_login_mode() ->
    emqx:get_config([dashboard, password_login], both).

-spec scram_challenge(binary(), binary()) ->
    {ok, map()}
    | {error,
        bad_request
        | capacity
        | challenge_id_collision
        | {storage_unavailable, term()}}.
scram_challenge(Username, ClientNonce) ->
    case is_binary(Username) andalso valid_client_nonce(ClientNonce) of
        false ->
            {error, bad_request};
        true ->
            do_scram_challenge(Username, ClientNonce)
    end.

do_scram_challenge(Username, ClientNonce) ->
    ServerNonce = nonce(),
    case lookup_credential(Username) of
        {ok, User, Verifier} ->
            create_challenge(
                Username,
                ClientNonce,
                ServerNonce,
                maps:get(salt, Verifier),
                maps:get(iterations, Verifier),
                credential_fingerprint(User)
            );
        {error, unsupported} ->
            log_password_migration_required(Username),
            create_fake_challenge(Username, ClientNonce, ServerNonce);
        {error, not_found} ->
            create_fake_challenge(Username, ClientNonce, ServerNonce)
    end.

-spec scram_verify(binary(), binary(), binary(), term()) -> {ok, map()} | {error, term()}.
scram_verify(ChallengeId, CombinedNonce, ClientProof, MfaToken) ->
    case {valid_id(ChallengeId), valid_combined_nonce(CombinedNonce), is_binary(ClientProof)} of
        {true, true, true} when byte_size(ClientProof) =:= 32 ->
            do_scram_verify(ChallengeId, CombinedNonce, ClientProof, MfaToken);
        _ ->
            {error, bad_request}
    end.

do_scram_verify(ChallengeId, CombinedNonce, ClientProof, MfaToken) ->
    Now = erlang:system_time(millisecond),
    case take(ChallengeId) of
        {error, not_found} ->
            %% Unknown, replayed, and stateless fake challenges all fail as
            %% authentication failures.  The client does not need to know
            %% which kind of challenge was missing.
            {error, password_error};
        {error, {storage_unavailable, _Reason} = Error} ->
            {error, Error};
        {ok, #emqx_dashboard_scram_challenge{expires_at = ExpiresAt} = Challenge} ->
            case ExpiresAt =< Now of
                true -> {error, invalid_challenge};
                false -> verify_challenge(Challenge, CombinedNonce, ClientProof, MfaToken)
            end
    end.

verify_challenge(
    #emqx_dashboard_scram_challenge{
        username = Username,
        client_nonce = ClientNonce,
        server_nonce = ServerNonce,
        credential_fingerprint = Fingerprint
    },
    CombinedNonce,
    ClientProof,
    MfaToken
) ->
    case CombinedNonce =:= <<ClientNonce/binary, ServerNonce/binary>> of
        false ->
            register_password_failure(Username),
            {error, password_error};
        true ->
            verify_challenge_credential(
                Username,
                Fingerprint,
                ClientNonce,
                ServerNonce,
                ClientProof,
                MfaToken
            )
    end.

verify_challenge_credential(Username, Fingerprint, ClientNonce, ServerNonce, ClientProof, MfaToken) ->
    case lookup_credential(Username) of
        {ok, User, Verifier} ->
            verify_challenge_credential(
                User,
                Verifier,
                Fingerprint,
                ClientNonce,
                ServerNonce,
                ClientProof,
                MfaToken
            );
        _ ->
            password_error(Username)
    end.

verify_challenge_credential(
    User,
    Verifier,
    Fingerprint,
    ClientNonce,
    ServerNonce,
    ClientProof,
    MfaToken
) ->
    case Fingerprint =:= credential_fingerprint(User) of
        true -> verify_proof(User, Verifier, ClientNonce, ServerNonce, ClientProof, MfaToken);
        false -> password_error(User#?ADMIN.username)
    end.

verify_proof(User, Verifier, ClientNonce, ServerNonce, ClientProof, MfaToken) ->
    case
        emqx_dashboard_scram:verify(
            Verifier, User#?ADMIN.username, ClientNonce, ServerNonce, ClientProof
        )
    of
        {ok, ServerSignature} -> complete_login(User, MfaToken, ServerSignature);
        {error, invalid_proof} -> password_error(User#?ADMIN.username)
    end.

password_error(Username) ->
    register_password_failure(Username),
    {error, password_error}.
complete_login(User, MfaToken, ServerSignature) ->
    case emqx_dashboard_admin:complete_scram_login(User, MfaToken) of
        {ok, Result} ->
            ok = emqx_dashboard_login_lock:reset(User#?ADMIN.username),
            {ok, Result#{server_signature => base64:encode(ServerSignature)}};
        Error ->
            Error
    end.

lookup_credential(Username) ->
    case emqx_dashboard_admin:lookup_user(Username) of
        [#?ADMIN{pwdhash = PwdHash} = User] ->
            case emqx_dashboard_scram:from_pwdhash(PwdHash) of
                {ok, Verifier} -> {ok, User, Verifier};
                {error, unsupported} -> {error, unsupported};
                {error, malformed} -> {error, unsupported}
            end;
        [] ->
            {error, not_found}
    end.

create_fake_challenge(Username, _ClientNonce, ServerNonce) ->
    %% Keep unknown and legacy users on the same stateless challenge path so
    %% the server does not disclose credential state. Fake challenges never
    %% issue a token and therefore must not consume challenge-table capacity.
    Salt = mock_salt(Username),
    {ok, #{
        mechanism => <<"SCRAM-SHA-256">>,
        challenge_id => id(),
        salt => base64:encode(Salt),
        iterations => ?SCRAM_ITERATIONS,
        server_nonce => ServerNonce
    }}.

create_challenge(Username, ClientNonce, ServerNonce, Salt, Iterations, Fingerprint) ->
    Id = id(),
    Challenge = #emqx_dashboard_scram_challenge{
        id = Id,
        username = Username,
        client_nonce = ClientNonce,
        server_nonce = ServerNonce,
        salt = Salt,
        iterations = Iterations,
        credential_fingerprint = Fingerprint,
        expires_at = erlang:system_time(millisecond) + ?CHALLENGE_TTL_MS
    },
    case put(Challenge) of
        ok ->
            {ok, #{
                mechanism => <<"SCRAM-SHA-256">>,
                challenge_id => Id,
                salt => base64:encode(Salt),
                iterations => Iterations,
                server_nonce => ServerNonce
            }};
        {error, capacity} ->
            {error, capacity};
        {error, Reason} ->
            {error, Reason}
    end.

register_password_failure(Username) when is_binary(Username) ->
    emqx_dashboard_login_lock:register_unsuccessful_login(Username);
register_password_failure(_) ->
    ok.

credential_fingerprint(#?ADMIN{pwdhash = PwdHash}) ->
    crypto:hash(sha256, PwdHash).

mock_salt(Username) ->
    Secret = mock_salt_secret(),
    Digest = crypto:mac(hmac, sha256, Secret, <<?MOCK_SALT_CONTEXT/binary, Username/binary>>),
    binary:part(Digest, 0, 16).

mock_salt_secret() ->
    case ets:lookup(?META_TAB, ?MOCK_SALT_SECRET_KEY) of
        [#emqx_dashboard_scram_meta{value = Secret}] ->
            Secret;
        [] ->
            error({scram_meta_not_found, ?MOCK_SALT_SECRET_KEY})
    end.

log_password_migration_required(Username) ->
    Event = #{
        msg => "dashboard_scram_password_migration_required",
        username => Username,
        migration_command => iolist_to_binary([
            <<"emqx ctl admins passwd ">>,
            Username,
            <<" <new-password>">>
        ])
    },
    case password_login_mode() of
        scram_only ->
            ?SLOG(error, Event#{password_login_mode => scram_only});
        both ->
            ?SLOG(warning, Event#{password_login_mode => both});
        Mode ->
            ?SLOG(warning, Event#{password_login_mode => Mode})
    end.

nonce() ->
    base64:encode(crypto:strong_rand_bytes(24), #{mode => urlsafe, padding => false}).

id() ->
    base64:encode(crypto:strong_rand_bytes(24), #{mode => urlsafe, padding => false}).

valid_id(Id) ->
    valid_nonce(Id, ?MIN_NONCE_BYTES, ?MAX_CLIENT_NONCE_BYTES).

valid_client_nonce(Nonce) ->
    valid_nonce(Nonce, ?MIN_NONCE_BYTES, ?MAX_CLIENT_NONCE_BYTES).

valid_combined_nonce(Nonce) ->
    valid_nonce(Nonce, ?MIN_NONCE_BYTES, ?MAX_COMBINED_NONCE_BYTES).

valid_nonce(Bin, MinBytes, MaxBytes) when is_binary(Bin) ->
    byte_size(Bin) >= MinBytes andalso byte_size(Bin) =< MaxBytes andalso
        lists:all(fun valid_nonce_char/1, binary_to_list(Bin));
valid_nonce(_, _, _) ->
    false.

valid_nonce_char(C) when C >= $A, C =< $Z -> true;
valid_nonce_char(C) when C >= $a, C =< $z -> true;
valid_nonce_char(C) when C >= $0, C =< $9 -> true;
valid_nonce_char($_) -> true;
valid_nonce_char($-) -> true;
valid_nonce_char(_) -> false.

%%--------------------------------------------------------------------
%% Challenge state implementation
%%--------------------------------------------------------------------

-spec put(challenge()) ->
    ok | {error, capacity | challenge_id_collision | {storage_unavailable, term()}}.
put(#emqx_dashboard_scram_challenge{username = Username} = Challenge) ->
    case ets:info(?TAB, size) of
        Size when is_integer(Size), Size >= ?MAX_CHALLENGES ->
            {error, capacity};
        Size when is_integer(Size) ->
            case local_username_challenge_count(Username) >= ?MAX_CHALLENGES_PER_USERNAME of
                true -> {error, capacity};
                false -> put_if_available(Challenge)
            end;
        undefined ->
            {error, {storage_unavailable, table_not_found}}
    end.

local_username_challenge_count(Username) ->
    ets:select_count(?TAB, [
        {
            {emqx_dashboard_scram_challenge, '_', Username, '_', '_', '_', '_', '_', '_', '_'},
            [],
            [true]
        }
    ]).

put_if_available(#emqx_dashboard_scram_challenge{id = Id} = Challenge) ->
    Result = mria:sync_transaction(?DASHBOARD_SHARD, fun() -> put_transaction(Id, Challenge) end),
    format_put_result(Result).

put_transaction(Id, Challenge) ->
    case mnesia:read(?TAB, Id, write) of
        [] ->
            mnesia:write(Challenge),
            ok;
        _ ->
            mnesia:abort(challenge_id_collision)
    end.

format_put_result({atomic, ok}) ->
    ok;
format_put_result({aborted, challenge_id_collision}) ->
    {error, challenge_id_collision};
format_put_result({aborted, Reason}) ->
    {error, {storage_unavailable, Reason}};
format_put_result({timeout, {atomic, ok}}) ->
    ok;
format_put_result({timeout, {aborted, challenge_id_collision}}) ->
    {error, challenge_id_collision};
format_put_result({timeout, {aborted, Reason}}) ->
    {error, {storage_unavailable, Reason}};
format_put_result({timeout, {error, Reason}}) ->
    {error, {storage_unavailable, Reason}}.

-spec take(binary()) ->
    {ok, challenge()}
    | {error, not_found | {storage_unavailable, term()}}.
take(Id) ->
    TransactionResult = mria:sync_transaction(
        ?DASHBOARD_SHARD,
        fun() -> take_transaction(Id) end
    ),
    case TransactionResult of
        {atomic, Value} -> Value;
        {aborted, Reason} -> {error, {storage_unavailable, Reason}};
        {timeout, {atomic, Value}} -> Value;
        {timeout, {aborted, Reason}} -> {error, {storage_unavailable, Reason}};
        {timeout, {error, Reason}} -> {error, {storage_unavailable, Reason}}
    end.

take_transaction(Id) ->
    case mnesia:read(?TAB, Id, write) of
        [Challenge] ->
            ok = mnesia:delete({?TAB, Id}),
            {ok, Challenge};
        [] ->
            {error, not_found}
    end.

-spec owner(binary()) -> {ok, binary()} | {error, not_found}.
owner(Id) ->
    case mria:ro_transaction(?DASHBOARD_SHARD, fun() -> mnesia:read(?TAB, Id) end) of
        {atomic, [#emqx_dashboard_scram_challenge{username = Username}]} ->
            {ok, Username};
        _ ->
            {error, not_found}
    end.

init([]) ->
    case ensure_mock_salt_secret() of
        ok ->
            schedule_cleanup(),
            {ok, state};
        {error, Reason} ->
            {stop, Reason}
    end.

ensure_mock_salt_secret() ->
    Result = mria:sync_transaction(?DASHBOARD_SHARD, fun() ->
        case mnesia:read(?META_TAB, ?MOCK_SALT_SECRET_KEY, write) of
            [#emqx_dashboard_scram_meta{value = _Secret}] ->
                ok;
            [] ->
                Secret = crypto:strong_rand_bytes(32),
                ok = mnesia:write(#emqx_dashboard_scram_meta{
                    key = ?MOCK_SALT_SECRET_KEY,
                    value = Secret
                }),
                ok
        end
    end),
    case Result of
        {atomic, ok} -> ok;
        {timeout, {atomic, ok}} -> ok;
        {aborted, Reason} -> {error, {storage_unavailable, Reason}};
        {timeout, {aborted, Reason}} -> {error, {storage_unavailable, Reason}};
        {timeout, {error, Reason}} -> {error, {storage_unavailable, Reason}}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(cleanup, State) ->
    cleanup_expired(erlang:system_time(millisecond)),
    schedule_cleanup(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

schedule_cleanup() ->
    _ = erlang:send_after(?CLEANUP_MS, self(), cleanup),
    ok.

cleanup_expired(Now) ->
    Spec = [
        {
            {emqx_dashboard_scram_challenge, '_', '_', '_', '_', '_', '_', '_', '$1', '_'},
            [{'<', '$1', Now}],
            ['$_']
        }
    ],
    case
        mria:ro_transaction(?DASHBOARD_SHARD, fun() ->
            mnesia:select(?TAB, Spec)
        end)
    of
        {atomic, []} ->
            ok;
        {atomic, Challenges} ->
            _ = mria:async_dirty(?DASHBOARD_SHARD, fun() ->
                lists:foreach(
                    fun(#emqx_dashboard_scram_challenge{id = Id}) ->
                        mria:dirty_delete(?TAB, Id)
                    end,
                    Challenges
                )
            end),
            ok;
        _ ->
            ok
    end.
