%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_token).

-include("emqx_dashboard.hrl").

-export([create_tables/0]).

-export([
    sign/1,
    verify/3,
    lookup/1,
    owner/1,
    destroy/1,
    destroy_by_username/1
]).

-ifdef(TEST).
-export([lookup_by_username/1, clean_expired_jwt/1]).
-endif.

-define(TAB, ?ADMIN_JWT).
-define(EXPTIME, 60 * 60 * 1000).

%%--------------------------------------------------------------------
%% gen server part
-behaviour(gen_server).

-export([start_link/0, salt/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%--------------------------------------------------------------------
%% jwt function

-spec verify(emqx_dashboard:request(), emqx_dashboard:handler_info(), Token :: binary()) ->
    Result ::
        {ok, binary()}
        | {error, token_timeout | not_found | unauthorized_role}.
verify(Req, HandlerInfo, Token) ->
    do_verify(Req, HandlerInfo, Token).

-spec destroy(KeyOrKeys :: list() | binary() | #?ADMIN_JWT{}) -> ok.
destroy([]) ->
    ok;
destroy(JWTorTokenList) when is_list(JWTorTokenList) ->
    lists:foreach(fun destroy/1, JWTorTokenList);
destroy(#?ADMIN_JWT{token = Token}) ->
    destroy(Token);
destroy(Token) when is_binary(Token) ->
    do_destroy(Token).

-spec destroy_by_username(Username :: binary()) -> ok.
destroy_by_username(Username) ->
    do_destroy_by_username(Username).

%% @doc create 4 bytes salt.
-spec salt() -> binary().
salt() ->
    <<X:16/big-unsigned-integer>> = crypto:strong_rand_bytes(2),
    iolist_to_binary(io_lib:format("~4.16.0b", [X])).

create_tables() ->
    ok = mria:create_table(?TAB, [
        {type, set},
        {rlog_shard, ?DASHBOARD_SHARD},
        {storage, disc_copies},
        {record_name, ?ADMIN_JWT},
        {attributes, record_info(fields, ?ADMIN_JWT)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    [?TAB].

-spec sign(User :: dashboard_user()) ->
    {ok, dashboard_user_role(), Token :: binary()}.
sign(#?ADMIN{username = Username} = User) ->
    ExpTime = jwt_expiration_time(),
    JWK = jwk(),
    JWS = #{
        <<"alg">> => <<"HS256">>
    },
    JWT = #{
        <<"iss">> => <<"EMQX">>,
        <<"exp">> => ExpTime
    },
    Signed = jose_jwt:sign(JWK, JWS, JWT),
    {_, Token} = jose_jws:compact(Signed),
    Role = emqx_dashboard_admin:role(User),
    JWTRec = format(Token, Username, Role, ExpTime),
    _ = mria:sync_transaction(?DASHBOARD_SHARD, fun mnesia:write/1, [JWTRec]),
    {ok, Role, Token}.

-spec do_verify(emqx_dashboard:request(), emqx_dashboard:handler_info(), Token :: binary()) ->
    Result ::
        {ok, binary()}
        | {error, token_timeout | not_found | unauthorized_role}.
do_verify(Req, HandlerInfo, Token) ->
    case lookup(Token) of
        {ok, JWT = #?ADMIN_JWT{exptime = ExpTime, extra = _Extra, username = _Username}} ->
            case ExpTime > erlang:system_time(millisecond) of
                true ->
                    check_rbac(Req, HandlerInfo, JWT);
                _ ->
                    {error, token_timeout}
            end;
        Error ->
            Error
    end.

do_destroy(Token) ->
    Fun = fun mnesia:delete/1,
    {atomic, ok} = mria:sync_transaction(?DASHBOARD_SHARD, Fun, [{?TAB, Token}]),
    ok.

do_destroy_by_username(Username) ->
    gen_server:cast(?MODULE, {destroy, Username}).

%%--------------------------------------------------------------------
%% jwt internal util function
-spec lookup(Token :: binary()) -> {ok, #?ADMIN_JWT{}} | {error, not_found}.
lookup(Token) ->
    Fun = fun() -> mnesia:read(?TAB, Token) end,
    case mria:ro_transaction(?DASHBOARD_SHARD, Fun) of
        {atomic, [JWT]} -> {ok, JWT};
        {atomic, []} -> {error, not_found}
    end.

lookup_by_username(Username) ->
    Spec = [{#?ADMIN_JWT{username = Username, _ = '_'}, [], ['$_']}],
    Fun = fun() -> mnesia:select(?TAB, Spec) end,
    {atomic, List} = mria:ro_transaction(?DASHBOARD_SHARD, Fun),
    List.

-spec owner(Token :: binary()) -> {ok, Username :: binary()} | {error, not_found}.
owner(Token) ->
    Fun = fun() -> mnesia:read(?TAB, Token) end,
    case mria:ro_transaction(?DASHBOARD_SHARD, Fun) of
        {atomic, [#?ADMIN_JWT{username = Username}]} -> {ok, Username};
        {atomic, []} -> {error, not_found}
    end.

jwk() ->
    Key = crypto:strong_rand_bytes(32),
    #{
        <<"kty">> => <<"oct">>,
        <<"k">> => jose_base64url:encode(Key)
    }.

jwt_expiration_time() ->
    erlang:system_time(millisecond) + token_ttl().

token_ttl() ->
    emqx_conf:get([dashboard, token_expired_time], ?EXPTIME).

format(Token, ?SSO_USERNAME(Backend, Name), Role, ExpTime) ->
    format(Token, Backend, Name, Role, ExpTime);
format(Token, Username, Role, ExpTime) ->
    format(Token, ?BACKEND_LOCAL, Username, Role, ExpTime).

format(Token, Backend, Username, Role, ExpTime) ->
    #?ADMIN_JWT{
        token = Token,
        username = Username,
        exptime = ExpTime,
        extra = #{role => Role, backend => Backend}
    }.

%%--------------------------------------------------------------------
%% gen server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    timer_clean(self()),
    {ok, state}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({destroy, Username}, State) ->
    Tokens = lookup_by_username(Username),
    destroy(Tokens),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(clean_jwt, State) ->
    timer_clean(self()),
    Now = erlang:system_time(millisecond),
    ok = clean_expired_jwt(Now),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

timer_clean(Pid) ->
    erlang:send_after(token_ttl(), Pid, clean_jwt).

clean_expired_jwt(Now) ->
    Spec = [{#?ADMIN_JWT{exptime = '$1', token = '$2', _ = '_'}, [{'<', '$1', Now}], ['$2']}],
    {atomic, JWTList} = mria:ro_transaction(
        ?DASHBOARD_SHARD,
        fun() -> mnesia:select(?TAB, Spec) end
    ),
    ok = destroy(JWTList).

check_rbac(Req, HandlerInfo, JWT) ->
    ActorContext = actor_context_of(JWT),
    case emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, ActorContext) of
        true ->
            save_new_jwt(JWT);
        _ ->
            {error, unauthorized_role}
    end.

save_new_jwt(OldJWT) ->
    #?ADMIN_JWT{exptime = _ExpTime, extra = _Extra, username = Username} = OldJWT,
    NewJWT = OldJWT#?ADMIN_JWT{exptime = jwt_expiration_time()},
    {atomic, Res} = mria:sync_transaction(
        ?DASHBOARD_SHARD,
        fun mnesia:write/1,
        [NewJWT]
    ),
    {Res, Username}.

actor_context_of(#?ADMIN_JWT{} = JWT) ->
    #?ADMIN_JWT{exptime = _ExpTime, extra = Extra, username = Username} = JWT,
    Role = role_of(Extra),
    #{
        actor => Username,
        role => Role
    }.

%% For compatibility
role_of([]) ->
    %% Very old record definition had `[]` as the default for `extra`....
    ?ROLE_SUPERUSER;
role_of(#{role := Role}) ->
    Role.
