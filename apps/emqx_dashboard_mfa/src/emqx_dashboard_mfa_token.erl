%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% @doc Use to store MFA tokens and other related contexts,
%% like verification code which has sent to an email or phone

-module(emqx_dashboard_mfa_token).

-feature(maybe_expr, enable).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([create_tables/0]).

-export([
    sign/3,
    verify/1,
    lookup/1,
    destroy/1
]).

-export_type([token/0, lazy_token/0, verify_resp/0, context/0]).

-define(TAB, emqx_dashboard_mfa_token).

-type token() :: binary().
-type lazy_token() :: fun((dashboard_user()) -> _).
-type context() :: emqx_dashboard_mfa:mfa_context().

-type verify_resp() :: #{
    username := dashboard_username(),
    lazy_token := lazy_token(),
    context := context()
}.

-record(?TAB, {
    token :: token(),
    username :: binary(),
    exptime :: integer(),
    lazy_token :: lazy_token(),
    context :: context(),
    extra = #{}
}).

-define(TOKEN_EXPIRATION_TIME, timer:minutes(5)).

%%--------------------------------------------------------------------
%% gen server part
-behaviour(gen_server).

-export([start_link/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%------------------------------------------------------------------------------
%% JWT API
%%------------------------------------------------------------------------------
-spec sign(Username :: dashboard_username(), Password :: binary(), Context :: context()) ->
    {ok, Token :: binary()} | {error, Reason :: term()}.
sign(Username, Password, Context) ->
    do_sign(Username, Password, Context).

-spec verify(Token :: binary()) ->
    Result ::
        {ok, verify_resp()}
        | {error, term()}.
verify(Token) ->
    do_verify(Token).

-spec destroy(Token :: binary()) -> ok.
destroy(Token) when is_binary(Token) ->
    do_destroy(Token).

%%------------------------------------------------------------------------------
%% mria
%%------------------------------------------------------------------------------
create_tables() ->
    ok = mria:create_table(?TAB, [
        {type, set},
        {rlog_shard, ?DASHBOARD_SHARD},
        {storage, disc_copies},
        {record_name, ?TAB},
        {attributes, record_info(fields, ?TAB)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    [?TAB].

%%------------------------------------------------------------------------------
%% Internal
%%------------------------------------------------------------------------------
do_sign(Username, Password, Context) ->
    ExpTime = jwt_expiration_time(),
    Salt = emqx_dashboard_token:salt(),
    JWK = emqx_dashboard_token:jwk(Username, Password, Salt),
    JWS = #{
        <<"alg">> => <<"HS256">>
    },
    JWT = #{
        <<"iss">> => <<"EMQX">>,
        <<"exp">> => ExpTime
    },
    Signed = jose_jwt:sign(JWK, JWS, JWT),
    {_, Token} = jose_jws:compact(Signed),
    LazyToken = fun(User) -> emqx_dashboard_admin:do_sign_token(User, Password) end,
    JWTRec = format(Token, Username, LazyToken, ExpTime, Context),
    _ = mria:sync_transaction(?DASHBOARD_SHARD, fun mnesia:write/1, [JWTRec]),
    {ok, Token}.

-spec do_verify(Token :: binary()) ->
    Result ::
        {ok, verify_resp()}
        | {error, token_timeout | not_found}.
do_verify(Token) ->
    maybe
        {ok, #?TAB{
            exptime = ExpTime, username = Username, lazy_token = LazyToken, context = Context
        }} ?=
            lookup(Token),
        true ?= ExpTime > erlang:system_time(millisecond),
        {ok, #{username => Username, lazy_token => LazyToken, context => Context}}
    else
        false ->
            {error, <<"token_timeout">>};
        Error ->
            Error
    end.

do_destroy(Token) ->
    Fun = fun mnesia:delete/1,
    {atomic, ok} = mria:sync_transaction(?DASHBOARD_SHARD, Fun, [{?TAB, Token}]),
    ok.

-spec lookup(Token :: binary()) -> {ok, #?TAB{}} | {error, not_found}.
lookup(Token) ->
    Fun = fun() -> mnesia:read(?TAB, Token) end,
    case mria:ro_transaction(?DASHBOARD_SHARD, Fun) of
        {atomic, [JWT]} -> {ok, JWT};
        {atomic, []} -> {error, <<"token_not_found">>}
    end.

jwt_expiration_time() ->
    erlang:system_time(millisecond) + ?TOKEN_EXPIRATION_TIME.

format(Token, Username, LazyToken, ExpTime, Context) ->
    #?TAB{
        token = Token,
        username = Username,
        exptime = ExpTime,
        lazy_token = LazyToken,
        context = Context
    }.

%%------------------------------------------------------------------------------
%% gen_server
%%------------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    tick_clean_token(self()),
    {ok, state}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(clean_token, State) ->
    tick_clean_token(self()),
    Now = erlang:system_time(millisecond),
    ok = clean_expired_token(Now),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

tick_clean_token(Pid) ->
    erlang:send_after(?TOKEN_EXPIRATION_TIME, Pid, clean_token).

-dialyzer({nowarn_function, clean_expired_token/1}).
clean_expired_token(Now) ->
    Spec = [{#?TAB{exptime = '$1', token = '$2', _ = '_'}, [{'<', '$1', Now}], ['$2']}],
    {atomic, Tokens} = mria:ro_transaction(
        ?DASHBOARD_SHARD,
        fun() -> mnesia:select(?TAB, Spec) end
    ),
    ok = destroy(Tokens).
