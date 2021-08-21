%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_dashboard_token).

-include("emqx_dashboard.hrl").

-define(TAB, mqtt_admin_jwt).

-export([ sign/2
        , verify/1
        , destroy/1
        , destroy_by_username/1
        ]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([mnesia/1]).

-define(EXPTIME, 60 * 60 * 1000).

-define(CLEAN_JWT_INTERVAL, 60 * 60 * 1000).

%%--------------------------------------------------------------------
%% gen server part
-behaviour(gen_server).

-export([start_link/0]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%%--------------------------------------------------------------------
%% jwt function
-spec(sign(Username :: binary(), Password :: binary()) ->
        {ok, Token :: binary()} | {error, Reason :: term()}).
sign(Username, Password) ->
    do_sign(Username, Password).

-spec(verify(Token :: binary()) -> Result :: ok | {error, token_timeout | not_found}).
verify(Token) ->
    do_verify(Token).

-spec(destroy(KeyOrKeys :: list() | binary() | #mqtt_admin_jwt{}) -> ok).
destroy([]) ->
    ok;
destroy(JWTorTokenList) when is_list(JWTorTokenList)->
    [destroy(JWTorToken) || JWTorToken <- JWTorTokenList],
    ok;
destroy(#mqtt_admin_jwt{token = Token}) ->
    destroy(Token);
destroy(Token) when is_binary(Token)->
    do_destroy(Token).

-spec(destroy_by_username(Username :: binary()) -> ok).
destroy_by_username(Username) ->
    do_destroy_by_username(Username).

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, set},
                {rlog_shard, ?DASHBOARD_SHARD},
                {disc_copies, [node()]},
                {record_name, mqtt_admin_jwt},
                {attributes, record_info(fields, mqtt_admin_jwt)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB, disc_copies).

%%--------------------------------------------------------------------
%% jwt apply
do_sign(Username, Password) ->
    ExpTime = jwt_expiration_time(),
    Salt = salt(),
    JWK = jwk(Username, Password, Salt),
    JWS = #{
        <<"alg">> => <<"HS256">>
    },
    JWT = #{
        <<"iss">> => <<"EMQ X">>,
        <<"exp">> => ExpTime
    },
    Signed = jose_jwt:sign(JWK, JWS, JWT),
    {_, Token} = jose_jws:compact(Signed),
    ok = ekka_mnesia:dirty_write(format(Token, Username, ExpTime)),
    {ok, Token}.

do_verify(Token)->
    case lookup(Token) of
        {ok, JWT = #mqtt_admin_jwt{exptime = ExpTime}} ->
            case ExpTime > erlang:system_time(millisecond) of
                true ->
                    ekka_mnesia:dirty_write(JWT#mqtt_admin_jwt{exptime = jwt_expiration_time()}),
                    ok;
                _ ->
                    {error, token_timeout}
            end;
        Error ->
            Error
    end.

do_destroy(Token) ->
    Fun = fun mnesia:delete/1,
    ekka_mnesia:transaction(?DASHBOARD_SHARD, Fun, [{?TAB, Token}]).

do_destroy_by_username(Username) ->
    gen_server:cast(?MODULE, {destroy, Username}).

%%--------------------------------------------------------------------
%% jwt internal util function

lookup(Token) ->
    case mnesia:dirty_read(?TAB, Token) of
        [JWT] -> {ok, JWT};
        [] -> {error, not_found}
    end.

lookup_by_username(Username) ->
    Spec = [{{mqtt_admin_jwt, '_', Username, '_'}, [], ['$_']}],
    mnesia:dirty_select(?TAB, Spec).

jwk(Username, Password, Salt) ->
    Key = erlang:md5(<<Salt/binary, Username/binary, Password/binary>>),
    #{
        <<"kty">> => <<"oct">>,
        <<"k">> => jose_base64url:encode(Key)
    }.

jwt_expiration_time() ->
    ExpTime = emqx:get_config([emqx_dashboard, token_expired_time], ?EXPTIME),
    erlang:system_time(millisecond) + ExpTime.

salt() ->
    _ = emqx_misc:rand_seed(),
    Salt = rand:uniform(16#ffffffff),
    <<Salt:32>>.

format(Token, Username, ExpTime) ->
    #mqtt_admin_jwt{
        token    = Token,
        username = Username,
        exptime  = ExpTime
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
    Spec = [{{mqtt_admin_jwt, '_', '_', '$1'}, [{'<', '$1', Now}], ['$_']}],
    JWTList = mnesia:dirty_select(?TAB, Spec),
    destroy(JWTList),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

timer_clean(Pid) ->
    erlang:send_after(?CLEAN_JWT_INTERVAL, Pid, clean_jwt).
