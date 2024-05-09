%%--------------------------------------------------------------------
%% Copyright (c) 2018-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_banned).

-behaviour(gen_server).
-behaviour(emqx_db_backup).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Mnesia bootstrap
-export([create_tables/0]).

-export([start_link/0, stop/0]).

-export([
    check/1,
    check_clientid/1,
    create/1,
    look_up/1,
    delete/1,
    info/1,
    format/1,
    parse/1,
    clear/0,
    who/2,
    tables/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([backup_tables/0]).

%% Internal exports (RPC)
-export([
    expire_banned_items/1
]).

-elvis([{elvis_style, state_record_and_type, disable}]).

-define(BANNED_INDIVIDUAL_TAB, ?MODULE).
-define(BANNED_RULE_TAB, emqx_banned_rules).

%% The default expiration time should be infinite
%% but for compatibility, a large number (1 years) is used here to represent the 'infinite'
-define(EXPIRATION_TIME, 31536000).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

create_tables() ->
    Options = [
        {type, set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, banned},
        {attributes, record_info(fields, banned)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ],
    ok = mria:create_table(?BANNED_INDIVIDUAL_TAB, Options),
    ok = mria:create_table(?BANNED_RULE_TAB, Options),
    [?BANNED_INDIVIDUAL_TAB, ?BANNED_RULE_TAB].

%%--------------------------------------------------------------------
%% Data backup
%%--------------------------------------------------------------------
backup_tables() -> tables().

-spec tables() -> [atom()].
tables() -> [?BANNED_RULE_TAB, ?BANNED_INDIVIDUAL_TAB].

%% @doc Start the banned server.
-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% for tests
-spec stop() -> ok.
stop() -> gen_server:stop(?MODULE).

-spec check(emqx_types:clientinfo()) -> boolean().
check(ClientInfo) ->
    do_check({clientid, maps:get(clientid, ClientInfo, undefined)}) orelse
        do_check({username, maps:get(username, ClientInfo, undefined)}) orelse
        do_check({peerhost, maps:get(peerhost, ClientInfo, undefined)}) orelse
        do_check_rules(ClientInfo).

-spec check_clientid(emqx_types:clientid()) -> boolean().
check_clientid(ClientId) ->
    do_check({clientid, ClientId}) orelse do_check_rules(#{clientid => ClientId}).

-spec format(emqx_types:banned()) -> map().
format(#banned{
    who = Who0,
    by = By,
    reason = Reason,
    at = At,
    until = Until
}) ->
    {As, Who} = format_who(Who0),
    #{
        as => As,
        who => Who,
        by => By,
        reason => Reason,
        at => to_rfc3339(At),
        until => to_rfc3339(Until)
    }.

-spec parse(map()) -> emqx_types:banned() | {error, term()}.
parse(Params) ->
    case parse_who(Params) of
        {error, Reason} ->
            {error, Reason};
        Who ->
            By = maps:get(<<"by">>, Params, <<"mgmt_api">>),
            Reason = maps:get(<<"reason">>, Params, <<"">>),
            At = maps:get(<<"at">>, Params, erlang:system_time(second)),
            Until = maps:get(<<"until">>, Params, At + ?EXPIRATION_TIME),
            case Until > erlang:system_time(second) of
                true ->
                    #banned{
                        who = Who,
                        by = By,
                        reason = Reason,
                        at = At,
                        until = Until
                    };
                false ->
                    ErrorReason =
                        io_lib:format("Cannot create expired banned, ~p to ~p", [At, Until]),
                    {error, ErrorReason}
            end
    end.

-spec create(emqx_types:banned() | map()) ->
    {ok, emqx_types:banned()} | {error, {already_exist, emqx_types:banned()}}.
create(#{
    who := Who,
    by := By,
    reason := Reason,
    at := At,
    until := Until
}) ->
    Banned = #banned{
        who = Who,
        by = By,
        reason = Reason,
        at = At,
        until = Until
    },
    create(Banned);
create(Banned = #banned{who = Who}) ->
    case look_up(Who) of
        [] ->
            insert_banned(table(Who), Banned),
            {ok, Banned};
        [OldBanned = #banned{until = Until}] ->
            %% Don't support shorten or extend the until time by overwrite.
            %% We don't support update api yet, user must delete then create new one.
            case Until > erlang:system_time(second) of
                true ->
                    {error, {already_exist, OldBanned}};
                %% overwrite expired one is ok.
                false ->
                    insert_banned(table(Who), Banned),
                    {ok, Banned}
            end
    end.

-spec look_up(emqx_types:banned_who() | map()) -> [emqx_types:banned()].
look_up(Who) when is_map(Who) ->
    look_up(parse_who(Who));
look_up(Who) ->
    mnesia:dirty_read(table(Who), Who).

-spec delete(map() | emqx_types:banned_who()) -> ok.
delete(Who) when is_map(Who) ->
    delete(parse_who(Who));
delete(Who) ->
    mria:dirty_delete(table(Who), Who).

-spec info(size) -> non_neg_integer().
info(size) ->
    mnesia:table_info(?BANNED_INDIVIDUAL_TAB, size) + mnesia:table_info(?BANNED_RULE_TAB, size).

-spec clear() -> ok.
clear() ->
    _ = mria:clear_table(?BANNED_INDIVIDUAL_TAB),
    _ = mria:clear_table(?BANNED_RULE_TAB),
    ok.

%% Creating banned with `#banned{}` records is exposed as a public API
%% so we need helpers to create the `who` field of `#banned{}` records
-spec who(atom(), binary() | inet:ip_address() | esockd_cidr:cidr()) -> emqx_types:banned_who().
who(clientid, ClientId) when is_binary(ClientId) -> {clientid, ClientId};
who(username, Username) when is_binary(Username) -> {username, Username};
who(peerhost, Peerhost) when is_tuple(Peerhost) -> {peerhost, Peerhost};
who(peerhost, Peerhost) when is_binary(Peerhost) ->
    {ok, Addr} = inet:parse_address(binary_to_list(Peerhost)),
    {peerhost, Addr};
who(clientid_re, RE) when is_binary(RE) ->
    {ok, RECompiled} = re:compile(RE),
    {clientid_re, {RECompiled, RE}};
who(username_re, RE) when is_binary(RE) ->
    {ok, RECompiled} = re:compile(RE),
    {username_re, {RECompiled, RE}};
who(peerhost_net, CIDR) when is_tuple(CIDR) -> {peerhost_net, CIDR};
who(peerhost_net, CIDR) when is_binary(CIDR) ->
    {peerhost_net, esockd_cidr:parse(binary_to_list(CIDR), true)}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, ensure_expiry_timer(#{expiry_timer => undefined})}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_msg", cast => Msg}),
    {noreply, State}.

handle_info({timeout, TRef, expire}, State = #{expiry_timer := TRef}) ->
    _ = mria:transaction(?COMMON_SHARD, fun ?MODULE:expire_banned_items/1, [
        erlang:system_time(second)
    ]),
    {noreply, ensure_expiry_timer(State), hibernate};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{expiry_timer := TRef}) ->
    emqx_utils:cancel_timer(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_check({_, undefined}) ->
    false;
do_check(Who) when is_tuple(Who) ->
    case mnesia:dirty_read(table(Who), Who) of
        [] -> false;
        [#banned{until = Until}] -> Until > erlang:system_time(second)
    end.

do_check_rules(ClientInfo) ->
    Rules = all_rules(),
    Now = erlang:system_time(second),
    lists:any(
        fun(Rule) -> is_rule_actual(Rule, Now) andalso do_check_rule(Rule, ClientInfo) end, Rules
    ).

is_rule_actual(#banned{until = Until}, Now) ->
    Until > Now.

do_check_rule(#banned{who = {clientid_re, {RE, _}}}, #{clientid := ClientId}) ->
    is_binary(ClientId) andalso re:run(ClientId, RE) =/= nomatch;
do_check_rule(#banned{who = {clientid_re, _}}, #{}) ->
    false;
do_check_rule(#banned{who = {username_re, {RE, _}}}, #{username := Username}) ->
    is_binary(Username) andalso re:run(Username, RE) =/= nomatch;
do_check_rule(#banned{who = {username_re, _}}, #{}) ->
    false;
do_check_rule(#banned{who = {peerhost_net, CIDR}}, #{peerhost := Peerhost}) ->
    esockd_cidr:match(Peerhost, CIDR);
do_check_rule(#banned{who = {peerhost_net, _}}, #{}) ->
    false.

parse_who(#{as := As, who := Who}) ->
    parse_who(#{<<"as">> => As, <<"who">> => Who});
parse_who(#{<<"as">> := peerhost, <<"who">> := Peerhost0}) ->
    case inet:parse_address(binary_to_list(Peerhost0)) of
        {ok, Peerhost} -> {peerhost, Peerhost};
        {error, einval} -> {error, "bad peerhost"}
    end;
parse_who(#{<<"as">> := peerhost_net, <<"who">> := CIDRString}) ->
    try esockd_cidr:parse(binary_to_list(CIDRString), true) of
        CIDR -> {peerhost_net, CIDR}
    catch
        error:Error -> {error, Error}
    end;
parse_who(#{<<"as">> := AsRE, <<"who">> := Who}) when
    AsRE =:= clientid_re orelse AsRE =:= username_re
->
    case re:compile(Who) of
        {ok, RE} -> {AsRE, {RE, Who}};
        {error, _} = Error -> Error
    end;
parse_who(#{<<"as">> := As, <<"who">> := Who}) when As =:= clientid orelse As =:= username ->
    {As, Who}.

format_who({peerhost, Host}) ->
    AddrBinary = list_to_binary(inet:ntoa(Host)),
    {peerhost, AddrBinary};
format_who({peerhost_net, CIDR}) ->
    CIDRBinary = list_to_binary(esockd_cidr:to_string(CIDR)),
    {peerhost_net, CIDRBinary};
format_who({AsRE, {_RE, REOriginal}}) when AsRE =:= clientid_re orelse AsRE =:= username_re ->
    {AsRE, REOriginal};
format_who({As, Who}) when As =:= clientid orelse As =:= username ->
    {As, Who}.

to_rfc3339(Timestamp) ->
    emqx_utils_calendar:epoch_to_rfc3339(Timestamp, second).

table({username, _Username}) -> ?BANNED_INDIVIDUAL_TAB;
table({clientid, _ClientId}) -> ?BANNED_INDIVIDUAL_TAB;
table({peerhost, _Peerhost}) -> ?BANNED_INDIVIDUAL_TAB;
table({username_re, _UsernameRE}) -> ?BANNED_RULE_TAB;
table({clientid_re, _ClientIdRE}) -> ?BANNED_RULE_TAB;
table({peerhost_net, _PeerhostNet}) -> ?BANNED_RULE_TAB.

-ifdef(TEST).
ensure_expiry_timer(State) ->
    State#{expiry_timer := emqx_utils:start_timer(10, expire)}.
-else.
ensure_expiry_timer(State) ->
    State#{expiry_timer := emqx_utils:start_timer(timer:minutes(1), expire)}.
-endif.

expire_banned_items(Now) ->
    lists:foreach(
        fun(Tab) ->
            expire_banned_items(Now, Tab)
        end,
        [?BANNED_INDIVIDUAL_TAB, ?BANNED_RULE_TAB]
    ).

expire_banned_items(Now, Tab) ->
    mnesia:foldl(
        fun
            (B = #banned{until = Until}, _Acc) when Until < Now ->
                mnesia:delete_object(Tab, B, sticky_write);
            (_, _Acc) ->
                ok
        end,
        ok,
        Tab
    ).

insert_banned(Tab, Banned) ->
    mria:dirty_write(Tab, Banned),
    on_banned(Banned).

on_banned(#banned{who = {clientid, ClientId}}) ->
    %% kick the session if the client is banned by clientid
    ?tp(
        warning,
        kick_session_due_to_banned,
        #{
            clientid => ClientId
        }
    ),
    emqx_cm:kick_session(ClientId),
    ok;
on_banned(_) ->
    ok.

all_rules() ->
    ets:tab2list(?BANNED_RULE_TAB).
