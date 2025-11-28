%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auth_mnesia_bookkeeper).

-behaviour(gen_server).

%% API
-export([
    start_link/0,

    tally_authn_now/0,
    tally_authz_now/0
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_auth_mnesia_internal.hrl").

-define(authz_tref, authz_tref).
-define(authn_tref, authn_tref).

%% Calls/casts/infos/continues
-record(tally_authn, {}).
-record(tally_authz, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, _Opts = #{}, []).

%% For debug/manual ops
tally_authn_now() ->
    gen_server:call(?MODULE, #tally_authn{}, infinity).

tally_authz_now() ->
    gen_server:call(?MODULE, #tally_authz{}, infinity).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    State = #{
        ?authz_tref => undefined,
        ?authn_tref => undefined
    },
    {ok, State, {continue, #tally_authn{}}}.

terminate(_Reason, _State) ->
    ok.

handle_continue(#tally_authn{}, State0) ->
    State = handle_tally({?AUTHN_NS_TAB, ?AUTHN_NS_COUNT_TAB}, State0),
    {noreply, State, {continue, #tally_authz{}}};
handle_continue(#tally_authz{}, State0) ->
    State = handle_tally({?AUTHZ_NS_TAB, ?AUTHZ_NS_COUNT_TAB}, State0),
    {noreply, State}.

handle_call(#tally_authz{}, _From, State0) ->
    State = handle_tally({?AUTHZ_NS_TAB, ?AUTHZ_NS_COUNT_TAB}, State0),
    {noreply, State};
handle_call(#tally_authn{}, _From, State0) ->
    State = handle_tally({?AUTHN_NS_TAB, ?AUTHN_NS_COUNT_TAB}, State0),
    {noreply, State};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#tally_authz{}, State0) ->
    State = handle_tally({?AUTHZ_NS_TAB, ?AUTHZ_NS_COUNT_TAB}, State0),
    {noreply, State};
handle_info(#tally_authn{}, State0) ->
    State = handle_tally({?AUTHN_NS_TAB, ?AUTHN_NS_COUNT_TAB}, State0),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

handle_tally({Tab, CountTab}, State0) ->
    State1 = cancel_tally_timer(Tab, State0),
    NsCounts = do_tally(Tab),
    ets:insert(CountTab, maps:to_list(NsCounts)),
    ensure_tally_timer(Tab, State1).

do_tally(?AUTHZ_NS_TAB) ->
    ets:foldl(
        fun(#?AUTHZ_NS_TAB{who = ?AUTHZ_WHO_NS(Ns, _)}, Acc) ->
            maps:update_with(Ns, fun(N) -> N + 1 end, 1, Acc)
        end,
        #{},
        ?AUTHZ_NS_TAB
    );
do_tally(?AUTHN_NS_TAB) ->
    ets:foldl(
        fun(#?AUTHN_NS_TAB{user_id = ?AUTHN_NS_KEY(Ns, _, _)}, Acc) ->
            maps:update_with(Ns, fun(N) -> N + 1 end, 1, Acc)
        end,
        #{},
        ?AUTHZ_NS_TAB
    ).

cancel_tally_timer(?AUTHZ_NS_TAB, State0) ->
    #{?authz_tref := TRef} = State0,
    maybe
        true ?= is_reference(TRef),
        receive
            #tally_authz{} -> ok
        after 0 -> ok
        end
    end,
    State0#{?authz_tref := undefined};
cancel_tally_timer(?AUTHN_NS_TAB, State0) ->
    #{?authn_tref := TRef} = State0,
    maybe
        true ?= is_reference(TRef),
        receive
            #tally_authn{} -> ok
        after 0 -> ok
        end
    end,
    State0#{?authn_tref := undefined}.

ensure_tally_timer(?AUTHZ_NS_TAB, State0) ->
    ensure_authz_tally_timer(State0);
ensure_tally_timer(?AUTHN_NS_TAB, State0) ->
    ensure_authn_tally_timer(State0).

ensure_authz_tally_timer(State0) ->
    Timeout = emqx_config:get([authorization, builtin_record_count_refresh_interval]),
    TRef = erlang:send_after(Timeout, self(), #tally_authz{}),
    State0#{?authz_tref := TRef}.

ensure_authn_tally_timer(State0) ->
    Timeout = emqx_config:get([authentication_settings, builtin_record_count_refresh_interval]),
    TRef = erlang:send_after(Timeout, self(), #tally_authn{}),
    State0#{?authn_tref := TRef}.
