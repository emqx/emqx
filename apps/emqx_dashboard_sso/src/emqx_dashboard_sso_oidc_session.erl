%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_oidc_session).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export([start_link/1, start/2, stop/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([new/1, delete/1, lookup/1, random_bin/0, random_bin/1]).

-define(TAB, ?MODULE).

-record(?TAB, {
    state :: binary(),
    created_at :: non_neg_integer(),
    data :: map()
}).

-define(DEFAULT_RANDOM_LEN, 32).
-define(NOW, erlang:system_time(millisecond)).
-define(BACKOFF_MIN, 5000).
-define(BACKOFF_MAX, 10000).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------
start_link(Cfg) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Cfg, []).

start(Name, #{issuer := Issuer, session_expiry := SessionExpiry0}) ->
    case
        emqx_dashboard_sso_oidc_sup:start_child(
            oidcc_provider_configuration_worker,
            [
                #{
                    issuer => Issuer,
                    name => {local, Name},
                    backoff_min => ?BACKOFF_MIN,
                    backoff_max => ?BACKOFF_MAX,
                    backoff_type => random
                }
            ]
        )
    of
        {error, _} = Error ->
            Error;
        _ ->
            SessionExpiry = timer:seconds(SessionExpiry0),
            emqx_dashboard_sso_oidc_sup:start_child(?MODULE, [SessionExpiry])
    end.

stop() ->
    _ = emqx_dashboard_sso_oidc_sup:stop_child(oidcc_provider_configuration_worker),
    _ = emqx_dashboard_sso_oidc_sup:stop_child(?MODULE),
    ok.

new(Data) ->
    case ets:whereis(?TAB) of
        undefined ->
            %% The OIDCC may crash for some reason, even if we have some monitor to observe it
            %% users also may open an OIDC login before the monitor finds it has crashed
            {error, <<"No valid OIDC provider">>};
        _ ->
            State = new_state(),
            ets:insert(
                ?TAB,
                #?TAB{
                    state = State,
                    created_at = ?NOW,
                    data = Data
                }
            ),
            {ok, State}
    end.

delete(State) ->
    ets:delete(?TAB, State).

lookup(State) ->
    case ets:lookup(?TAB, State) of
        [#?TAB{data = Data}] ->
            {ok, Data};
        _ ->
            undefined
    end.

random_bin() ->
    random_bin(?DEFAULT_RANDOM_LEN).

random_bin(Len) ->
    emqx_utils_conv:bin(emqx_utils:gen_id(Len)).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------
init(SessionExpiry) ->
    process_flag(trap_exit, true),
    emqx_utils_ets:new(
        ?TAB,
        [
            ordered_set,
            public,
            named_table,
            {keypos, #?TAB.state},
            {read_concurrency, true}
        ]
    ),
    State = #{session_expiry => SessionExpiry},
    tick_session_expiry(State),
    {ok, State}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(tick_session_expiry, #{session_expiry := SessionExpiry} = State) ->
    Now = ?NOW,
    Spec = ets:fun2ms(fun(#?TAB{created_at = CreatedAt}) ->
        Now - CreatedAt >= SessionExpiry
    end),
    _ = ets:select_delete(?TAB, Spec),
    tick_session_expiry(State),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------
new_state() ->
    State = random_bin(),
    case ets:lookup(?TAB, State) of
        [] ->
            State;
        _ ->
            new_state()
    end.

tick_session_expiry(#{session_expiry := SessionExpiry}) ->
    erlang:send_after(SessionExpiry, self(), tick_session_expiry).
