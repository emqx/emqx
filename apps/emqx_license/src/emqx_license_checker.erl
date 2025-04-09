%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_checker).

-include("emqx_license.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-define(CHECK_INTERVAL, timer:seconds(5)).
-define(REFRESH_INTERVAL, timer:minutes(2)).
-define(EXPIRY_ALARM_CHECK_INTERVAL, timer:hours(24)).

-define(OK(EXPR),
    try
        _ = begin
            EXPR
        end,
        ok
    catch
        _:_ -> ok
    end
).

-export([
    start_link/1,
    start_link/2,
    update/1,
    dump/0,
    expiry_epoch/0,
    purge/0,
    limits/0,
    print_warnings/1,
    get_max_sessions/1,
    get_dynamic_max_sessions/0,
    no_violation/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-export_type([limits/0, license/0, fetcher/0]).

-define(LICENSE_TAB, emqx_license).

-type limits() :: #{max_sessions := non_neg_integer() | ?ERR_EXPIRED | ?ERR_MAX_UPTIME}.
-type license() :: emqx_license_parser:license().
-type fetcher() :: fun(() -> {ok, license()} | {error, term()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link(fetcher()) -> {ok, pid()}.
start_link(LicenseFetcher) ->
    start_link(LicenseFetcher, ?CHECK_INTERVAL).

-spec start_link(fetcher(), timeout()) -> {ok, pid()}.
start_link(LicenseFetcher, CheckInterval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [LicenseFetcher, CheckInterval], []).

-spec update(license()) -> map().
update(License) ->
    gen_server:call(?MODULE, {update, License}, infinity).

-spec dump() -> [{atom(), term()}].
dump() ->
    gen_server:call(?MODULE, dump, infinity).

-spec expiry_epoch() -> integer().
expiry_epoch() ->
    gen_server:call(?MODULE, expiry_epoch, infinity).

-spec limits() -> {ok, limits()} | {error, any()}.
limits() ->
    try ets:lookup(?LICENSE_TAB, limits) of
        [{limits, Limits}] -> {ok, Limits};
        _ -> {error, no_license}
    catch
        error:badarg ->
            {error, no_license}
    end.

%% @doc Force purge the license table.
-spec purge() -> ok.
purge() ->
    gen_server:call(?MODULE, purge, infinity).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([LicenseFetcher, CheckInterval]) ->
    {ok, License} = LicenseFetcher(),
    ?LICENSE_TAB = ets:new(?LICENSE_TAB, [
        set, protected, named_table, {read_concurrency, true}
    ]),
    State0 = ensure_check_license_timer(#{
        check_license_interval => CheckInterval,
        license => License,
        start_time => erlang:monotonic_time(seconds)
    }),
    State1 = ensure_refresh_timer(State0),
    State = ensure_check_expiry_timer(State1),
    ok = print_warnings(check_license(State)),
    {ok, State}.

handle_call({update, License}, _From, #{license := Old} = State0) ->
    ok = expiry_early_alarm(License),
    State1 = ensure_refresh_timer(State0),
    ok = log_new_license(Old, License),
    State2 = State1#{license => License},
    {reply, check_license(State2), State2};
handle_call(dump, _From, #{license := License} = State) ->
    Dump0 = emqx_license_parser:dump(License),
    %% resolve the current dynamic limit
    MaybeDynamic = get_max_sessions(License),
    Dump = lists:keyreplace(max_sessions, 1, Dump0, {max_sessions, MaybeDynamic}),
    {reply, Dump, State};
handle_call(expiry_epoch, _From, #{license := License} = State) ->
    ExpiryEpoch = date_to_expiry_epoch(emqx_license_parser:expiry_date(License)),
    {reply, ExpiryEpoch, State};
handle_call(purge, _From, State) ->
    _ = ets:delete_all_objects(?LICENSE_TAB),
    {reply, ok, State};
handle_call(_Req, _From, State) ->
    {reply, unknown, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_license, State) ->
    #{} = check_license(State),
    NewState = ensure_check_license_timer(State),
    ?tp(emqx_license_checked, #{}),
    {noreply, NewState};
handle_info(check_expiry_alarm, #{license := License} = State) ->
    ok = expiry_early_alarm(License),
    NewState = ensure_check_expiry_timer(State),
    {noreply, NewState};
handle_info(refresh, State0) ->
    State1 = refresh(State0),
    NewState = ensure_refresh_timer(State1),
    {noreply, NewState};
handle_info(_Msg, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------

refresh(#{license := #{source := <<"file://", _/binary>> = Source} = License} = State) ->
    case emqx_license_parser:parse(Source) of
        {ok, License} ->
            ?tp(emqx_license_refresh_no_change, #{}),
            %% no change
            State;
        {ok, NewLicense} ->
            ok = log_new_license(License, NewLicense),
            %% ensure alarm is set or cleared
            ok = expiry_early_alarm(NewLicense),
            ?tp(emqx_license_refresh_changed, #{new_license => NewLicense}),
            State#{license => NewLicense};
        {error, Reason} ->
            ?tp(
                error,
                emqx_license_refresh_failed,
                Reason#{continue_with_license => emqx_license_parser:summary(License)}
            ),
            State
    end;
refresh(State) ->
    State.

log_new_license(Old, New) ->
    ?SLOG(
        info,
        #{
            msg => "new_license_loaded",
            old_license => emqx_license_parser:summary(Old),
            new_license => emqx_license_parser:summary(New)
        },
        #{tag => "LICENSE"}
    ).

ensure_check_license_timer(#{check_license_interval := CheckInterval} = State) ->
    ok = cancel_timer(State, check_timer),
    State#{check_timer => erlang:send_after(CheckInterval, self(), check_license)}.

ensure_check_expiry_timer(State) ->
    ok = cancel_timer(State, expiry_alarm_timer),
    Ref = erlang:send_after(?EXPIRY_ALARM_CHECK_INTERVAL, self(), check_expiry_alarm),
    State#{expiry_alarm_timer => Ref}.

%% refresh is to work with file:// license keys.
ensure_refresh_timer(State) ->
    ok = cancel_timer(State, refresh_timer),
    Ref = erlang:send_after(?REFRESH_INTERVAL, self(), refresh),
    State#{refresh_timer => Ref}.

cancel_timer(State, Key) ->
    case maps:find(Key, State) of
        {ok, Ref} when is_reference(Ref) ->
            _ = erlang:cancel_timer(Ref),
            ok;
        _ ->
            ok
    end.

-spec no_violation(license()) -> ok | {error, atom()}.
no_violation(License) ->
    %% check only running nodes to allow once misconfigured cluster recover
    case
        emqx_license_parser:is_single_node(License) andalso length(emqx:cluster_nodes(running)) > 1
    of
        true -> {error, 'SINGLE_NODE_LICENSE'};
        false -> ok
    end.

check_license(#{license := License, start_time := StartTime} = _State) ->
    DaysLeft = days_left(License),
    IsOverdue = is_overdue(License, DaysLeft),
    IsMaxUptimeReached = is_max_uptime_reached(License, StartTime),
    #{max_sessions := MaxConn} =
        Limits = limits(License, #{
            is_overdue => IsOverdue,
            is_max_uptime_reached => IsMaxUptimeReached
        }),
    true = apply_limits(Limits),
    ok = ensure_cluster_mode(License),
    ok = ensure_telemetry_default_status(License),
    #{
        warn_default => warn_community(License),
        warn_evaluation => warn_evaluation(License, IsOverdue, MaxConn),
        warn_expiry => {(DaysLeft < 0), -DaysLeft}
    }.

ensure_cluster_mode(License) ->
    case emqx_license_parser:is_single_node(License) of
        true ->
            ok = emqx_cluster:ensure_singleton_mode();
        false ->
            ok = emqx_cluster:ensure_normal_mode()
    end.

%% Enable telemetry when any of the following conditions are met:
%% 1. License type is community (LTYPE=2)
%% 2. Customer type is education  | non-profit (CTYPE=5)
%% 3. Customer type is evaluation (CTYPE=10)
%% 4. Customer type is developer (CTYPE=11)
ensure_telemetry_default_status(License) ->
    LType = emqx_license_parser:license_type(License),
    CType = emqx_license_parser:customer_type(License),
    EnableByDefault =
        LType =:= ?COMMUNITY orelse
            CType =:= ?EDUCATION_NONPROFIT_CUSTOMER orelse
            CType =:= ?EVALUATION_CUSTOMER orelse
            CType =:= ?DEVELOPER_CUSTOMER,
    emqx_telemetry_config:set_default_status(EnableByDefault).

warn_community(License) ->
    emqx_license_parser:license_type(License) == ?COMMUNITY.

warn_evaluation(License, false, MaxConn) ->
    {emqx_license_parser:customer_type(License) == ?EVALUATION_CUSTOMER, MaxConn};
warn_evaluation(_License, _IsOverdue, _MaxConn) ->
    false.

limits(_License, #{is_overdue := true}) ->
    #{
        max_sessions => ?ERR_EXPIRED
    };
limits(_License, #{is_max_uptime_reached := true}) ->
    #{
        max_sessions => ?ERR_MAX_UPTIME
    };
limits(License, #{}) ->
    #{
        max_sessions => get_max_sessions(License)
    }.

%% @doc Return the max_sessions limit defined in license.
%% For business-critical type, it returns the dynamic value set in config.
-spec get_max_sessions(license()) -> non_neg_integer().
get_max_sessions(License) ->
    Max = emqx_license_parser:max_sessions(License),
    Dyn =
        case emqx_license_parser:customer_type(License) of
            ?BUSINESS_CRITICAL_CUSTOMER ->
                min(get_dynamic_max_sessions(), Max);
            _ ->
                Max
        end,
    min(Max, Dyn).

%% @doc Get the dynamic max_sessions limit set in config.
%% It's only meaningful for business-critical license.
-spec get_dynamic_max_sessions() -> non_neg_integer().
get_dynamic_max_sessions() ->
    %% For config backward compatibility, dynamic_max_connections is not renamed to dynamic_max_sesssions
    emqx_conf:get([license, dynamic_max_connections]).

days_left(License) ->
    DateEnd = emqx_license_parser:expiry_date(License),
    {DateNow, _} = calendar:universal_time(),
    calendar:date_to_gregorian_days(DateEnd) - calendar:date_to_gregorian_days(DateNow).

is_overdue(License, DaysLeft) ->
    CType = emqx_license_parser:customer_type(License),
    Type = emqx_license_parser:license_type(License),

    small_customer_overdue(CType, DaysLeft) orelse
        non_official_license_overdue(Type, DaysLeft).

is_max_uptime_reached(License, StartTime) ->
    case emqx_license_parser:max_uptime_seconds(License) of
        infinity ->
            false;
        MaxUptimeSeconds when is_integer(MaxUptimeSeconds) ->
            seconds_from_start(StartTime) >= MaxUptimeSeconds
    end.

seconds_from_start(StartTime) ->
    erlang:monotonic_time(seconds) - StartTime.

%% small customers overdue 90 days after license expiry date
small_customer_overdue(?SMALL_CUSTOMER, DaysLeft) -> DaysLeft < ?EXPIRED_DAY;
small_customer_overdue(_CType, _DaysLeft) -> false.

%% never restrict official license
non_official_license_overdue(?OFFICIAL, _) -> false;
non_official_license_overdue(_, DaysLeft) -> DaysLeft < 0.

%% 62167219200 =:= calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}).
-define(EPOCH_START, 62167219200).
-spec date_to_expiry_epoch(calendar:date()) -> Seconds :: non_neg_integer().
date_to_expiry_epoch({Y, M, D}) ->
    calendar:datetime_to_gregorian_seconds({{Y, M, D}, {0, 0, 0}}) - ?EPOCH_START.

apply_limits(Limits) ->
    ets:insert(?LICENSE_TAB, {limits, Limits}).

expiry_early_alarm(License) ->
    case days_left(License) < 30 of
        true ->
            {Y, M, D} = emqx_license_parser:expiry_date(License),
            Date = iolist_to_binary(io_lib:format("~B~2..0B~2..0B", [Y, M, D])),
            ?OK(emqx_alarm:activate(license_expiry, #{expiry_at => Date}));
        false ->
            ?OK(emqx_alarm:ensure_deactivated(license_expiry))
    end.

print_warnings(State) ->
    ok = print_community_warning(State),
    ok = print_evaluation_warning(State),
    ok = print_expiry_warning(State).

print_community_warning(#{warn_default := true}) ->
    io:format(?COMMUNITY_LICENSE_LOG);
print_community_warning(_) ->
    ok.

print_evaluation_warning(#{warn_evaluation := {true, MaxConn}}) ->
    io:format(?EVALUATION_LOG, [MaxConn]);
print_evaluation_warning(_) ->
    ok.

print_expiry_warning(#{warn_expiry := {true, Days}}) ->
    io:format(?EXPIRY_LOG, [Days]);
print_expiry_warning(_) ->
    ok.
