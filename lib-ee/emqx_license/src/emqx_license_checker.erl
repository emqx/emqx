%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_checker).

-include("emqx_license.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-define(CHECK_INTERVAL, 5000).

-export([start_link/1,
         start_link/2,
         update/1,
         dump/0,
         limits/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-type limits() :: #{max_connections := non_neg_integer()}.

-spec start_link(emqx_license_parser:license()) -> {ok, pid()}.
start_link(LicenseFetcher) ->
    start_link(LicenseFetcher, ?CHECK_INTERVAL).

-spec start_link(emqx_license_parser:license(), timeout()) -> {ok, pid()}.
start_link(LicenseFetcher, CheckInterval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [LicenseFetcher, CheckInterval], []).

-spec update(emqx_license_parser:license()) -> ok.
update(License) ->
    gen_server:call(?MODULE, {update, License}).

-spec dump() -> [{atom(), term()}].
dump() ->
    gen_server:call(?MODULE, dump).

-spec limits() -> limits().
limits() ->
    try ets:lookup(?MODULE, limits) of
        [{limits, Limits}] -> Limits;
        _ -> default_limits()
    catch
        error:badarg -> default_limits()
    end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([LicenseFetcher, CheckInterval]) ->
    case LicenseFetcher() of
        {ok, License} ->
            _ = ets:new(?MODULE, [set, protected, named_table]),
            #{} = check_license(License),
            State = ensure_timer(#{check_license_interval => CheckInterval,
                                   license => License}),
            {ok, State};
        {error, _} = Error ->
            Error
    end.

handle_call({update, License}, _From, State) ->
    {reply, check_license(License), State#{license => License}};

handle_call(dump, _From, #{license := License} = State) ->
    {reply, emqx_license_parser:dump(License), State};

handle_call(_Req, _From, State) ->
    {reply, unknown, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_license, #{license := License} = State) ->
    #{} = check_license(License),
    NewState = ensure_timer(State),
    ?tp(debug, emqx_license_checked, #{}),
    {noreply, NewState};

handle_info(_Msg, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------

ensure_timer(#{check_license_interval := CheckInterval} = State) ->
    _ = case State of
            #{timer := Timer} -> erlang:cancel_timer(Timer);
            _ -> ok
        end,
    State#{timer => erlang:send_after(CheckInterval, self(), check_license)}.

check_license(License) ->
    NeedRestrict = need_restrict(License),
    Limits = limits(License, NeedRestrict),
    true = apply_limits(Limits),
    #{warn_evaluation => warn_evaluation(License, NeedRestrict),
      warn_expiry => warn_expiry(License, NeedRestrict)}.

warn_evaluation(License, false) ->
    emqx_license_parser:customer_type(License) == ?EVALUATION_CUSTOMER;
warn_evaluation(_License, _NeedRestrict) -> false.

warn_expiry(_License, NeedRestrict) -> NeedRestrict.

limits(License, false) -> #{max_connections => emqx_license_parser:max_connections(License)};
limits(_License, true) -> #{max_connections => 0}.

default_limits() -> #{max_connections => 0}.

days_left(License) ->
    DateEnd = emqx_license_parser:expiry_date(License),
    {DateNow, _} = calendar:universal_time(),
    calendar:date_to_gregorian_days(DateEnd) - calendar:date_to_gregorian_days(DateNow).

need_restrict(License)->
    DaysLeft = days_left(License),
    CType = emqx_license_parser:customer_type(License),
    Type = emqx_license_parser:license_type(License),

    DaysLeft < 0
    andalso (Type =/= ?OFFICIAL) or small_customer_overexpired(CType, DaysLeft).

small_customer_overexpired(?SMALL_CUSTOMER, DaysLeft)
    when DaysLeft < ?EXPIRED_DAY -> true;
small_customer_overexpired(_CType, _DaysLeft) -> false.

apply_limits(Limits) ->
    ets:insert(?MODULE, {limits, Limits}).
