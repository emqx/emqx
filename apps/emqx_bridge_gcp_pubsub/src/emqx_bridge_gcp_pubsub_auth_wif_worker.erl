%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_auth_wif_worker).

-behaviour(gen_server).

%% API
-export([
    start_link/2,
    child_spec/2,

    ensure_token/2
]).

%% `gen_server' API
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Internal exports (only for testing/mocking)
-export([now_ms/0, request/5]).

-ifdef(TEST).
-export([init_state/1, do_handle_advance/1]).
-endif.

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("snabbkaffe/include/trace.hrl").

-define(NAME(ResId), {n, l, {?MODULE, ResId}}).
-define(REF(ResId), {via, gproc, ?NAME(ResId)}).

-define(MIN_RETRY_DELAY_MS, 500).
-define(MAX_RETRY_DELAY_MS, timer:seconds(120)).

-define(undefined, undefined).

-define(callers, callers).
-define(insert_fn, insert_fn).
-define(n_failures, n_failures).
-define(next_steps, next_steps).
-define(ongoing_deadline, ongoing_deadline).
-define(prev_steps, prev_steps).
-define(refresh_timer, refresh_timer).
-define(res_id, res_id).
-define(retry_timer, retry_timer).
-define(step_context, step_context).

-type state() :: #{
    ?callers := [gen_server:from()],
    ?insert_fn := {function(), [term()]},
    ?n_failures := non_neg_integer(),
    ?next_steps := [step()],
    ?ongoing_deadline := ?undefined | time_ms(),
    ?prev_steps := [step()],
    ?refresh_timer := ?undefined | reference(),
    ?res_id := binary(),
    ?retry_timer := ?undefined | reference(),
    ?step_context := step_context()
}.

-doc """
Represents the blueprint of what needs to be done for a token step.
""".
-type step() :: #{
    name := atom(),
    method := post | get,
    url := fun((step_context()) -> binary()),
    lifetime := pos_integer(),
    %% query_string := fun((step_context()) -> [{binary(), binary()}]),
    body := fun((step_context()) -> binary()),
    headers := fun((step_context()) -> map()),
    extract_result := fun((#{body := binary()}) -> {ok, #{token => binary()}} | {error, term()})
}.

-type step_context() :: #{
    {step, _} => map()
}.

-type step_result() :: #{
    token := emqx_secret:t(binary()),
    deadline := time_ms()
}.

-type time_ms() :: integer().

%% Calls/casts/infos/continues
-record(advance, {}).
-record(ensure_token, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(ResId, Opts) ->
    gen_server:start_link(?REF(ResId), ?MODULE, Opts, []).

child_spec(ResId, Opts) ->
    #{
        id => ResId,
        start => {?MODULE, start_link, [ResId, Opts]},
        type => worker,
        shutdown => 1_000
    }.

ensure_token(ResId, Timeout) ->
    try
        gen_server:call(?REF(ResId), #ensure_token{}, Timeout)
    catch
        exit:{timeout, _} ->
            {error, timeout}
    end.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(Opts) ->
    State = init_state(Opts),
    {ok, State, {continue, #advance{}}}.

handle_continue(#advance{}, State0) ->
    handle_advance(State0).

handle_call(#ensure_token{}, From, State0) ->
    handle_ensure_token(From, State0);
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({timeout, _TRef, #advance{}}, State0) ->
    ?tp("gcp_wif_worker_info_advance_enter", #{}),
    %% Refresh or retry
    State1 = cancel_timer(?refresh_timer, State0),
    State2 = cancel_timer(?retry_timer, State1),
    handle_advance(State2);
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal exports (only for testing/mocking)
%%------------------------------------------------------------------------------

now_ms() ->
    erlang:system_time(millisecond).

request(Method, URL, Headers, Body, ReqOpts) ->
    case hackney:request(Method, URL, Headers, Body, ReqOpts) of
        {ok, Code, RespHeaders, ClientRef} ->
            {ok, RespBody} = hackney:body(ClientRef),
            {ok, Code, RespHeaders, RespBody};
        {error, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec init_state(map()) -> state().
init_state(Opts) ->
    #{
        resource_id := ResId,
        steps := [_ | _] = Steps,
        insert_fn := InsertFn
    } = Opts,
    #{
        ?callers => [],
        ?insert_fn => InsertFn,
        ?n_failures => 0,
        ?next_steps => Steps,
        ?ongoing_deadline => ?undefined,
        ?prev_steps => [],
        ?refresh_timer => ?undefined,
        ?res_id => ResId,
        ?retry_timer => ?undefined,
        ?step_context => #{}
    }.

handle_ensure_token(From, State0) ->
    case State0 of
        #{?refresh_timer := ?undefined, ?callers := Callers} ->
            ?tp("gcp_wif_worker_ensure_token_before_first_token", #{}),
            %% Haven't reached the end of the flow yet.
            State = State0#{?callers := [From | Callers]},
            {noreply, State};
        #{?refresh_timer := TRef} when is_reference(TRef) ->
            ?tp("gcp_wif_worker_ensure_token_after_first_token", #{}),
            %% Wrote at least one token.
            {reply, ok, State0}
    end.

reply_callers(#{?callers := Callers} = State0) ->
    lists:foreach(fun(From) -> gen_server:reply(From, ok) end, Callers),
    State0#{?callers := []}.

handle_advance(State0) ->
    case do_handle_advance(State0) of
        {done, State1} ->
            #{?prev_steps := [#{lifetime := Lifetime} = _LastStep | _]} = State1,
            State2 = reply_callers(State1),
            State = schedule_refresh(Lifetime, State2),
            {noreply, State};
        {retry, State1} ->
            ?tp("gcp_wif_worker_will_retry", #{}),
            State = schedule_retry_step(State1),
            {noreply, State};
        {continue, State} ->
            {noreply, State, {continue, #advance{}}}
    end.

-spec do_handle_advance(state()) -> {done | retry | continue, state()}.
do_handle_advance(#{?next_steps := []} = State0) ->
    %% Done
    #{
        ?res_id := ResId,
        ?prev_steps := [#{lifetime := Lifetime, name := Name} = _LastStep | _],
        ?insert_fn := {InsertFn, Args}
    } = State0,
    #{?step_context := #{{step, Name} := #{token := FinalToken}}} = State0,
    ?tp(debug, "gcp_wif_worker_reached_final_token", #{
        step => Name,
        refresh_in_ms => ceil(Lifetime * 0.75),
        resource_id => ResId
    }),
    apply(InsertFn, [emqx_secret:unwrap(FinalToken) | Args]),
    {done, State0};
do_handle_advance(#{?next_steps := [Step | Rest]} = State0) ->
    #{?res_id := ResId} = State0,
    #{name := Name} = Step,
    ?tp(debug, "gcp_wif_worker_advancing_step", #{
        step => Name,
        resource_id => ResId
    }),
    NowMS = ?MODULE:now_ms(),
    #{?ongoing_deadline := OngoingDeadline} = State0,
    case OngoingDeadline of
        ?undefined ->
            %% First step.
            continue_advance(State0);
        _ when NowMS >= OngoingDeadline ->
            ?tp(debug, "gcp_wif_worker_previous_step_expired", #{
                step => Name,
                resource_id => ResId
            }),
            %% The previous token expired; start over.
            #{?prev_steps := RevSteps} = State0,
            State = State0#{
                ?n_failures := 0,
                ?ongoing_deadline := ?undefined,
                ?prev_steps := [],
                ?next_steps := lists:reverse(RevSteps) ++ [Step | Rest],
                ?step_context := #{}
            },
            {continue, State};
        _ ->
            %% Previous token still valid.
            continue_advance(State0)
    end.

continue_advance(#{?next_steps := [Step | Rest]} = State0) ->
    #{?res_id := ResId, ?prev_steps := PrevSteps} = State0,
    case handle_one_step(Step, State0) of
        {ok, _StepRes, State1} ->
            #{name := Name} = Step,
            ?tp(debug, "gcp_wif_worker_step_succeeded", #{
                step => Name,
                result => _StepRes,
                resource_id => ResId
            }),
            State = State1#{
                ?next_steps := Rest,
                ?prev_steps := [Step | PrevSteps]
            },
            {continue, State};
        {error, Reason} ->
            #{name := Name} = Step,
            ?tp(warning, "gcp_wif_worker_step_failed", #{
                step => Name,
                reason => Reason,
                resource_id => ResId
            }),
            {retry, State0}
    end.

-spec handle_one_step(step(), state()) -> {ok, step_result(), state()} | {error, term()}.
handle_one_step(Step, State0) ->
    #{name := Name} = Step,
    #{step_context := StepContext0} = State0,
    case eval_step(Step, StepContext0) of
        {ok, #{token := _, deadline := Deadline} = Res} ->
            StepContext = StepContext0#{{step, Name} => Res},
            State = State0#{
                ?step_context := StepContext,
                ?ongoing_deadline := Deadline,
                ?n_failures := 0
            },
            {ok, Res, State};
        {error, Reason} ->
            {error, Reason}
    end.

-spec eval_step(step(), step_context()) -> {ok, step_result()} | {error, term()}.
eval_step(Step, StepContext) ->
    #{
        name := _Name,
        method := Method,
        lifetime := Lifetime,
        url := URLFn,
        body := BodyFn,
        headers := HeadersFn,
        extract_result := ExtractResultFn
    } = Step,
    URL = URLFn(StepContext),
    Body = BodyFn(StepContext),
    Headers = HeadersFn(StepContext),
    Res = ?MODULE:request(Method, URL, Headers, Body, _ReqOpts = []),
    case Res of
        {error, Reason} ->
            {error, Reason};
        {ok, Code, _RespHeaders, RespBody} when 200 =< Code, Code < 300 ->
            case ExtractResultFn(#{body => RespBody}) of
                {ok, TokenRes = #{token := Token}} ->
                    Deadline = ?MODULE:now_ms() + Lifetime,
                    {ok, TokenRes#{
                        token := emqx_secret:wrap(Token),
                        deadline => Deadline
                    }};
                {error, Reason} ->
                    {error, {bad_token_result, Reason}}
            end;
        {ok, Code, _RespHeaders, RespBody0} ->
            RespBody =
                case emqx_utils_json:safe_decode(RespBody0) of
                    {ok, RespBody1} -> RespBody1;
                    {error, _} -> RespBody0
                end,
            {error, {http, {Code, RespBody}}}
    end.

cancel_timer(Key, State0) ->
    #{Key := TRef} = State0,
    emqx_utils:cancel_timer(TRef),
    State0#{Key := ?undefined}.

schedule_refresh(Lifetime, State0) ->
    #{
        ?next_steps := [],
        ?prev_steps := RevSteps
    } = State0,
    %% Tokens typically last for 1 hour
    RefreshTime = ceil(Lifetime * 0.75),
    State1 = cancel_timer(?refresh_timer, State0),
    TRef = emqx_utils:start_timer(RefreshTime, #advance{}),
    State1#{
        ?ongoing_deadline := ?undefined,
        ?n_failures := 0,
        ?refresh_timer := TRef,
        ?next_steps := lists:reverse(RevSteps),
        ?prev_steps := [],
        ?step_context := #{}
    }.

schedule_retry_step(State0) ->
    #{?n_failures := NFailures0} = State0,
    State1 = cancel_timer(?retry_timer, State0),
    NFailures = NFailures0 + 1,
    Delay = min(?MAX_RETRY_DELAY_MS, max(?MIN_RETRY_DELAY_MS, (1 bsl NFailures) * 500)),
    TRef = emqx_utils:start_timer(Delay, #advance{}),
    State1#{?retry_timer := TRef}.
