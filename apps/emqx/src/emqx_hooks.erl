%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_hooks).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    'alarm.activated'/1,
    'alarm.deactivated'/1,
    'client.connect'/2,
    'client.connack'/3,
    'client.connack'/4,
    'client.connected'/2,
    'client.disconnected'/3,
    'client.authorize'/4,
    'client.authorize_strict'/4,
    'client.check_authz_complete'/5,
    'client.authenticate'/2,
    'client.authenticate_strict'/2,
    'client.post_authn'/1,
    'client.subscribe'/3,
    'client.unsubscribe'/3,
    'client.timeout'/3,
    'client.timeout'/4,
    'client.handle_info'/3,
    'client.handle_info'/4,
    'client.ping'/3,
    'session.created'/2,
    'session.created'/3,
    'session.subscribed'/3,
    'session.unsubscribed'/3,
    'session.disconnected'/2,
    'session.resumed'/2,
    'session.resumed'/3,
    'session.discarded'/2,
    'session.takenover'/2,
    'session.terminated'/3,
    'session.save_subopts'/2,
    'message.ingress'/2,
    'message.publish'/1,
    'message.puback'/4,
    'message.dropped'/3,
    'message.delivered'/2,
    'message.acked'/2,
    'message.nack'/2,
    'delivery.dropped'/3,
    'delivery.completed'/2,
    'message.transformation_failed'/2,
    'schema.validation_failed'/2,
    'channel.limiter_adjustment'/2,
    'session.limiter_adjustment'/2,
    'cm.channel.unregistered'/1,
    'tls_handshake.psk_lookup'/2,
    'config.zones_updated'/2,
    'api_actor.pre_create'/2,
    'namespace.delete'/1,
    'namespace.resource_pre_create'/2,
    'client.check_authn_complete'/2
]).

%% Generic dispatch API
-export([
    run/2,
    run/3,
    run_fold/3,
    run_fold/4,
    run_fold_strict/3,
    run_fold_strict/4
]).

%% Hooks API
-export([
    add/3,
    add/4,
    put/3,
    put/4,
    del/2,
    lookup/1
]).

-export([
    context/1,
    stash_context/2,
    unstash_context/1
]).

-export([
    callback_action/1,
    callback_filter/1,
    callback_priority/1
]).

-export_type([
    callback_result/0,
    fold_callback_result/1,
    post_authn_context/0,
    hookpoint/0,
    action/0,
    filter/0
]).

%% Multiple callbacks can be registered on a hookpoint.
%% The execution order depends on the priority value:
%%   - Callbacks with greater priority values will be run before
%%     the ones with lower priority values. e.g. A Callback with
%%     priority = 2 precedes the callback with priority = 1.
%%   - If the priorities of the hooks are equal then their execution
%%     order is determined by the lexicographic of hook function
%%     names.

-type hookpoint() :: atom() | binary().
-type action() :: {module(), atom(), [term()] | undefined}.
-type filter() :: {module(), atom(), [term()] | undefined}.
-type context() :: term().
-type callback_result() :: stop | any().
-type fold_callback_result(Acc) :: {stop, Acc} | {ok, Acc} | stop | any().

-include("emqx_hooks_internal.hrl").

-type callback() :: #callback{}.

-define(PTERM, ?MODULE).
-define(SERVER, emqx_hook_manager).

-define(HOOK_CTX_PD_KEY(NAME), {?MODULE, ctx, NAME}).

%%--------------------------------------------------------------------
%% Test APIs
%%--------------------------------------------------------------------

%% @doc Get callback action.
callback_action(#callback{action = A}) -> A.

%% @doc Get callback filter.
callback_filter(#callback{filter = F}) -> F.

%% @doc Get callback priority.
callback_priority(#callback{priority = P}) -> P.

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------

%% @doc `add/3,4` add a new hook, returns 'already_exists' if the hook exists.
-spec add(hookpoint(), action(), integer()) ->
    ok_or_error(already_exists).
add(HookPoint, Action, Priority) when is_integer(Priority) ->
    do_add(HookPoint, #callback{action = Action, priority = Priority}).

-spec add(hookpoint(), action(), integer(), filter()) ->
    ok_or_error(already_exists).
add(HookPoint, Action, Priority, Filter) when is_integer(Priority) ->
    do_add(HookPoint, #callback{action = Action, filter = Filter, priority = Priority}).

do_add(HookPoint, Callback) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    gen_server:call(?SERVER, {add, HookPoint, Callback}, infinity).

%% @doc `put/3,4` updates the existing hook, add it if not exists.
-spec put(hookpoint(), action(), integer()) -> ok.
put(HookPoint, Action, Priority) when is_integer(Priority) ->
    do_put(HookPoint, #callback{action = Action, priority = Priority}).

-spec put(hookpoint(), action(), integer(), filter()) -> ok.
put(HookPoint, Action, Priority, Filter) when is_integer(Priority) ->
    do_put(HookPoint, #callback{action = Action, filter = Filter, priority = Priority}).

do_put(HookPoint, Callback) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    case do_add(HookPoint, Callback) of
        ok -> ok;
        {error, already_exists} -> gen_server:call(?SERVER, {put, HookPoint, Callback}, infinity)
    end.

%% @doc Unregister a callback.
-spec del(hookpoint(), action() | {module(), atom()}) -> ok.
del(HookPoint, Action) ->
    gen_server:call(?SERVER, {del, HookPoint, Action}, infinity).

%% @doc Run hooks.
-spec run(hookpoint(), list(Arg :: term())) -> ok.
run(HookPoint, Args) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    do_run(lookup(HookPoint), Args).

-spec run(hookpoint(), context(), list(Arg :: term())) -> ok.
run(HookPoint, Ctx, Args) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    ok = stash_context(HookPoint, Ctx),
    do_run(lookup(HookPoint), Args),
    ok = unstash_context(HookPoint).

%% @doc Run hooks with Accumulator.
-spec run_fold(hookpoint(), list(_Arg :: term()), Acc) -> Acc when Acc :: term().
run_fold(HookPoint, Args, Acc) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    do_run_fold(lookup(HookPoint), Args, Acc).

-spec run_fold(hookpoint(), context(), list(_Arg :: term()), Acc) -> Acc when Acc :: term().
run_fold(HookPoint, Ctx, Args, Acc) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    ok = stash_context(HookPoint, Ctx),
    Result = do_run_fold(lookup(HookPoint), Args, Acc),
    ok = unstash_context(HookPoint),
    Result.

-spec run_fold_strict(hookpoint(), list(_Arg :: term()), Acc) -> {ok, Acc} | {error, term()} when
    Acc :: term().
run_fold_strict(HookPoint, Args, Acc) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    do_run_fold_strict(lookup(HookPoint), Args, Acc).

-spec run_fold_strict(hookpoint(), context(), list(_Arg :: term()), Acc) ->
    {ok, Acc} | {error, term()}
when
    Acc :: term().
run_fold_strict(HookPoint, Ctx, Args, Acc) ->
    ok = emqx_hookpoints:verify_hookpoint(HookPoint),
    ok = stash_context(HookPoint, Ctx),
    Result = do_run_fold_strict(lookup(HookPoint), Args, Acc),
    ok = unstash_context(HookPoint),
    Result.

do_run([#callback{action = Action, filter = Filter} | Callbacks], Args) ->
    case filter_passed(Filter, Args) andalso safe_execute(Action, Args) of
        %% stop the hook chain and return
        {ok, stop} -> ok;
        %% continue the hook chain, in following cases:
        %%   - the filter validation failed with 'false'
        %%   - the callback returns any term other than 'stop'
        _ -> do_run(Callbacks, Args)
    end;
do_run([], _Args) ->
    ok.

do_run_fold([#callback{action = Action, filter = Filter} | Callbacks], Args, Acc) ->
    Args1 = append(Args, [Acc]),
    case filter_passed(Filter, Args1) andalso safe_execute(Action, Args1) of
        %% stop the hook chain
        {ok, stop} -> Acc;
        %% stop the hook chain with NewAcc
        {ok, {stop, NewAcc}} -> NewAcc;
        %% continue the hook chain with NewAcc
        {ok, {ok, NewAcc}} -> do_run_fold(Callbacks, Args, NewAcc);
        %% continue the hook chain, in following cases:
        %%   - the filter validation failed with 'false'
        %%   - the callback returns any term other than 'stop' or {'stop', NewAcc}
        %%   - the callback crashes
        _ -> do_run_fold(Callbacks, Args, Acc)
    end;
do_run_fold([], _Args, Acc) ->
    Acc.

do_run_fold_strict([#callback{action = Action, filter = Filter} | Callbacks], Args, Acc) ->
    Args1 = append(Args, [Acc]),
    case filter_passed(Filter, Args1) andalso safe_execute(Action, Args1) of
        %% stop on crash
        {error, Reason} -> {error, Reason};
        %% stop the hook chain
        {ok, stop} -> {ok, Acc};
        %% stop the hook chain with NewAcc
        {ok, {stop, NewAcc}} -> {ok, NewAcc};
        %% continue the hook chain with NewAcc
        {ok, {ok, NewAcc}} -> do_run_fold_strict(Callbacks, Args, NewAcc);
        %% continue the hook chain, in following cases:
        %%   - the filter validation failed with 'false'
        %%   - the callback returns any term other than 'stop' or {'stop', NewAcc}
        _ -> do_run_fold_strict(Callbacks, Args, Acc)
    end;
do_run_fold_strict([], _Args, Acc) ->
    {ok, Acc}.

%% Same as `do_run_fold/3', but a callback cannot clear the consumed mark
%% that an earlier callback set, see `emqx_message:set_consumed/1'.
do_run_fold_publish([#callback{action = Action, filter = Filter} | Callbacks], Msg) ->
    Args = [Msg],
    case filter_passed(Filter, Args) andalso safe_execute(Action, Args) of
        {ok, stop} -> Msg;
        {ok, {stop, NewMsg}} -> keep_consumed(Msg, NewMsg);
        {ok, {ok, NewMsg}} -> do_run_fold_publish(Callbacks, keep_consumed(Msg, NewMsg));
        _ -> do_run_fold_publish(Callbacks, Msg)
    end;
do_run_fold_publish([], Msg) ->
    Msg.

keep_consumed(#message{headers = #{consumed := true}}, #message{} = NewMsg) ->
    emqx_message:set_consumed(NewMsg);
keep_consumed(_Msg, NewMsg) ->
    NewMsg.

-spec filter_passed(filter(), Args :: term()) -> true | false.
filter_passed(undefined, _Args) -> true;
filter_passed(Filter, Args) -> execute(Filter, Args).

safe_execute({M, F, A}, Args) ->
    try execute({M, F, A}, Args) of
        Result -> {ok, Result}
    catch
        Error:Reason:Stacktrace ->
            ?tp(error, "hook_callback_exception", #{
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace,
                callback_module => M,
                callback_function => F,
                callback_args => emqx_utils_redact:redact(Args ++ A)
            }),
            {error, Reason}
    end.

%% @doc execute a function.
execute({M, F, A}, Args) ->
    erlang:apply(M, F, append(Args, A)).

-compile({inline, [append/2]}).
-doc "Append two proper lists. Return the other list when one is empty.".
append([], R) -> R;
append(L, []) -> L;
append(L, R) -> L ++ R.

%% @doc Lookup callbacks.
-spec lookup(hookpoint()) -> [callback()].
lookup(HookPoint) ->
    persistent_term:get({?PTERM, HookPoint}, []).

%%--------------------------------------------------------------------
%% Context stashing
%%   We need this here to maintain backwards compatibility while still being able to
%%   thread more context down to hooks.
%%--------------------------------------------------------------------

-spec context(hookpoint()) -> undefined | context().
context(Name) ->
    get(?HOOK_CTX_PD_KEY(Name)).

-spec stash_context(hookpoint(), context()) -> ok.
stash_context(Name, Context) ->
    _ = put(?HOOK_CTX_PD_KEY(Name), Context),
    ok.

-spec unstash_context(hookpoint()) -> ok.
unstash_context(Name) ->
    _ = erase(?HOOK_CTX_PD_KEY(Name)),
    ok.

-type alarm_activated_context() :: #{
    name := emqx_alarm:name(),
    details := map() | list(),
    message := binary(),
    activated_at := integer()
}.
-type alarm_deactivated_context() :: #{
    name := emqx_alarm:name(),
    details := map() | list() | no_details,
    message := binary(),
    activated_at := integer(),
    deactivated_at := integer()
}.
-type transformation_context() :: #{name := binary()}.
-type validation_context() :: #{name := binary()}.

%%-----------------------------------------------------------------------------
%% Hookpoints
%%-----------------------------------------------------------------------------

%% Named functions dispatch callbacks registered with add/put.
%% Notification hooks return ok; fold hooks return the accumulated value.
%% Callbacks may return stop, {stop, Acc}, or {ok, Acc} to control folding.
%% Strict folds additionally return {error, Reason} when a callback crashes.
%% Metrics and interpretation of hook results remain the caller's responsibility.
%% Each hook callback implementation can also accept any number of custom arguments
%% after the mandatory ones.
%%
%% By default, callbacks are executed in the channel process context.

-compile({inline, [lookup/1]}).

-spec 'alarm.activated'(alarm_activated_context()) ->
    ok.
'alarm.activated'(Context) ->
    do_run(lookup('alarm.activated'), [Context]).

-spec 'alarm.deactivated'(alarm_deactivated_context()) ->
    ok.
'alarm.deactivated'(Context) ->
    do_run(lookup('alarm.deactivated'), [Context]).

-spec 'client.connect'(emqx_types:conninfo(), Props) ->
    Props | {error, term()}
when
    Props :: emqx_types:properties().
'client.connect'(ConnInfo, Props) ->
    do_run_fold(lookup('client.connect'), [ConnInfo], Props).

-spec 'client.connack'(emqx_types:conninfo(), _Reason :: atom(), Props) ->
    Props
when
    Props :: emqx_types:properties().
'client.connack'(ConnInfo, Reason, Props) ->
    do_run_fold(lookup('client.connack'), [ConnInfo, Reason], Props).

-spec 'client.connack'(context(), emqx_types:conninfo(), _Reason :: atom(), Props) ->
    Props
when
    Props :: emqx_types:properties().
'client.connack'(HookContext, ConnInfo, Reason, Props) ->
    with_context('client.connack', HookContext, fun() ->
        'client.connack'(ConnInfo, Reason, Props)
    end).

-spec 'client.connected'(emqx_types:clientinfo(), emqx_types:conninfo()) ->
    ok.
'client.connected'(ClientInfo, ConnInfo) ->
    do_run(lookup('client.connected'), [ClientInfo, ConnInfo]).

-spec 'client.disconnected'(emqx_types:clientinfo(), _Reason :: atom(), emqx_types:conninfo()) ->
    ok.
'client.disconnected'(ClientInfo, Reason, ConnInfo) ->
    do_run(lookup('client.disconnected'), [ClientInfo, Reason, ConnInfo]).

-spec 'client.authorize'(
    emqx_authz_context:t(),
    emqx_types:pubsub(),
    emqx_types:topic(),
    emqx_access_control:authorize_hook_result()
) ->
    emqx_access_control:authorize_hook_result().
'client.authorize'(AuthzContext, Action, Topic, Result) ->
    do_run_fold(lookup('client.authorize'), [AuthzContext, Action, Topic], Result).

%% Strict invocation of the same 'client.authorize' callbacks.
-spec 'client.authorize_strict'(
    emqx_authz_context:t(),
    emqx_types:pubsub(),
    emqx_types:topic(),
    emqx_access_control:authorize_hook_result()
) ->
    {ok, emqx_access_control:authorize_hook_result()} | {error, term()}.
'client.authorize_strict'(AuthzContext, Action, Topic, Result) ->
    do_run_fold_strict(lookup('client.authorize'), [AuthzContext, Action, Topic], Result).

%% NOTE: This hookpoint uses strict fold.
-spec 'message.ingress'(
    emqx_message_ingress:context(),
    emqx_types:message()
) ->
    {ok, emqx_types:message() | {error, term()}} | {error, term()}.
'message.ingress'(Context, Message) ->
    do_run_fold_strict(lookup('message.ingress'), [Context], Message).

-spec 'client.check_authz_complete'(
    emqx_authz_context:t(),
    emqx_types:pubsub(),
    emqx_types:topic(),
    emqx_access_control:authz_result(),
    _From :: term()
) ->
    ok.
'client.check_authz_complete'(AuthzContext, Action, Topic, Result, From) ->
    do_run(lookup('client.check_authz_complete'), [AuthzContext, Action, Topic, Result, From]).

-spec 'client.authenticate'(
    emqx_types:clientinfo(), emqx_access_control:authenticate_hook_result()
) ->
    emqx_access_control:authenticate_hook_result().
'client.authenticate'(ClientInfo, Result) ->
    do_run_fold(lookup('client.authenticate'), [ClientInfo], Result).

%% Strict invocation of the same 'client.authenticate' callbacks.
-spec 'client.authenticate_strict'(
    emqx_types:clientinfo(),
    emqx_access_control:authenticate_hook_result()
) ->
    {ok, emqx_access_control:authenticate_hook_result()} | {error, term()}.
'client.authenticate_strict'(ClientInfo, Result) ->
    do_run_fold_strict(lookup('client.authenticate'), [ClientInfo], Result).

-type post_authn_context() :: #{
    client_info := emqx_types:clientinfo()
    %% Future-proofing: more input fields (e.g. conninfo) and/or extra output
    %% fields (e.g. log_metadata) may be added without breaking callbacks that
    %% ignore unknown keys.
}.

-spec 'client.post_authn'(post_authn_context()) ->
    post_authn_context() | {error, term()}.
'client.post_authn'(Context) ->
    do_run_fold(lookup('client.post_authn'), [], Context).

-spec 'client.subscribe'(emqx_types:clientinfo(), emqx_types:properties(), TopicFilters) ->
    TopicFilters
when
    TopicFilters :: list({emqx_types:topic() | emqx_types:share(), map()}).
'client.subscribe'(ClientInfo, Props, TopicFilters) ->
    do_run_fold(lookup('client.subscribe'), [ClientInfo, Props], TopicFilters).

-spec 'client.unsubscribe'(emqx_types:clientinfo(), emqx_types:properties(), TopicFilters) ->
    TopicFilters
when
    TopicFilters :: list({emqx_types:topic() | emqx_types:share(), map()}).
'client.unsubscribe'(ClientInfo, Props, TopicFilters) ->
    do_run_fold(lookup('client.unsubscribe'), [ClientInfo, Props], TopicFilters).

-spec 'client.timeout'(_TimerRef :: reference(), _Msg :: term(), Replies) ->
    Replies
when
    Replies :: emqx_channel:replies().
'client.timeout'(TimerRef, Message, Replies) ->
    do_run_fold(lookup('client.timeout'), [TimerRef, Message], Replies).

-spec 'client.timeout'(context(), _TimerRef :: reference(), _Msg :: term(), Replies) ->
    Replies
when
    Replies :: emqx_channel:replies().
'client.timeout'(HookContext, TimerRef, Message, Replies) ->
    with_context('client.timeout', HookContext, fun() ->
        'client.timeout'(TimerRef, Message, Replies)
    end).

-spec 'client.handle_info'(emqx_types:clientinfo(), _Msg :: term(), Replies) ->
    Replies
when
    Replies :: #{deliver := list(emqx_types:deliver()), replies := emqx_channel:replies()}.
'client.handle_info'(ClientInfo, Message, Replies) ->
    do_run_fold(lookup('client.handle_info'), [ClientInfo, Message], Replies).

-spec 'client.handle_info'(context(), emqx_types:clientinfo(), _Msg :: term(), Replies) ->
    Replies
when
    Replies :: #{deliver := list(emqx_types:deliver()), replies := emqx_channel:replies()}.
'client.handle_info'(HookContext, ClientInfo, Message, Replies) ->
    with_context('client.handle_info', HookContext, fun() ->
        'client.handle_info'(ClientInfo, Message, Replies)
    end).

-spec 'client.ping'(emqx_types:clientinfo(), emqx_types:conninfo(), Acc) -> Acc when
    Acc :: term().
'client.ping'(ClientInfo, ConnInfo, Acc) ->
    do_run_fold(lookup('client.ping'), [ClientInfo, ConnInfo], Acc).

-spec 'session.created'(emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    ok.
'session.created'(ClientInfo, SessionInfo) ->
    do_run(lookup('session.created'), [ClientInfo, SessionInfo]).

-spec 'session.created'(context(), emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    ok.
'session.created'(HookContext, ClientInfo, SessionInfo) ->
    with_context('session.created', HookContext, fun() ->
        'session.created'(ClientInfo, SessionInfo)
    end).

-spec 'session.subscribed'(
    emqx_types:clientinfo(), emqx_types:topic() | emqx_types:share(), emqx_types:subopts()
) ->
    ok.
'session.subscribed'(ClientInfo, Topic, SubOpts) ->
    do_run(lookup('session.subscribed'), [ClientInfo, Topic, SubOpts]).

-spec 'session.unsubscribed'(
    emqx_types:clientinfo(), emqx_types:topic() | emqx_types:share(), emqx_types:subopts()
) ->
    ok.
'session.unsubscribed'(ClientInfo, Topic, SubOpts) ->
    do_run(lookup('session.unsubscribed'), [ClientInfo, Topic, SubOpts]).

-spec 'session.disconnected'(emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    ok.
'session.disconnected'(ClientInfo, SessionInfo) ->
    do_run(lookup('session.disconnected'), [ClientInfo, SessionInfo]).

-spec 'session.resumed'(emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    ok.
'session.resumed'(ClientInfo, SessionInfo) ->
    do_run(lookup('session.resumed'), [ClientInfo, SessionInfo]).

-spec 'session.resumed'(context(), emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    ok.
'session.resumed'(HookContext, ClientInfo, SessionInfo) ->
    with_context('session.resumed', HookContext, fun() ->
        'session.resumed'(ClientInfo, SessionInfo)
    end).

-spec 'session.discarded'(emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    ok.
'session.discarded'(ClientInfo, SessionInfo) ->
    do_run(lookup('session.discarded'), [ClientInfo, SessionInfo]).

-spec 'session.takenover'(emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    ok.
'session.takenover'(ClientInfo, SessionInfo) ->
    do_run(lookup('session.takenover'), [ClientInfo, SessionInfo]).

-spec 'session.terminated'(
    emqx_types:clientinfo(), _Reason :: atom(), _SessionInfo :: emqx_types:infos()
) ->
    ok.
'session.terminated'(ClientInfo, Reason, SessionInfo) ->
    do_run(lookup('session.terminated'), [ClientInfo, Reason, SessionInfo]).

-spec 'session.save_subopts'(
    #{
        topic_filter := emqx_types:topic() | emqx_types:share()
    },
    SubOpts
) ->
    SubOpts
when
    SubOpts :: #{
        rh => _,
        rap => _,
        nl => _,
        qos => _,
        subid => _,
        _ => _
    }.
'session.save_subopts'(Context, SubOpts) ->
    do_run_fold(lookup('session.save_subopts'), [Context], SubOpts).

-spec 'message.publish'(emqx_types:message()) ->
    emqx_types:message() | [emqx_types:message()].
'message.publish'(Message) ->
    do_run_fold_publish(lookup('message.publish'), Message).

-spec 'message.puback'(
    emqx_types:packet_id(),
    emqx_types:message(),
    emqx_types:publish_result(),
    emqx_types:reason_code()
) ->
    undefined | emqx_types:reason_code().
'message.puback'(PacketId, Message, PublishResult, ReasonCode) ->
    do_run_fold(lookup('message.puback'), [PacketId, Message, PublishResult], ReasonCode).

-spec 'message.dropped'(emqx_types:message(), #{node => node()}, _Reason :: atom()) ->
    ok.
'message.dropped'(Message, By, Reason) ->
    do_run(lookup('message.dropped'), [Message, By, Reason]).

-spec 'message.transformation_failed'(emqx_types:message(), transformation_context()) ->
    ok.
'message.transformation_failed'(Message, Context) ->
    do_run(lookup('message.transformation_failed'), [Message, Context]).

-spec 'schema.validation_failed'(emqx_types:message(), validation_context()) ->
    ok.
'schema.validation_failed'(Message, Context) ->
    do_run(lookup('schema.validation_failed'), [Message, Context]).

-spec 'message.delivered'(emqx_types:clientinfo(), Msg) ->
    Msg
when
    Msg :: emqx_types:message().
'message.delivered'(ClientInfo, Message) ->
    do_run_fold(lookup('message.delivered'), [ClientInfo], Message).

-spec 'message.acked'(emqx_types:clientinfo(), emqx_types:message()) ->
    ok.
'message.acked'(ClientInfo, Message) ->
    do_run(lookup('message.acked'), [ClientInfo, Message]).

-spec 'message.nack'(emqx_types:message(), boolean()) ->
    boolean().
'message.nack'(Message, Nacked) ->
    do_run_fold(lookup('message.nack'), [Message], Nacked).

-spec 'delivery.dropped'(emqx_types:clientinfo(), emqx_types:message(), _Reason :: atom()) ->
    ok.
'delivery.dropped'(ClientInfo, Message, Reason) ->
    do_run(lookup('delivery.dropped'), [ClientInfo, Message, Reason]).

-spec 'delivery.completed'(emqx_types:message(), #{
    session_birth_time := emqx_utils_calendar:epoch_millisecond(),
    clientid := emqx_types:clientid(),
    reason_code => emqx_types:reason_code()
}) ->
    ok.
'delivery.completed'(Message, Context) ->
    do_run(lookup('delivery.completed'), [Message, Context]).

-spec 'channel.limiter_adjustment'(
    emqx_types:clientinfo(), undefined | emqx_limiter_client:t()
) ->
    undefined | emqx_limiter_client:t().
'channel.limiter_adjustment'(ClientInfo, Limiter) ->
    do_run_fold(lookup('channel.limiter_adjustment'), [ClientInfo], Limiter).

-spec 'session.limiter_adjustment'(
    emqx_types:clientinfo(), emqx_limiter_client:t() | {lazy, emqx_limiter:listener_id()}
) ->
    emqx_limiter_client:t() | {lazy, emqx_limiter:listener_id()}.
'session.limiter_adjustment'(ClientInfo, Limiter) ->
    do_run_fold(lookup('session.limiter_adjustment'), [ClientInfo], Limiter).

%% NOTE
%% Executed out of channel process context
-spec 'cm.channel.unregistered'(_ChanPid :: pid()) ->
    ok.
'cm.channel.unregistered'(ChannelPid) ->
    do_run(lookup('cm.channel.unregistered'), [ChannelPid]).

%% NOTE
%% Executed out of channel process context
-spec 'tls_handshake.psk_lookup'(emqx_tls_psk:psk_identity(), normal) ->
    {ok, _SharedSecret :: binary()}
    | {error, term()}
    | normal.
'tls_handshake.psk_lookup'(Identity, Result) ->
    do_run_fold(lookup('tls_handshake.psk_lookup'), [Identity], Result).

%% NOTE
%% Executed out of channel process context
-spec 'config.zones_updated'(_Old :: emqx_config:config(), _New :: emqx_config:config()) ->
    ok.
'config.zones_updated'(Old, New) ->
    do_run(lookup('config.zones_updated'), [Old, New]).

%% NOTE
%% Executed out of channel process context
-spec 'api_actor.pre_create'(
    emqx_dashboard_admin:actor_props(), ok | {error, term()}
) ->
    ok | {error, term()}.
'api_actor.pre_create'(ActorProps, Result) ->
    do_run_fold(lookup('api_actor.pre_create'), [ActorProps], Result).

%% NOTE
%% Executed out of channel process context
%% Implementations must be idempotent.

-spec 'namespace.delete'(emqx_config:namespace()) ->
    ok.
'namespace.delete'(Namespace) ->
    do_run(lookup('namespace.delete'), [Namespace]).

%% NOTE
%% Executed out of channel process context
%% Called before create a namespaced resource (e.g.: mnesia authz rules) as a way for
%% initial applications to check if a namespace exists, since that information lives in
%% the terminal `emqx_mt` application.
%% The accumulated exists field indicates whether the managed namespace exists.

-spec 'namespace.resource_pre_create'(
    #{namespace => emqx_config:namespace()},
    #{exists := boolean()}
) ->
    #{exists := boolean()}.
'namespace.resource_pre_create'(Context, Result) ->
    do_run_fold(lookup('namespace.resource_pre_create'), [Context], Result).

-spec 'client.check_authn_complete'(emqx_types:clientinfo(), map()) -> ok.
'client.check_authn_complete'(ClientInfo, Result) ->
    do_run(lookup('client.check_authn_complete'), [ClientInfo, Result]).

%% Context is extra metadata and is not appended to callback arguments.
with_context(Name, Context, Fun) ->
    ok = stash_context(Name, Context),
    try
        Fun()
    after
        ok = unstash_context(Name)
    end.
