%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Pipeline manager — message routing and hook management.
%%
%% Responsibilities
%%   1. Hook on message.publish to intercept three topic families:
%%        sess/out/<sid>/     — frames from an LLM session
%%        cap/<type>/<id>/response/<req_id>  — skill replies
%%        evt/...             — trigger events and wait_for_event candidates
%%
%%   2. Route intercepted messages to the correct pipeline instance by
%%      reading the `iid` correlation field from the payload.
%%
%%   3. For evt/... messages additionally check the `emqx_agent_pipeline_waiting`
%%      ETS table and forward to any pipeline instance that registered interest
%%      in that topic via register_waiting/2.
%%
%%   4. Start new pipeline instances when an evt/... topic matches a registered
%%      pipeline definition's trigger.
%%
%% Waiting-instance table
%%   Key: Iid (binary)
%%   Value: WaitTopic (binary MQTT filter)
%%   Written by pipeline processes via register_waiting/unregister_waiting.
%%   The table is public so pipeline processes can write directly without
%%   going through a gen_server call.

-module(emqx_agent_pipeline_mgr).

-behaviour(gen_server).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_agent_pipeline.hrl").

-export([start_link/0, init_hook/0, deinit_hook/0]).
-export([register_waiting/2, unregister_waiting/1]).
-export([on_message_publish/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(WAITING_TAB, emqx_agent_pipeline_waiting).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init_hook() -> ok.
init_hook() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit_hook() -> ok.
deinit_hook() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok.

%% Called by pipeline processes when entering the waiting_event state.
-spec register_waiting(binary(), binary()) -> ok.
register_waiting(Iid, Topic) ->
    true = ets:insert(?WAITING_TAB, {Iid, Topic}),
    ok.

%% Called by pipeline processes when leaving the waiting_event state
%% (either because the awaited event arrived or because the process is
%% terminating).
-spec unregister_waiting(binary()) -> ok.
unregister_waiting(Iid) ->
    true = ets:delete(?WAITING_TAB, Iid),
    ok.

%%--------------------------------------------------------------------
%% Hook callback
%%--------------------------------------------------------------------

on_message_publish(
    #message{topic = <<"sess/out/", Rest/binary>>, payload = Payload} = Msg
) ->
    handle_sess_out(Rest, Payload),
    {ok, Msg};
on_message_publish(
    #message{topic = <<"cap/", Rest/binary>>, payload = Payload} = Msg
) when Rest =/= <<>> ->
    case binary:split(Rest, <<"/response/">>) of
        [_TypeSkill, ReqId] -> handle_cap_reply(ReqId, Payload);
        _ -> ok
    end,
    {ok, Msg};
on_message_publish(
    #message{topic = <<"evt/", _/binary>> = Topic, payload = Payload} = Msg
) ->
    handle_evt(Topic, Payload),
    {ok, Msg};
on_message_publish(Msg) ->
    {ok, Msg}.

%%--------------------------------------------------------------------
%% Internal routing
%%--------------------------------------------------------------------

handle_sess_out(Rest, Payload) ->
    case binary:split(Rest, <<"/">>) of
        [Sid, <<>>] ->
            Frame = safe_decode(Payload),
            %% Only log frames that require pipeline action; skip intermediate
            %% streaming chunks (content/reasoning tokens) to avoid O(N) ct:print
            %% calls that serialise through the CT master for every token.
            case maps:get(<<"type">>, Frame, undefined) of
                <<"intermediate">> -> ok;
                _ -> log_received(sess_out, #{sid => Sid, frame => Frame})
            end,
            Iid = maps:get(<<"iid">>, Frame, undefined),
            route_to_pipeline(Iid, #sess_frame{sid = Sid, frame = Frame});
        _ ->
            ok
    end.

handle_cap_reply(ReqId, Payload) ->
    Frame = safe_decode(Payload),
    log_received(cap_reply, #{req_id => ReqId, frame => Frame}),
    Iid = maps:get(<<"iid">>, Frame, undefined),
    route_to_pipeline(Iid, #cap_reply{req_id = ReqId, frame = Frame}).

handle_evt(Topic, Payload) ->
    Event = safe_decode(Payload),
    log_received(evt, #{topic => Topic, event => Event}),
    %% Start new instances for every active pipeline whose trigger matches.
    Defs = emqx_agent_pipeline_registry:match_trigger(Topic),
    ActiveDefs = [D || D <- Defs, maps:get(<<"active">>, D, false)],
    case Defs of
        [] ->
            ?SLOG(warning, #{
                msg => "pipeline_no_trigger_match",
                topic => Topic,
                hint => "no pipeline definition has a trigger that matches this topic"
            });
        _ ->
            ok
    end,
    % ct:print("ActiveDefs: ~p", [ActiveDefs]),
    lists:foreach(fun(Def) -> start_instance(Def, Event) end, ActiveDefs),
    %% Forward to any instance that registered interest in this topic.
    forward_to_waiting(Topic, Event).

forward_to_waiting(Topic, Event) ->
    ets:foldl(
        fun({Iid, WaitTopic}, _Acc) ->
            case emqx_topic:match(Topic, WaitTopic) of
                true -> route_to_pipeline(Iid, #pipe_evt{topic = Topic, event = Event});
                false -> ok
            end
        end,
        ok,
        ?WAITING_TAB
    ).

start_instance(Def, Event) ->
    case emqx_agent_pipeline_sup:start_pipeline(Def, Event) of
        {ok, _Pid} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "pipeline_instance_start_failed",
                pipeline_id => maps:get(<<"pipeline_id">>, Def, <<"unknown">>),
                reason => Reason
            })
    end.

route_to_pipeline(undefined, _Msg) ->
    ok;
route_to_pipeline(Iid, Msg) ->
    case global:whereis_name({emqx_agent_pipeline, Iid}) of
        undefined ->
            ok;
        Pid ->
            gen_statem:cast(Pid, Msg)
    end.

safe_decode(Payload) ->
    try
        emqx_utils_json:decode(Payload)
    catch
        _:_ -> #{}
    end.

log_received(Kind, Data) ->
    ?SLOG(warning, #{msg => "pipeline_mgr_received", kind => Kind, data => Data}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?WAITING_TAB, [named_table, set, public, {write_concurrency, true}]),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
