%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Pipeline manager — message routing and hook management.
%%
%% Responsibilities
%%   1. Hook on message.publish to intercept trigger events:
%%        evt/...             — trigger events
%%
%%   2. Start new pipeline instances when an evt/... topic matches a registered
%%      pipeline definition's trigger.

-module(emqx_agent_pipeline_mgr).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([init_hook/0, deinit_hook/0]).
-export([on_message_publish/1]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec init_hook() -> ok.
init_hook() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit_hook() -> ok.
deinit_hook() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok.

%%--------------------------------------------------------------------
%% Hook callback
%%--------------------------------------------------------------------

on_message_publish(#message{topic = <<"$evt/", _/binary>>} = Msg) ->
    handle_evt(Msg),
    {ok, Msg};
on_message_publish(Msg) ->
    {ok, Msg}.

%%--------------------------------------------------------------------
%% Internal routing
%%--------------------------------------------------------------------

handle_evt(#message{topic = Topic, payload = Payload} = Msg) ->
    Event = safe_decode(Payload),
    log_received(evt, #{topic => Topic, event => Event}),
    %% Start new instances for every active pipeline whose trigger matches.
    Defs = emqx_agent_pipeline:match_triggers(Topic),
    ActiveDefs = [D || D <- Defs, maps:get(<<"active">>, D, false)],
    case Defs of
        [] ->
            ?SLOG(debug, #{
                msg => "pipeline_no_trigger_match",
                topic => Topic,
                hint => "no pipeline definition has a trigger that matches this topic"
            });
        _ ->
            ok
    end,
    lists:foreach(
        fun(Def) -> start_instance(Def, #{event => Event, message => Msg}) end, ActiveDefs
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

safe_decode(Payload) ->
    try
        emqx_utils_json:decode(Payload)
    catch
        _:_ -> #{}
    end.

log_received(Kind, Data) ->
    ?SLOG(warning, #{msg => "pipeline_mgr_received", kind => Kind, data => Data}).
