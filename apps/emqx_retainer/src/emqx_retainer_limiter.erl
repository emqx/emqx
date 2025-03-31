%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_limiter).

-include("emqx_retainer.hrl").

-export([
    create/0,
    delete/0,
    update/0
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec create() -> ok | {error, term()}.
create() ->
    emqx_limiter:create_group(shared, ?RETAINER_LIMITER_GROUP, limiter_configs()).

-spec delete() -> ok | {error, term()}.
delete() ->
    emqx_limiter:delete_group(?RETAINER_LIMITER_GROUP).

-spec update() -> ok.
update() ->
    emqx_limiter:update_group(?RETAINER_LIMITER_GROUP, limiter_configs()).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

limiter_configs() ->
    DispatcherLimiterConfig =
        case emqx:get_config([retainer, flow_control, batch_deliver_limiter], infinity) of
            infinity ->
                emqx_limiter:config_unlimited();
            Rate ->
                emqx_limiter:config_from_rate(Rate)
        end,
    PublisherLimiterConfig = emqx_limiter:config(max_publish, emqx_config:get([retainer])),
    [
        {?DISPATCHER_LIMITER_NAME, DispatcherLimiterConfig},
        {?PUBLISHER_LIMITER_NAME, PublisherLimiterConfig}
    ].
