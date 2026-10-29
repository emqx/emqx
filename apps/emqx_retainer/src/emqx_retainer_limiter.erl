%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_limiter).

-include("emqx_retainer.hrl").

-export([
    create/0,
    delete/0,
    update/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec create() -> ok | {error, term()}.
create() ->
    RetainerConf = emqx:get_config([retainer]),
    emqx_limiter:create_group(shared, ?RETAINER_LIMITER_GROUP, limiter_configs(RetainerConf)).

-spec delete() -> ok | {error, term()}.
delete() ->
    emqx_limiter:delete_group(?RETAINER_LIMITER_GROUP).

-doc """
Apply the rates in `RetainerConf`. `post_config_update` calls this before
the new config is saved, so do not read the global config here.
""".
-spec update(emqx_config:config()) -> ok.
update(RetainerConf) ->
    emqx_limiter:update_group(?RETAINER_LIMITER_GROUP, limiter_configs(RetainerConf)).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

limiter_configs(#{flow_control := #{batch_deliver_limiter := DeliverRate}} = RetainerConf) ->
    DispatcherLimiterConfig = emqx_limiter:config_from_rate(DeliverRate),
    PublisherLimiterConfig = emqx_limiter:config(max_publish, RetainerConf),
    [
        {?DISPATCHER_LIMITER_NAME, DispatcherLimiterConfig},
        {?PUBLISHER_LIMITER_NAME, PublisherLimiterConfig}
    ].
