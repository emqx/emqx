%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = mria:wait_for_tables(emqx_cluster_link_extrouter:create_tables()),
    ok = emqx_cluster_link_config:load(),
    ok = emqx_cluster_link:register_external_broker(),
    ok = emqx_cluster_link:put_hook(),
    {ok, Sup} = emqx_cluster_link_sup:start_link(),
    lists:foreach(
        fun(LinkConf) ->
            {ok, _} = emqx_cluster_link_sup:ensure_actor(LinkConf),
            {ok, _} = emqx_cluster_link_mqtt:ensure_msg_fwd_resource(LinkConf)
        end,
        emqx_cluster_link_config:get_enabled_links()
    ),
    {ok, Sup}.

stop(_State) ->
    _ = emqx_cluster_link:delete_hook(),
    _ = emqx_cluster_link:unregister_external_broker(),
    _ = emqx_cluster_link_config:unload(),
    lists:foreach(
        fun(_LinkConf = #{name := ClusterName}) ->
            emqx_cluster_link_mqtt:remove_msg_fwd_resource(ClusterName)
        end,
        emqx_cluster_link_config:get_links()
    ).
