%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_app).

-behaviour(application).

-export([start/2, prep_stop/1, stop/1]).

-define(BROKER_MOD, emqx_cluster_link).

start(_StartType, _StartArgs) ->
    ok = mria:wait_for_tables(emqx_cluster_link_extrouter:create_tables()),
    emqx_cluster_link_config:add_handler(),
    LinksConf = enabled_links(),
    _ =
        case LinksConf of
            [_ | _] ->
                ok = emqx_cluster_link:register_external_broker(),
                ok = emqx_cluster_link:put_hook(),
                ok = start_msg_fwd_resources(LinksConf);
            _ ->
                ok
        end,
    emqx_cluster_link_sup:start_link(LinksConf).

prep_stop(State) ->
    emqx_cluster_link_config:remove_handler(),
    State.

stop(_State) ->
    _ = emqx_cluster_link:delete_hook(),
    _ = emqx_cluster_link:unregister_external_broker(),
    _ = stop_msg_fwd_resources(emqx_cluster_link_config:links()),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

enabled_links() ->
    lists:filter(
        fun(#{enable := IsEnabled}) -> IsEnabled =:= true end,
        emqx_cluster_link_config:links()
    ).

start_msg_fwd_resources(LinksConf) ->
    lists:foreach(
        fun(LinkConf) ->
            {ok, _} = emqx_cluster_link_mqtt:ensure_msg_fwd_resource(LinkConf)
        end,
        LinksConf
    ).

stop_msg_fwd_resources(LinksConf) ->
    lists:foreach(
        fun(#{upstream := Name}) ->
            emqx_cluster_link_mqtt:stop_msg_fwd_resource(Name)
        end,
        LinksConf
    ).
