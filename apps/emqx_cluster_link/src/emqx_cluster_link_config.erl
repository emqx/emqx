%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_config).

-behaviour(emqx_config_handler).

-include_lib("emqx/include/logger.hrl").

-define(LINKS_PATH, [cluster, links]).
-define(CERTS_PATH(LinkName), filename:join(["cluster", "links", LinkName])).

-define(MQTT_HOST_OPTS, #{default_port => 1883}).

-ifndef(TEST).
-define(DEFAULT_ACTOR_TTL, 30_000).
-else.
-define(DEFAULT_ACTOR_TTL, 3_000).
-endif.

-export([
    %% General
    update/1,
    cluster/0,
    enabled_links/0,
    links/0,
    link/1,
    topic_filters/1,
    %% Connections
    emqtt_options/1,
    mk_emqtt_options/1,
    %% Actor Lifecycle
    actor_ttl/0,
    actor_gc_interval/0,
    actor_heartbeat_interval/0
]).

-export([
    add_handler/0,
    remove_handler/0
]).

-export([
    pre_config_update/3,
    post_config_update/5
]).

%%

update(Config) ->
    case
        emqx_conf:update(
            ?LINKS_PATH,
            Config,
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, #{raw_config := NewConfigRows}} ->
            {ok, NewConfigRows};
        {error, Reason} ->
            {error, Reason}
    end.

cluster() ->
    atom_to_binary(emqx_config:get([cluster, name])).

links() ->
    emqx:get_config(?LINKS_PATH, []).

enabled_links() ->
    [L || L = #{enable := true} <- links()].

link(Name) ->
    case lists:dropwhile(fun(L) -> Name =/= upstream_name(L) end, links()) of
        [LinkConf | _] -> LinkConf;
        [] -> undefined
    end.

emqtt_options(LinkName) ->
    emqx_maybe:apply(fun mk_emqtt_options/1, ?MODULE:link(LinkName)).

topic_filters(LinkName) ->
    maps:get(topics, ?MODULE:link(LinkName), []).

-spec actor_ttl() -> _Milliseconds :: pos_integer().
actor_ttl() ->
    ?DEFAULT_ACTOR_TTL.

-spec actor_gc_interval() -> _Milliseconds :: pos_integer().
actor_gc_interval() ->
    actor_ttl().

-spec actor_heartbeat_interval() -> _Milliseconds :: pos_integer().
actor_heartbeat_interval() ->
    actor_ttl() div 3.

%%

mk_emqtt_options(#{server := Server, ssl := #{enable := EnableSsl} = Ssl} = LinkConf) ->
    ClientId = maps:get(clientid, LinkConf, cluster()),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?MQTT_HOST_OPTS),
    Opts = #{
        host => Host,
        port => Port,
        clientid => ClientId,
        proto_ver => v5,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl))
    },
    with_password(with_user(Opts, LinkConf), LinkConf).

with_user(Opts, #{username := U} = _LinkConf) ->
    Opts#{username => U};
with_user(Opts, _LinkConf) ->
    Opts.

with_password(Opts, #{password := P} = _LinkConf) ->
    Opts#{password => emqx_secret:unwrap(P)};
with_password(Opts, _LinkConf) ->
    Opts.

%%

add_handler() ->
    ok = emqx_config_handler:add_handler(?LINKS_PATH, ?MODULE).

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?LINKS_PATH).

pre_config_update(?LINKS_PATH, RawConf, RawConf) ->
    {ok, RawConf};
pre_config_update(?LINKS_PATH, NewRawConf, OldRawConf) ->
    {ok, convert_certs(maybe_increment_ps_actor_incr(NewRawConf, OldRawConf))}.

post_config_update(?LINKS_PATH, _Req, Old, Old, _AppEnvs) ->
    ok;
post_config_update(?LINKS_PATH, _Req, New, Old, _AppEnvs) ->
    ok = toggle_hook_and_broker(enabled_links(New), enabled_links(Old)),
    #{
        removed := Removed,
        added := Added,
        changed := Changed
    } = emqx_utils:diff_lists(New, Old, fun upstream_name/1),
    RemovedRes = remove_links(Removed),
    AddedRes = add_links(Added),
    UpdatedRes = update_links(Changed),
    IsAllOk = all_ok(RemovedRes) andalso all_ok(AddedRes) andalso all_ok(UpdatedRes),
    case IsAllOk of
        true ->
            ok;
        false ->
            {error, #{added => AddedRes, removed => RemovedRes, updated => UpdatedRes}}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

toggle_hook_and_broker([_ | _] = _NewEnabledLinks, [] = _OldEnabledLinks) ->
    ok = emqx_cluster_link:register_external_broker(),
    ok = emqx_cluster_link:put_hook();
toggle_hook_and_broker([] = _NewEnabledLinks, _OldLinks) ->
    ok = emqx_cluster_link:unregister_external_broker(),
    ok = emqx_cluster_link:delete_hook();
toggle_hook_and_broker(_, _) ->
    ok.

enabled_links(LinksConf) ->
    [L || #{enable := true} = L <- LinksConf].

all_ok(Results) ->
    lists:all(
        fun
            (ok) -> true;
            ({ok, _}) -> true;
            (_) -> false
        end,
        Results
    ).

add_links(LinksConf) ->
    [add_link(Link) || Link <- LinksConf].

add_link(#{enable := true} = LinkConf) ->
    {ok, _Pid} = emqx_cluster_link_sup:ensure_actor(LinkConf),
    {ok, _} = emqx_cluster_link_mqtt:ensure_msg_fwd_resource(LinkConf),
    ok;
add_link(_DisabledLinkConf) ->
    ok.

remove_links(LinksConf) ->
    [remove_link(Name) || #{upstream := Name} <- LinksConf].

remove_link(Name) ->
    _ = emqx_cluster_link_mqtt:remove_msg_fwd_resource(Name),
    ensure_actor_stopped(Name).

update_links(LinksConf) ->
    [update_link(Link) || Link <- LinksConf].

update_link({OldLinkConf, #{enable := true, upstream := Name} = NewLinkConf}) ->
    _ = ensure_actor_stopped(Name),
    {ok, _Pid} = emqx_cluster_link_sup:ensure_actor(NewLinkConf),
    %% TODO: if only msg_fwd resource related config is changed,
    %% we can skip actor reincarnation/restart.
    ok = update_msg_fwd_resource(OldLinkConf, NewLinkConf),
    ok;
update_link({_OldLinkConf, #{enable := false, upstream := Name} = _NewLinkConf}) ->
    _ = emqx_cluster_link_mqtt:remove_msg_fwd_resource(Name),
    ensure_actor_stopped(Name).

update_msg_fwd_resource(#{pool_size := Old}, #{pool_size := Old} = NewConf) ->
    {ok, _} = emqx_cluster_link_mqtt:ensure_msg_fwd_resource(NewConf),
    ok;
update_msg_fwd_resource(_, #{upstream := Name} = NewConf) ->
    _ = emqx_cluster_link_mqtt:remove_msg_fwd_resource(Name),
    {ok, _} = emqx_cluster_link_mqtt:ensure_msg_fwd_resource(NewConf),
    ok.

ensure_actor_stopped(ClusterName) ->
    emqx_cluster_link_sup:ensure_actor_stopped(ClusterName).

upstream_name(#{upstream := N}) -> N;
upstream_name(#{<<"upstream">> := N}) -> N.

maybe_increment_ps_actor_incr(New, Old) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            %% TODO: what if a link was removed and then added again?
            %% Assume that incarnation was 0 when the link was removed
            %% and now it's also 0 (a default value for new actor).
            %% If persistent routing state changed during this link absence
            %% and remote GC has not started before ps actor restart (with the same incarnation),
            %% then some old (stale) external ps routes may be never cleaned on the remote side.
            %% No (permanent) message loss is expected, as new actor incrantaion will re-bootstrap.
            %% Similarly, irrelevant messages will be filtered out at receiving end, so
            %% the main risk is having some stale routes unreachable for GC...
            #{changed := Changed} = emqx_utils:diff_lists(New, Old, fun upstream_name/1),
            ChangedNames = [upstream_name(C) || {_, C} <- Changed],
            lists:foldr(
                fun(LConf, Acc) ->
                    case lists:member(upstream_name(LConf), ChangedNames) of
                        true -> [increment_ps_actor_incr(LConf) | Acc];
                        false -> [LConf | Acc]
                    end
                end,
                [],
                New
            );
        false ->
            New
    end.

increment_ps_actor_incr(#{ps_actor_incarnation := Incr} = Conf) ->
    Conf#{ps_actor_incarnation => Incr + 1};
increment_ps_actor_incr(#{<<"ps_actor_incarnation">> := Incr} = Conf) ->
    Conf#{<<"ps_actor_incarnation">> => Incr + 1};
%% Default value set in schema is 0, so need to set it to 1 during the first update.
increment_ps_actor_incr(#{<<"upstream">> := _} = Conf) ->
    Conf#{<<"ps_actor_incarnation">> => 1};
increment_ps_actor_incr(#{upstream := _} = Conf) ->
    Conf#{ps_actor_incarnation => 1}.

convert_certs(LinksConf) ->
    lists:map(
        fun
            (#{ssl := SSLOpts} = LinkConf) ->
                LinkConf#{ssl => do_convert_certs(upstream_name(LinkConf), SSLOpts)};
            (#{<<"ssl">> := SSLOpts} = LinkConf) ->
                LinkConf#{<<"ssl">> => do_convert_certs(upstream_name(LinkConf), SSLOpts)};
            (LinkConf) ->
                LinkConf
        end,
        LinksConf
    ).

do_convert_certs(LinkName, SSLOpts) ->
    case emqx_tls_lib:ensure_ssl_files(?CERTS_PATH(LinkName), SSLOpts) of
        {ok, undefined} ->
            SSLOpts;
        {ok, SSLOpts1} ->
            SSLOpts1;
        {error, Reason} ->
            ?SLOG(
                error,
                #{
                    msg => "bad_ssl_config",
                    config_path => ?LINKS_PATH,
                    name => LinkName,
                    reason => Reason
                }
            ),
            throw({bad_ssl_config, Reason})
    end.
