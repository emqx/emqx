%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_config).

-feature(maybe_expr, enable).

-include_lib("emqx/include/logger.hrl").

-define(LINKS_PATH, [cluster, links]).
-define(CERTS_PATH(LinkName), filename:join(["cluster", "links", LinkName])).

-define(MQTT_HOST_OPTS, #{default_port => 1883}).

-ifndef(TEST).
-define(DEFAULT_ACTOR_TTL, 30_000).
-else.
-define(DEFAULT_ACTOR_TTL, 3_000).
-endif.

-define(COMMON_FIELDS, [username, password, clientid, server, ssl]).
%% NOTE: retry_interval, max_inflight may be used for router syncer client as well,
%% but for now they are not.
-define(MSG_RES_FIELDS, [resource_opts, pool_size, retry_interval, max_inflight]).
%% Excludes a special hidden `ps_actor_incarnation` field.
-define(ACTOR_FIELDS, [topics]).

-export([
    %% Shortconf
    links/0,
    link/1,
    enabled_links/0,
    %% Configuration
    cluster/0,
    get_links/0,
    get_link/1,
    get_enabled_links/0,
    get_link_raw/1,
    %% Connections
    mk_emqtt_options/1,
    %% Actor Lifecycle
    actor_ttl/0,
    actor_gc_interval/0,
    actor_heartbeat_interval/0
]).

%% Managing configuration:
-export([
    update/1,
    create_link/1,
    delete_link/1,
    update_link/1
]).

%% Application lifecycle:
-export([
    load/0,
    unload/0
]).

-behaviour(emqx_config_handler).
-export([
    pre_config_update/3,
    post_config_update/5
]).

%% Test exports
-export([prepare_link/1]).

-export_type([shortconf/0]).

-type shortconf() :: #{
    name := binary(),
    enable := boolean(),
    topics := _Union :: [emqx_types:words()]
}.

-define(PTERM(K), {?MODULE, K}).

%%

cluster() ->
    atom_to_binary(emqx_config:get([cluster, name])).

-spec links() -> [shortconf()].
links() ->
    get_shortconf(links).

-spec enabled_links() -> [shortconf()].
enabled_links() ->
    get_shortconf(enabled).

-spec link(_Name :: binary()) -> shortconf() | undefined.
link(Name) ->
    find_link(Name, links()).

-spec get_links() -> [emqx_cluster_link_schema:link()].
get_links() ->
    emqx:get_config(?LINKS_PATH, []).

-spec get_enabled_links() -> [emqx_cluster_link_schema:link()].
get_enabled_links() ->
    [L || L = #{enable := true} <- get_links()].

-spec get_link(_Name :: binary()) -> emqx_cluster_link_schema:link() | undefined.
get_link(Name) ->
    find_link(Name, get_links()).

-spec get_link_raw(_Name :: binary()) -> emqx_config:raw_config().
get_link_raw(Name) ->
    find_link(Name, get_links_raw()).

get_links_raw() ->
    emqx:get_raw_config(?LINKS_PATH, []).

find_link(Name, Links) ->
    case lists:dropwhile(fun(L) -> Name =/= upstream_name(L) end, Links) of
        [LinkConf | _] -> LinkConf;
        [] -> undefined
    end.

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
    Opts = maps:with([username, retry_interval, max_inflight], LinkConf),
    Opts1 = Opts#{
        host => Host,
        port => Port,
        clientid => ClientId,
        proto_ver => v5,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl))
    },
    with_password(Opts1, LinkConf).

with_password(Opts, #{password := P} = _LinkConf) ->
    Opts#{password => emqx_secret:unwrap(P)};
with_password(Opts, _LinkConf) ->
    Opts.

%%

create_link(LinkConfig) ->
    #{<<"name">> := Name} = LinkConfig,
    case
        emqx_conf:update(
            ?LINKS_PATH,
            {create, LinkConfig},
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, #{raw_config := NewConfigRows}} ->
            NewLinkConfig = find_link(Name, NewConfigRows),
            {ok, NewLinkConfig};
        {error, Reason} ->
            {error, Reason}
    end.

delete_link(Name) ->
    case
        emqx_conf:update(
            ?LINKS_PATH,
            {delete, Name},
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

update_link(LinkConfig) ->
    #{<<"name">> := Name} = LinkConfig,
    case
        emqx_conf:update(
            ?LINKS_PATH,
            {update, LinkConfig},
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, #{raw_config := NewConfigRows}} ->
            NewLinkConfig = find_link(Name, NewConfigRows),
            {ok, NewLinkConfig};
        {error, Reason} ->
            {error, Reason}
    end.

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

load() ->
    ok = prepare_shortconf(get_links()),
    ok = emqx_config_handler:add_handler(?LINKS_PATH, ?MODULE).

unload() ->
    ok = emqx_config_handler:remove_handler(?LINKS_PATH),
    ok = cleanup_shortconf().

pre_config_update(?LINKS_PATH, RawConf, RawConf) ->
    {ok, RawConf};
pre_config_update(?LINKS_PATH, {create, LinkRawConf}, OldRawConf) ->
    #{<<"name">> := Name} = LinkRawConf,
    maybe
        undefined ?= find_link(Name, OldRawConf),
        NewRawConf0 = OldRawConf ++ [LinkRawConf],
        NewRawConf = convert_certs(maybe_increment_ps_actor_incr(NewRawConf0, OldRawConf)),
        {ok, NewRawConf}
    else
        _ ->
            {error, already_exists}
    end;
pre_config_update(?LINKS_PATH, {update, LinkRawConf}, OldRawConf) ->
    #{<<"name">> := Name} = LinkRawConf,
    maybe
        {_Found, Front, Rear} ?= safe_take(Name, OldRawConf),
        NewRawConf0 = Front ++ [LinkRawConf] ++ Rear,
        NewRawConf = convert_certs(maybe_increment_ps_actor_incr(NewRawConf0, OldRawConf)),
        {ok, NewRawConf}
    else
        not_found ->
            {error, not_found}
    end;
pre_config_update(?LINKS_PATH, {delete, Name}, OldRawConf) ->
    maybe
        {_Found, Front, Rear} ?= safe_take(Name, OldRawConf),
        NewRawConf = Front ++ Rear,
        {ok, NewRawConf}
    else
        _ ->
            {error, not_found}
    end;
pre_config_update(?LINKS_PATH, NewRawConf, OldRawConf) ->
    {ok, convert_certs(maybe_increment_ps_actor_incr(NewRawConf, OldRawConf))}.

post_config_update(?LINKS_PATH, _Req, Old, Old, _AppEnvs) ->
    ok;
post_config_update(?LINKS_PATH, _Req, New, Old, _AppEnvs) ->
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
            prepare_shortconf(New);
        false ->
            {error, #{added => AddedRes, removed => RemovedRes, updated => UpdatedRes}}
    end.

%%

prepare_shortconf(Config) ->
    Links = [prepare_link(L) || L <- Config],
    ok = persistent_term:put(?PTERM(links), Links),
    ok = persistent_term:put(?PTERM(enabled), [L || L = #{enable := true} <- Links]).

cleanup_shortconf() ->
    _ = persistent_term:erase(?PTERM(links)),
    _ = persistent_term:erase(?PTERM(enabled)),
    ok.

prepare_link(#{name := Name, enable := Enabled, topics := Topics}) ->
    #{
        name => Name,
        enable => Enabled,
        topics => prepare_topics(Topics)
    }.

prepare_topics(Topics) ->
    Union = emqx_topic:union(Topics),
    lists:map(fun emqx_topic:words/1, Union).

get_shortconf(K) ->
    persistent_term:get(?PTERM(K)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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

add_link(#{name := ClusterName, enable := true} = LinkConf) ->
    {ok, _Pid} = emqx_cluster_link_sup:ensure_actor(LinkConf),
    {ok, _} = emqx_cluster_link_mqtt:ensure_msg_fwd_resource(LinkConf),
    ok = emqx_cluster_link_metrics:maybe_create_metrics(ClusterName),
    ok;
add_link(_DisabledLinkConf) ->
    ok.

remove_links(LinksConf) ->
    [remove_link(Name) || #{name := Name} <- LinksConf].

remove_link(Name) ->
    _ = emqx_cluster_link_mqtt:remove_msg_fwd_resource(Name),
    _ = ensure_actor_stopped(Name),
    emqx_cluster_link_metrics:drop_metrics(Name).

update_links(LinksConf) ->
    [do_update_link(Link) || Link <- LinksConf].

do_update_link({OldLinkConf, #{enable := true, name := Name} = NewLinkConf}) ->
    case what_is_changed(OldLinkConf, NewLinkConf) of
        both ->
            _ = ensure_actor_stopped(Name),
            {ok, _Pid} = emqx_cluster_link_sup:ensure_actor(NewLinkConf),
            ok;
        actor ->
            _ = ensure_actor_stopped(Name),
            {ok, _Pid} = emqx_cluster_link_sup:ensure_actor(NewLinkConf),
            ok;
        _ ->
            ok
    end,
    ok = update_msg_fwd_resource(OldLinkConf, NewLinkConf);
do_update_link({_OldLinkConf, #{enable := false, name := Name} = _NewLinkConf}) ->
    _ = emqx_cluster_link_mqtt:remove_msg_fwd_resource(Name),
    ensure_actor_stopped(Name).

what_is_changed(OldLink, NewLink) ->
    CommonChanged = are_fields_changed(?COMMON_FIELDS, OldLink, NewLink),
    ActorChanged = are_fields_changed(?ACTOR_FIELDS, OldLink, NewLink),
    MsgResChanged = are_fields_changed(?MSG_RES_FIELDS, OldLink, NewLink),
    AllChanged = ActorChanged andalso MsgResChanged,
    case CommonChanged orelse AllChanged of
        true ->
            both;
        false ->
            %% This function is only applicable when it's certain that link conf is changed,
            %% so if resource fields are the same,
            %% then some other actor-related fields are definitely changed.
            case MsgResChanged of
                true -> msg_resource;
                false -> actor
            end
    end.

are_fields_changed(Fields, OldLink, NewLink) ->
    maps:with(Fields, OldLink) =/= maps:with(Fields, NewLink).

update_msg_fwd_resource(_, #{name := Name} = NewConf) ->
    _ = emqx_cluster_link_mqtt:remove_msg_fwd_resource(Name),
    {ok, _} = emqx_cluster_link_mqtt:ensure_msg_fwd_resource(NewConf),
    ok.

ensure_actor_stopped(ClusterName) ->
    emqx_cluster_link_sup:ensure_actor_stopped(ClusterName).

upstream_name(#{name := N}) -> N;
upstream_name(#{<<"name">> := N}) -> N.

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
increment_ps_actor_incr(#{<<"name">> := _} = Conf) ->
    Conf#{<<"ps_actor_incarnation">> => 1};
increment_ps_actor_incr(#{name := _} = Conf) ->
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
    case emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(?CERTS_PATH(LinkName), SSLOpts) of
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

safe_take(Name, Transformations) ->
    case lists:splitwith(fun(#{<<"name">> := N}) -> N =/= Name end, Transformations) of
        {_Front, []} ->
            not_found;
        {Front, [Found | Rear]} ->
            {Found, Front, Rear}
    end.
