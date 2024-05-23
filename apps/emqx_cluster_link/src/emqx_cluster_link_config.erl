%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_config).

-behaviour(emqx_config_handler).

-include_lib("emqx/include/logger.hrl").

-define(LINKS_PATH, [cluster, links]).
-define(CERTS_PATH(LinkName), filename:join(["cluster", "links", LinkName])).

-define(MQTT_HOST_OPTS, #{default_port => 1883}).

-export([
    %% General
    cluster/0,
    enabled_links/0,
    links/0,
    link/1,
    topic_filters/1,
    %% Connections
    emqtt_options/1,
    mk_emqtt_options/1
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

%%

mk_emqtt_options(#{server := Server, ssl := #{enable := EnableSsl} = Ssl} = LinkConf) ->
    ClientId = maps:get(client_id, LinkConf, cluster()),
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
pre_config_update(?LINKS_PATH, NewRawConf, _RawConf) ->
    {ok, convert_certs(NewRawConf)}.

post_config_update(?LINKS_PATH, _Req, Old, Old, _AppEnvs) ->
    ok;
post_config_update(?LINKS_PATH, _Req, New, Old, _AppEnvs) ->
    ok = maybe_toggle_hook_and_provider(New),
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

maybe_toggle_hook_and_provider(LinksConf) ->
    case is_any_enabled(LinksConf) of
        true ->
            ok = emqx_cluster_link:register_external_broker(),
            ok = emqx_cluster_link:put_hook();
        false ->
            _ = emqx_cluster_link:delete_hook(),
            _ = emqx_cluster_link:unregister_external_broker(),
            ok
    end.

is_any_enabled(LinksConf) ->
    lists:any(
        fun(#{enable := IsEnabled}) -> IsEnabled =:= true end,
        LinksConf
    ).

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

add_link(#{enabled := true} = LinkConf) ->
    %% NOTE: this can be started later during init_link phase, but it looks not harmful to start it beforehand...
    MsgFwdRes = emqx_cluster_link_mqtt:ensure_msg_fwd_resource(LinkConf),
    %% TODO
    ActorRes = ok,
    combine_results(ActorRes, MsgFwdRes);
add_link(_DisabledLinkConf) ->
    ok.

remove_links(LinksConf) ->
    [remove_link(Link) || Link <- LinksConf].

remove_link(_LinkConf) ->
    %% TODO
    ok.

update_links(LinksConf) ->
    [update_link(Link) || Link <- LinksConf].

%% TODO: do some updates without restart (at least without coordinator restart and re-election)
update_link(#{enabled := true} = LinkConf) ->
    _ = remove_link(LinkConf),
    add_link(LinkConf);
update_link(#{enabled := false} = LinkConf) ->
    case remove_link(LinkConf) of
        {error, not_found} -> ok;
        Other -> Other
    end.

combine_results(ok, ok) ->
    ok;
combine_results(CoordRes, MsgFwdRes) ->
    {error, #{coordinator => CoordRes, msg_fwd_resource => MsgFwdRes}}.

upstream_name(#{upstream := N}) -> N;
upstream_name(#{<<"upstream">> := N}) -> N.

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
