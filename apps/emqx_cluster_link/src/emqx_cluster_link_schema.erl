%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_schema).

-behaviour(emqx_schema_hooks).

-include("emqx_cluster_link.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([injected_fields/0]).

%% Used in emqx_cluster_link_api
-export([links_schema/1]).

-export([
    roots/0,
    fields/1,
    namespace/0,
    desc/1
]).

-import(emqx_schema, [mk_duration/2]).

-define(MQTT_HOST_OPTS, #{default_port => 1883}).

namespace() -> "cluster".

roots() -> [].

injected_fields() ->
    #{cluster => [{links, links_schema(#{})}]}.

links_schema(Meta) ->
    ?HOCON(?ARRAY(?R_REF("link")), Meta#{
        default => [], validator => fun links_validator/1, desc => ?DESC("links")
    }).

fields("link") ->
    [
        {enable, ?HOCON(boolean(), #{default => true, desc => ?DESC(enable)})},
        {upstream, ?HOCON(binary(), #{required => true, desc => ?DESC(upstream)})},
        {server,
            emqx_schema:servers_sc(#{required => true, desc => ?DESC(server)}, ?MQTT_HOST_OPTS)},
        {clientid, ?HOCON(binary(), #{desc => ?DESC(clientid)})},
        {username, ?HOCON(binary(), #{desc => ?DESC(username)})},
        {password, emqx_schema_secret:mk(#{desc => ?DESC(password)})},
        {ssl, #{
            type => ?R_REF(emqx_schema, "ssl_client_opts"),
            default => #{<<"enable">> => false},
            desc => ?DESC(ssl)
        }},
        {topics,
            ?HOCON(?ARRAY(binary()), #{
                desc => ?DESC(topics), required => true, validator => fun topics_validator/1
            })},
        {pool_size, ?HOCON(pos_integer(), #{default => 8, desc => ?DESC(pool_size)})},
        {retry_interval,
            mk_duration(
                "MQTT Message retry interval. Delay for the link to retry sending the QoS1/QoS2 "
                "messages in case of ACK not received.",
                #{default => <<"15s">>}
            )},
        {max_inflight,
            ?HOCON(
                non_neg_integer(),
                #{
                    default => 32,
                    desc => ?DESC("max_inflight")
                }
            )},
        {resource_opts,
            ?HOCON(
                ?R_REF(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, <<"resource_opts">>)
                }
            )},
        %% Must not be configured manually. The value is incremented by cluster link config handler
        %% and is used as a globally synchronized sequence to ensure persistent routes actors have
        %% the same next incarnation after each config change.
        {ps_actor_incarnation, ?HOCON(integer(), #{default => 0, importance => ?IMPORTANCE_HIDDEN})}
    ];
fields("creation_opts") ->
    Opts = emqx_resource_schema:fields("creation_opts"),
    [O || {Field, _} = O <- Opts, not is_hidden_res_opt(Field)].

desc("links") ->
    ?DESC("links");
desc("link") ->
    ?DESC("link");
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc(_) ->
    undefined.

is_hidden_res_opt(Field) ->
    lists:member(
        Field,
        [start_after_created, query_mode, enable_batch, batch_size, batch_time]
    ).

%% TODO: check that no link name equals local cluster name,
%% but this may be tricky since the link config is injected into cluster config (emqx_conf_schema).
links_validator(Links) ->
    {_, Dups} = lists:foldl(
        fun(Link, {Acc, DupAcc}) ->
            Name = link_name(Link),
            case Acc of
                #{Name := _} ->
                    {Acc, [Name | DupAcc]};
                _ ->
                    {Acc#{Name => undefined}, DupAcc}
            end
        end,
        {#{}, []},
        Links
    ),
    check_errors(Dups, duplicated_cluster_links, names).

link_name(#{upstream := Name}) -> Name;
link_name(#{<<"upstream">> := Name}) -> Name.

topics_validator(Topics) ->
    Errors = lists:foldl(
        fun(T, ErrAcc) ->
            try
                _ = emqx_topic:validate(T),
                validate_sys_link_topic(T, ErrAcc)
            catch
                E:R ->
                    [{T, {E, R}} | ErrAcc]
            end
        end,
        [],
        Topics
    ),
    check_errors(Errors, invalid_topics, topics).

validate_sys_link_topic(T, ErrAcc) ->
    case emqx_topic:match(T, ?TOPIC_PREFIX_WILDCARD) of
        true ->
            [{T, {error, topic_not_allowed}} | ErrAcc];
        false ->
            ErrAcc
    end.

check_errors([] = _Errors, _Reason, _ValuesField) ->
    ok;
check_errors(Errors, Reason, ValuesField) ->
    {error, #{reason => Reason, ValuesField => Errors}}.
