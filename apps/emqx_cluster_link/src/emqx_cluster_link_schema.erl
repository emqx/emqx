%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_schema).

-behaviour(emqx_schema_hooks).

-include("emqx_cluster_link.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([injected_fields/0]).

%% Used in emqx_cluster_link_api
-export([links_schema/1, link_schema/0]).

-export([
    roots/0,
    fields/1,
    namespace/0,
    desc/1
]).

-export_type([link/0]).

-type link() :: #{
    name := binary(),
    enable := boolean(),
    server := binary(),
    topics := [emqx_types:topic()],
    clientid => binary(),
    username => binary(),
    password => emqx_secret:t(binary()),
    ssl := #{atom() => _},
    retry_interval := non_neg_integer(),
    max_inflight := pos_integer(),
    atom() => _
}.

-define(MQTT_HOST_OPTS, #{default_port => 1883}).

namespace() -> "cluster".

roots() -> [].

injected_fields() ->
    #{
        cluster => [
            {links, links_schema(#{})}
        ]
    }.

links_schema(Meta) ->
    ?HOCON(?ARRAY(?R_REF("link")), Meta#{
        default => [],
        validator => fun validate_unique_names/1,
        desc => ?DESC("links")
    }).

link_schema() ->
    hoconsc:ref(?MODULE, "link").

fields("link") ->
    [
        {enable,
            ?HOCON(boolean(), #{
                default => true,
                importance => ?IMPORTANCE_NO_DOC,
                desc => ?DESC(enable)
            })},
        {name, ?HOCON(binary(), #{required => true, desc => ?DESC(link_name)})},
        {server,
            emqx_schema:servers_sc(#{required => true, desc => ?DESC(server)}, ?MQTT_HOST_OPTS)},
        {clientid, ?HOCON(binary(), #{required => false, desc => ?DESC(clientid)})},
        {username, ?HOCON(binary(), #{required => false, desc => ?DESC(username)})},
        {password, emqx_schema_secret:mk(#{required => false, desc => ?DESC(password)})},
        {ssl, #{
            type => ?R_REF(emqx_schema, "ssl_client_opts"),
            default => #{<<"enable">> => false},
            desc => ?DESC(ssl)
        }},
        {topics,
            ?HOCON(?ARRAY(binary()), #{
                desc => ?DESC(topics),
                required => true,
                validator => [
                    fun validate_topics/1,
                    fun validate_no_special_topics/1,
                    fun validate_no_redundancy/1
                ]
            })},
        {pool_size, ?HOCON(pos_integer(), #{default => 8, desc => ?DESC(pool_size)})},
        {retry_interval,
            emqx_schema:mk_duration(
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
validate_unique_names(Links) ->
    Names = [link_name(L) || L <- Links],
    Dups = Names -- lists:usort(Names),
    Dups == [] orelse mk_error(duplicated_cluster_links, duplicates, Dups).

link_name(#{name := Name}) -> Name;
link_name(#{<<"name">> := Name}) -> Name.

validate_topics(Topics) ->
    Errors = lists:flatmap(fun validate_topic/1, Topics),
    Errors == [] orelse mk_error(invalid_topics, topics, Errors).

validate_topic(Topic) ->
    try
        emqx_topic:validate(Topic),
        []
    catch
        error:Reason ->
            [{Topic, Reason}]
    end.

validate_no_special_topics(Topics) ->
    Errors = lists:flatmap(fun validate_sys_link_topic/1, Topics),
    Errors == [] orelse mk_error(invalid_topics, topics, Errors).

validate_sys_link_topic(Topic) ->
    [{Topic, topic_not_allowed} || emqx_topic:match(Topic, ?TOPIC_PREFIX_WILDCARD)].

validate_no_redundancy(Topics) ->
    Redundant = Topics -- emqx_topic:union(Topics),
    Redundant == [] orelse mk_error(redundant_topics, topics, Redundant).

mk_error(Reason, Field, Errors) ->
    {error, #{reason => Reason, Field => Errors}}.
