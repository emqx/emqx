%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_enterprise_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([namespace/0, roots/0, fields/1, translations/0, translation/1, desc/1, validations/0]).
-export([upgrade_raw_conf/1]).
-export([injected_fields/0]).

%% DO NOT REMOVE.  This is used in other repositories.
-define(EXTRA_SCHEMA_MODULES, []).
-define(EE_SCHEMA_MODULES, [
    emqx_license_schema,
    emqx_schema_registry_schema,
    emqx_schema_validation_schema,
    emqx_message_transformation_schema,
    emqx_ft_schema,
    emqx_ds_shared_sub_schema
]).

%% Callback to upgrade config after loaded from config file but before validation.
upgrade_raw_conf(RawConf0) ->
    RawConf = emqx_bridge_gcp_pubsub:upgrade_raw_conf(RawConf0),
    emqx_conf_schema:upgrade_raw_conf(RawConf).

namespace() ->
    emqx_conf_schema:namespace().

roots() ->
    redefine_roots(emqx_conf_schema:roots()) ++ ee_roots().

fields("node") ->
    redefine_node(emqx_conf_schema:fields("node"));
fields("log") ->
    redefine_log(emqx_conf_schema:fields("log"));
fields("log_audit_handler") ->
    CommonConfs = emqx_conf_schema:log_handler_common_confs(file, #{}),
    CommonConfs1 = lists:filter(
        fun({Key, _}) ->
            not lists:member(Key, ["level", "formatter"])
        end,
        CommonConfs
    ),
    [
        {"level",
            hoconsc:mk(
                emqx_conf_schema:log_level(),
                #{
                    default => info,
                    desc => ?DESC(emqx_conf_schema, "audit_handler_level"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"path",
            hoconsc:mk(
                string(),
                #{
                    desc => ?DESC(emqx_conf_schema, "audit_file_handler_path"),
                    default => <<"${EMQX_LOG_DIR}/audit.log">>,
                    importance => ?IMPORTANCE_HIGH,
                    converter => fun emqx_conf_schema:log_file_path_converter/2
                }
            )},
        {"rotation_count",
            hoconsc:mk(
                range(1, 128),
                #{
                    default => 10,
                    converter => fun emqx_conf_schema:convert_rotation/2,
                    desc => ?DESC(emqx_conf_schema, "log_rotation_count"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"rotation_size",
            hoconsc:mk(
                hoconsc:union([infinity, emqx_schema:bytesize()]),
                #{
                    default => <<"50MB">>,
                    desc => ?DESC(emqx_conf_schema, "log_file_handler_max_size"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"max_filter_size",
            hoconsc:mk(
                range(10, 30000),
                #{
                    default => 5000,
                    desc => ?DESC(emqx_conf_schema, "audit_log_max_filter_limit"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"ignore_high_frequency_request",
            hoconsc:mk(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(emqx_conf_schema, "audit_log_ignore_high_frequency_request"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ] ++ CommonConfs1;
fields(Name) ->
    ee_delegate(fields, ee_mods(), Name).

translations() ->
    emqx_conf_schema:translations().

translation("prometheus") ->
    [
        {"collectors", fun tr_prometheus_collectors/1}
    ];
translation(Name) ->
    emqx_conf_schema:translation(Name).

desc("log_audit_handler") ->
    ?DESC(emqx_conf_schema, "desc_audit_log_handler");
desc(Name) ->
    ee_delegate(desc, ee_mods(), Name).

validations() ->
    emqx_conf_schema:validations() ++ emqx_license_schema:validations().

injected_fields() ->
    #{
        'node.role' => [replicant]
    }.

%%------------------------------------------------------------------------------
%% helpers
%%------------------------------------------------------------------------------

ee_roots() ->
    lists:flatmap(
        fun(Module) ->
            apply(Module, roots, [])
        end,
        ee_mods()
    ).

ee_mods() -> ?EXTRA_SCHEMA_MODULES ++ ?EE_SCHEMA_MODULES.

ee_delegate(Method, [EEMod | EEMods], Name) ->
    case lists:member(Name, apply(EEMod, roots, [])) of
        true ->
            apply(EEMod, Method, [Name]);
        false ->
            ee_delegate(Method, EEMods, Name)
    end;
ee_delegate(Method, [], Name) ->
    apply(emqx_conf_schema, Method, [Name]).

redefine_roots(Roots) ->
    Overrides = [
        {node, #{type => hoconsc:ref(?MODULE, "node")}},
        {log, #{type => hoconsc:ref(?MODULE, "log")}}
    ],
    override(Roots, Overrides).

redefine_node(Fields) ->
    Overrides = [],
    override(Fields, Overrides).

redefine_log(Fields) ->
    Overrides = [],
    override(Fields, Overrides) ++ audit_log_conf().

override(Fields, []) ->
    Fields;
override(Fields, [{Name, Override} | More]) ->
    Schema = find_schema(Name, Fields),
    NewSchema = hocon_schema:override(Schema, Override),
    NewFields = replace_schema(Name, NewSchema, Fields),
    override(NewFields, More).

find_schema(Name, Fields) ->
    {Name, Schema} = lists:keyfind(Name, 1, Fields),
    Schema.

replace_schema(Name, Schema, Fields) ->
    lists:keyreplace(Name, 1, Fields, {Name, Schema}).

audit_log_conf() ->
    [
        {"audit",
            hoconsc:mk(
                hoconsc:ref(?MODULE, "log_audit_handler"),
                #{
                    %% note: we need to keep the descriptions associated with
                    %% `emqx_conf_schema' module hocon i18n file because that's what
                    %% `emqx_conf:gen_config_md' seems to expect.
                    desc => ?DESC(emqx_conf_schema, "log_audit_handler"),
                    importance => ?IMPORTANCE_HIGH,
                    default => #{<<"enable">> => false, <<"level">> => <<"info">>}
                }
            )}
    ].

tr_prometheus_collectors(Conf) ->
    [
        {'/prometheus/schema_validation', emqx_prometheus_schema_validation},
        {'/prometheus/message_transformation', emqx_prometheus_message_transformation}
        | emqx_conf_schema:tr_prometheus_collectors(Conf)
    ].
