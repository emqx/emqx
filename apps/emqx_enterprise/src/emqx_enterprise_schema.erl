%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_enterprise_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([namespace/0, roots/0, fields/1, translations/0, translation/1, desc/1, validations/0]).

-define(EE_SCHEMA_MODULES, [
    emqx_license_schema,
    emqx_schema_registry_schema,
    emqx_ft_schema
]).

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
                emqx_conf_schema:file(),
                #{
                    desc => ?DESC(emqx_conf_schema, "audit_file_handler_path"),
                    default => <<"${EMQX_LOG_DIR}/audit.log">>,
                    importance => ?IMPORTANCE_HIGH,
                    converter => fun(Path, Opts) ->
                        emqx_schema:naive_env_interpolation(
                            emqx_conf_schema:ensure_unicode_path(Path, Opts)
                        )
                    end
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
            )}
    ] ++ CommonConfs1;
fields(Name) ->
    ee_delegate(fields, ?EE_SCHEMA_MODULES, Name).

translations() ->
    emqx_conf_schema:translations().

translation(Name) ->
    emqx_conf_schema:translation(Name).

desc("log_audit_handler") ->
    ?DESC(emqx_conf_schema, "desc_audit_log_handler");
desc(Name) ->
    ee_delegate(desc, ?EE_SCHEMA_MODULES, Name).

validations() ->
    emqx_conf_schema:validations() ++ emqx_license_schema:validations().

%%------------------------------------------------------------------------------
%% helpers
%%------------------------------------------------------------------------------

ee_roots() ->
    lists:flatmap(
        fun(Module) ->
            apply(Module, roots, [])
        end,
        ?EE_SCHEMA_MODULES
    ).

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
        {"node", #{type => hoconsc:ref(?MODULE, "node")}},
        {"log", #{type => hoconsc:ref(?MODULE, "log")}}
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
