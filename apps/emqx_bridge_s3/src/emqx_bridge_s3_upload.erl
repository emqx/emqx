%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3_upload).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_s3.hrl").

-define(ACTION, ?ACTION_UPLOAD).

-behaviour(hocon_schema).
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% Interpreting options
-export([
    mk_key_template/1,
    mk_upload_options/1
]).

-export([
    bridge_v2_examples/1
]).

%% Internal exports
-export([
    convert_actions/2,
    validate_key_template/1
]).

-define(DEFAULT_AGGREG_BATCH_SIZE, 100).
-define(DEFAULT_AGGREG_BATCH_TIME, <<"10ms">>).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_s3".

roots() ->
    [].

fields(Field) when
    Field == "get_bridge_v2";
    Field == "put_bridge_v2";
    Field == "post_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION, fields(?ACTION));
fields(action) ->
    {?ACTION,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, ?ACTION)),
            #{
                desc => <<"S3 Upload Action Config">>,
                required => false,
                converter => fun ?MODULE:convert_actions/2
            }
        )};
fields(?ACTION) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            emqx_schema:mkunion(
                mode,
                #{
                    <<"direct">> => ?R_REF(s3_direct_upload_parameters),
                    <<"aggregated">> => ?R_REF(s3_aggregated_upload_parameters)
                },
                <<"direct">>
            ),
            #{
                required => true,
                desc => ?DESC(s3_upload),
                %% NOTE
                %% There seems to be no way to attach validators to union types, thus we
                %% have to attach a "common denominator" validator here.
                validator => validators(s3_upload_parameters)
            }
        ),
        #{
            resource_opts_ref => ?R_REF(s3_upload_resource_opts)
        }
    );
fields(s3_direct_upload_parameters) ->
    emqx_s3_schema:fields(s3_upload) ++
        [
            {mode,
                hoconsc:mk(
                    direct,
                    #{
                        default => <<"direct">>,
                        desc => ?DESC(s3_direct_upload_mode)
                    }
                )},
            {content,
                hoconsc:mk(
                    emqx_schema:template(),
                    #{
                        required => false,
                        default => <<"${.}">>,
                        desc => ?DESC(s3_object_content)
                    }
                )}
        ];
fields(s3_aggregated_upload_parameters) ->
    lists:append([
        [
            {mode,
                hoconsc:mk(
                    aggregated,
                    #{
                        required => true,
                        desc => ?DESC(s3_aggregated_upload_mode)
                    }
                )},
            {container,
                hoconsc:mk(
                    emqx_schema:mkunion(type, #{
                        <<"csv">> => ?REF(s3_aggregated_container_csv)
                    }),
                    #{
                        required => true,
                        default => #{<<"type">> => <<"csv">>},
                        desc => ?DESC(s3_aggregated_container)
                    }
                )},
            {aggregation,
                hoconsc:mk(
                    ?REF(s3_aggregation),
                    #{
                        required => true,
                        desc => ?DESC(s3_aggregation)
                    }
                )}
        ],
        emqx_resource_schema:override(emqx_s3_schema:fields(s3_upload), [
            {key, #{
                desc => ?DESC(s3_aggregated_upload_key),
                validator => fun ?MODULE:validate_key_template/1
            }}
        ]),
        emqx_s3_schema:fields(s3_uploader)
    ]);
fields(s3_aggregated_container_csv) ->
    [
        {type,
            hoconsc:mk(
                csv,
                #{
                    required => true,
                    desc => ?DESC(s3_aggregated_container_csv)
                }
            )},
        {column_order,
            hoconsc:mk(
                hoconsc:array(string()),
                #{
                    required => false,
                    default => [],
                    desc => ?DESC(s3_aggregated_container_csv_column_order)
                }
            )}
    ];
fields(s3_aggregation) ->
    [
        %% TODO: Needs bucketing? (e.g. messages falling in this 1h interval)
        {time_interval,
            hoconsc:mk(
                emqx_schema:duration_s(),
                #{
                    required => false,
                    default => <<"30m">>,
                    desc => ?DESC(s3_aggregation_interval)
                }
            )},
        {max_records,
            hoconsc:mk(
                pos_integer(),
                #{
                    required => false,
                    default => <<"100000">>,
                    desc => ?DESC(s3_aggregation_max_records)
                }
            )}
    ];
fields(s3_upload_resource_opts) ->
    %% NOTE: Aggregated action should benefit from generous batching defaults.
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{default => ?DEFAULT_AGGREG_BATCH_SIZE}},
        {batch_time, #{default => ?DEFAULT_AGGREG_BATCH_TIME}}
    ]).

desc(s3) ->
    ?DESC(s3_upload);
desc(Name) when
    Name == s3_upload;
    Name == s3_direct_upload_parameters;
    Name == s3_aggregated_upload_parameters;
    Name == s3_aggregation;
    Name == s3_aggregated_container_csv
->
    ?DESC(Name);
desc(s3_upload_resource_opts) ->
    ?DESC(emqx_resource_schema, resource_opts);
desc(_Name) ->
    undefined.

validators(s3_upload_parameters) ->
    emqx_s3_schema:validators(s3_uploader).

convert_actions(Conf = #{}, Opts) ->
    maps:map(fun(_Name, ConfAction) -> convert_action(ConfAction, Opts) end, Conf);
convert_actions(undefined, _) ->
    undefined.

convert_action(Conf = #{<<"parameters">> := Params, <<"resource_opts">> := ResourceOpts}, _) ->
    case Params of
        #{<<"mode">> := <<"aggregated">>} ->
            Conf;
        #{} ->
            %% NOTE: Disable batching for direct uploads.
            NResourceOpts = ResourceOpts#{<<"batch_size">> => 1, <<"batch_time">> => 0},
            Conf#{<<"resource_opts">> := NResourceOpts}
    end.

validate_key_template(Conf) ->
    Template = emqx_template:parse(Conf),
    case validate_bindings(emqx_template:placeholders(Template)) of
        Bindings when is_list(Bindings) ->
            ok;
        {error, {disallowed_placeholders, Disallowed}} ->
            {error, emqx_utils:format("Template placeholders are disallowed: ~p", [Disallowed])}
    end.

validate_bindings(Bindings) ->
    Formats = ["rfc3339", "rfc3339utc", "unix"],
    AllowedBindings = lists:append([
        ["action", "node", "sequence"],
        ["datetime." ++ F || F <- Formats],
        ["datetime_until." ++ F || F <- Formats]
    ]),
    case Bindings -- AllowedBindings of
        [] ->
            Bindings;
        Disallowed ->
            {error, {disallowed_placeholders, Disallowed}}
    end.

%% Interpreting options

-spec mk_key_template(unicode:chardata()) ->
    emqx_template:str().
mk_key_template(Key) ->
    Template = emqx_template:parse(Key),
    UsedBindings = emqx_template:placeholders(Template),
    SuffixTemplate = mk_suffix_template(UsedBindings),
    case emqx_template:is_const(SuffixTemplate) of
        true ->
            Template;
        false ->
            Template ++ SuffixTemplate
    end.

mk_suffix_template(UsedBindings) ->
    RequiredBindings = ["action", "node", "datetime.", "sequence"],
    SuffixBindings = [
        mk_default_binding(RB)
     || RB <- RequiredBindings,
        lists:all(fun(UB) -> string:prefix(UB, RB) == nomatch end, UsedBindings)
    ],
    SuffixTemplate = [["/", B] || B <- SuffixBindings],
    emqx_template:parse(SuffixTemplate).

mk_default_binding("datetime.") ->
    "${datetime.rfc3339utc}";
mk_default_binding(Binding) ->
    "${" ++ Binding ++ "}".

-spec mk_upload_options(_Parameters :: map()) -> emqx_s3_client:upload_options().
mk_upload_options(Parameters) ->
    Headers = mk_upload_headers(Parameters),
    #{
        headers => Headers,
        acl => maps:get(acl, Parameters, undefined)
    }.

mk_upload_headers(Parameters = #{container := Container}) ->
    Headers = normalize_headers(maps:get(headers, Parameters, #{})),
    ContainerHeaders = mk_container_headers(Container),
    maps:merge(ContainerHeaders, Headers).

normalize_headers(Headers) ->
    maps:fold(
        fun(Header, Value, Acc) ->
            maps:put(string:lowercase(emqx_utils_conv:str(Header)), Value, Acc)
        end,
        #{},
        Headers
    ).

mk_container_headers(#{type := csv}) ->
    #{"content-type" => "text/csv"};
mk_container_headers(#{}) ->
    #{}.

%% Examples

bridge_v2_examples(Method) ->
    [
        #{
            <<"s3">> => #{
                summary => <<"S3 Direct Upload">>,
                value => s3_upload_action_example(Method, direct)
            },
            <<"s3_aggreg">> => #{
                summary => <<"S3 Aggregated Upload">>,
                value => s3_upload_action_example(Method, aggreg)
            }
        }
    ].

s3_upload_action_example(post, Mode) ->
    maps:merge(
        s3_upload_action_example(put, Mode),
        #{
            type => atom_to_binary(?ACTION_UPLOAD),
            name => <<"my_s3_action">>,
            enable => true,
            connector => <<"my_s3_connector">>
        }
    );
s3_upload_action_example(get, Mode) ->
    maps:merge(
        s3_upload_action_example(put, Mode),
        #{
            enable => true,
            connector => <<"my_s3_connector">>,
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        }
    );
s3_upload_action_example(put, direct) ->
    #{
        description => <<"My upload action">>,
        parameters => #{
            mode => <<"direct">>,
            bucket => <<"${clientid}">>,
            key => <<"${topic}">>,
            content => <<"${payload}">>,
            acl => <<"public_read">>
        },
        resource_opts => #{
            query_mode => <<"sync">>,
            inflight_window => 10
        }
    };
s3_upload_action_example(put, aggreg) ->
    #{
        description => <<"My aggregated upload action">>,
        parameters => #{
            mode => <<"aggregated">>,
            bucket => <<"mqtt-aggregated">>,
            key => <<"${action}/${node}/${datetime.rfc3339utc}_N${sequence}.csv">>,
            acl => <<"public_read">>,
            aggregation => #{
                time_interval => <<"15m">>,
                max_records => 100_000
            },
            <<"container">> => #{
                type => <<"csv">>,
                column_order => [<<"clientid">>, <<"topic">>, <<"publish_received_at">>]
            }
        },
        resource_opts => #{
            health_check_interval => <<"10s">>,
            query_mode => <<"async">>,
            inflight_window => 100
        }
    }.
