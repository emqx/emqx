%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_influxdb).

-behaviour(emqx_connector_examples).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    write_syntax_type/0
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% Examples
-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

-type write_syntax() :: list().
-reflect_type([write_syntax/0]).
-typerefl_from_string({write_syntax/0, ?MODULE, to_influx_lines}).
-export([to_influx_lines/1]).

-define(CONNECTOR_TYPE, influxdb).
-define(ACTION_TYPE, influxdb).

%% -------------------------------------------------------------------------------------------------
%% api

write_syntax_type() ->
    typerefl:alias("template", write_syntax()).

%% Examples
conn_bridge_examples(Method) ->
    [
        #{
            <<"influxdb_api_v1">> => #{
                summary => <<"InfluxDB HTTP API V1 Bridge">>,
                value => values("influxdb_api_v1", Method)
            }
        },
        #{
            <<"influxdb_api_v2">> => #{
                summary => <<"InfluxDB HTTP API V2 Bridge">>,
                value => values("influxdb_api_v2", Method)
            }
        }
    ].

bridge_v2_examples(Method) ->
    WriteExample =
        <<"${topic},clientid=${clientid} ", "payload=${payload},",
            "${clientid}_int_value=${payload.int_key}i,", "bool=${payload.bool}">>,
    ParamsExample = #{
        parameters => #{
            write_syntax => WriteExample, precision => ms
        }
    },
    [
        #{
            <<"influxdb">> => #{
                summary => <<"InfluxDB Action">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method, influxdb, influxdb, ParamsExample
                )
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"influxdb_api_v1">> => #{
                summary => <<"InfluxDB HTTP API V1 Connector">>,
                value => emqx_connector_schema:connector_values(
                    Method, influxdb, connector_values(influxdb_api_v1)
                )
            }
        },
        #{
            <<"influxdb_api_v2">> => #{
                summary => <<"InfluxDB HTTP API V2 Connector">>,
                value => emqx_connector_schema:connector_values(
                    Method, influxdb, connector_values(influxdb_api_v2)
                )
            }
        }
    ].

connector_values(Type) ->
    maps:merge(basic_connector_values(), #{parameters => connector_values_v(Type)}).

connector_values_v(influxdb_api_v2) ->
    #{
        influxdb_type => influxdb_api_v2,
        bucket => <<"example_bucket">>,
        org => <<"examlpe_org">>,
        token => <<"example_token">>
    };
connector_values_v(influxdb_api_v1) ->
    #{
        influxdb_type => influxdb_api_v1,
        database => <<"example_database">>,
        username => <<"example_username">>,
        password => <<"******">>
    }.

basic_connector_values() ->
    #{
        enable => true,
        server => <<"127.0.0.1:8086">>,
        pool_size => 8,
        ssl => #{enable => false}
    }.

values(Protocol, get) ->
    values(Protocol, post);
values("influxdb_api_v2", post) ->
    SupportUint = <<"uint_value=${payload.uint_key}u,">>,
    TypeOpts = connector_values_v(influxdb_api_v2),
    values(common, "influxdb_api_v2", SupportUint, TypeOpts);
values("influxdb_api_v1", post) ->
    SupportUint = <<>>,
    TypeOpts = connector_values_v(influxdb_api_v1),
    values(common, "influxdb_api_v1", SupportUint, TypeOpts);
values(Protocol, put) ->
    values(Protocol, post).

values(common, Protocol, SupportUint, TypeOpts) ->
    CommonConfigs = #{
        type => list_to_atom(Protocol),
        name => <<"demo">>,
        enable => true,
        local_topic => <<"local/topic/#">>,
        write_syntax =>
            <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
                "${clientid}_int_value=${payload.int_key}i,", SupportUint/binary,
                "bool=${payload.bool}">>,
        precision => ms,
        resource_opts => #{
            batch_size => 100,
            batch_time => <<"20ms">>
        },
        server => <<"127.0.0.1:8086">>,
        ssl => #{enable => false}
    },
    maps:merge(TypeOpts, CommonConfigs).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_influxdb".

roots() -> [].

fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        emqx_bridge_influxdb_connector:fields("connector") ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields("post_api_v1") ->
    method_fields(post, influxdb_api_v1);
fields("post_api_v2") ->
    method_fields(post, influxdb_api_v2);
fields("put_api_v1") ->
    method_fields(put, influxdb_api_v1);
fields("put_api_v2") ->
    method_fields(put, influxdb_api_v2);
fields("get_api_v1") ->
    method_fields(get, influxdb_api_v1);
fields("get_api_v2") ->
    method_fields(get, influxdb_api_v2);
fields(action) ->
    {influxdb,
        mk(
            hoconsc:map(name, ref(?MODULE, influxdb_action)),
            #{desc => <<"InfluxDB Action Config">>, required => false}
        )};
fields(influxdb_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(ref(?MODULE, action_parameters), #{
            required => true, desc => ?DESC(action_parameters)
        })
    );
fields(action_parameters) ->
    [
        {write_syntax, fun write_syntax/1},
        emqx_bridge_influxdb_connector:precision_field()
    ];
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        emqx_bridge_influxdb_connector:fields("connector") ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(influxdb_action));
fields(Type) when
    Type == influxdb_api_v1 orelse Type == influxdb_api_v2
->
    influxdb_bridge_common_fields() ++
        connector_fields(Type).

method_fields(post, ConnectorType) ->
    influxdb_bridge_common_fields() ++
        connector_fields(ConnectorType) ++
        type_name_fields(ConnectorType);
method_fields(get, ConnectorType) ->
    influxdb_bridge_common_fields() ++
        connector_fields(ConnectorType) ++
        type_name_fields(ConnectorType) ++
        emqx_bridge_schema:status_fields();
method_fields(put, ConnectorType) ->
    influxdb_bridge_common_fields() ++
        connector_fields(ConnectorType).

influxdb_bridge_common_fields() ->
    emqx_bridge_schema:common_bridge_fields() ++
        [
            {local_topic, mk(binary(), #{desc => ?DESC("local_topic")})},
            {write_syntax, fun write_syntax/1}
        ] ++
        emqx_resource_schema:fields("resource_opts").

connector_fields(Type) ->
    emqx_bridge_influxdb_connector:fields(Type).

type_name_fields(Type) ->
    [
        {type, mk(Type, #{required => true, desc => ?DESC("desc_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}
    ].

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for InfluxDB using `", string:to_upper(Method), "` method."];
desc(influxdb_api_v1) ->
    ?DESC(emqx_bridge_influxdb_connector, "influxdb_api_v1");
desc(influxdb_api_v2) ->
    ?DESC(emqx_bridge_influxdb_connector, "influxdb_api_v2");
desc(influxdb_action) ->
    ?DESC(influxdb_action);
desc(action_parameters) ->
    ?DESC(action_parameters);
desc("config_connector") ->
    ?DESC("desc_config");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.

write_syntax(type) ->
    write_syntax_type();
write_syntax(required) ->
    true;
write_syntax(validator) ->
    [?NOT_EMPTY("the value of the field 'write_syntax' cannot be empty")];
write_syntax(converter) ->
    fun to_influx_lines/1;
write_syntax(desc) ->
    ?DESC("write_syntax");
write_syntax(format) ->
    <<"sql">>;
write_syntax(_) ->
    undefined.

to_influx_lines(Lines = [#{} | _]) ->
    %% already parsed/converted (e.g.: bridge_probe, after hocon_tconf:check_plain)
    Lines;
to_influx_lines(RawLines) ->
    try
        influx_lines(str(RawLines), [])
    catch
        _:Reason:Stacktrace ->
            Msg = lists:flatten(
                io_lib:format("Unable to parse InfluxDB line protocol: ~p", [RawLines])
            ),
            ?SLOG(error, #{msg => Msg, error_reason => Reason, stacktrace => Stacktrace}),
            {error, Msg}
    end.

-define(MEASUREMENT_ESC_CHARS, [$,, $\s]).
-define(TAG_FIELD_KEY_ESC_CHARS, [$,, $=, $\s]).
-define(FIELD_VAL_ESC_CHARS, [$", $\\]).
% Common separator for both tags and fields
-define(SEP, $\s).
-define(MEASUREMENT_TAG_SEP, $,).
-define(KEY_SEP, $=).
-define(VAL_SEP, $,).
-define(NON_EMPTY, [_ | _]).

influx_lines([] = _RawLines, Acc) ->
    ?NON_EMPTY = lists:reverse(Acc);
influx_lines(RawLines, Acc) ->
    {Acc1, RawLines1} = influx_line(string:trim(RawLines, leading, "\s\n"), Acc),
    influx_lines(RawLines1, Acc1).

influx_line([], Acc) ->
    {Acc, []};
influx_line(Line, Acc) ->
    {?NON_EMPTY = Measurement, Line1} = measurement(Line),
    {Tags, Line2} = tags(Line1),
    {?NON_EMPTY = Fields, Line3} = influx_fields(Line2),
    {Timestamp, Line4} = timestamp(Line3),
    {
        [
            #{
                measurement => Measurement,
                tags => Tags,
                fields => Fields,
                timestamp => Timestamp
            }
            | Acc
        ],
        Line4
    }.

measurement(Line) ->
    unescape(?MEASUREMENT_ESC_CHARS, [?MEASUREMENT_TAG_SEP, ?SEP], Line, []).

tags([?MEASUREMENT_TAG_SEP | Line]) ->
    tags1(Line, []);
tags(Line) ->
    {[], Line}.

%% Empty line is invalid as fields are required after tags,
%% need to break recursion here and fail later on parsing fields
tags1([] = Line, Acc) ->
    {lists:reverse(Acc), Line};
%% Matching non empty Acc treats lines like "m, field=field_val" invalid
tags1([?SEP | _] = Line, ?NON_EMPTY = Acc) ->
    {lists:reverse(Acc), Line};
tags1(Line, Acc) ->
    {Tag, Line1} = tag(Line),
    tags1(Line1, [Tag | Acc]).

tag(Line) ->
    {?NON_EMPTY = Key, Line1} = key(Line),
    {?NON_EMPTY = Val, Line2} = tag_val(Line1),
    {{Key, Val}, Line2}.

tag_val(Line) ->
    {Val, Line1} = unescape(?TAG_FIELD_KEY_ESC_CHARS, [?VAL_SEP, ?SEP], Line, []),
    {Val, strip_l(Line1, ?VAL_SEP)}.

influx_fields([?SEP | Line]) ->
    fields1(string:trim(Line, leading, "\s"), []).

%% Timestamp is optional, so fields may be at the very end of the line
fields1([Ch | _] = Line, Acc) when Ch =:= ?SEP; Ch =:= $\n ->
    {lists:reverse(Acc), Line};
fields1([] = Line, Acc) ->
    {lists:reverse(Acc), Line};
fields1(Line, Acc) ->
    {Field, Line1} = field(Line),
    fields1(Line1, [Field | Acc]).

field(Line) ->
    {?NON_EMPTY = Key, Line1} = key(Line),
    {Val, Line2} = field_val(Line1),
    {{Key, Val}, Line2}.

field_val([$" | Line]) ->
    {Val, [$" | Line1]} = unescape(?FIELD_VAL_ESC_CHARS, [$"], Line, []),
    %% Quoted val can be empty
    {{quoted, Val}, strip_l(Line1, ?VAL_SEP)};
field_val(Line) ->
    %% Unquoted value should not be un-escaped according to InfluxDB protocol,
    %% as it can only hold float, integer, uinteger or boolean value.
    %% However, as templates are possible, un-escaping is applied here,
    %% which also helps to detect some invalid lines, e.g.: "m,tag=1 field= ${timestamp}"
    {Val, Line1} = unescape(?TAG_FIELD_KEY_ESC_CHARS, [?VAL_SEP, ?SEP, $\n], Line, []),
    {?NON_EMPTY = Val, strip_l(Line1, ?VAL_SEP)}.

timestamp([?SEP | Line]) ->
    Line1 = string:trim(Line, leading, "\s"),
    %% Similarly to unquoted field value, un-escape a timestamp to validate and handle
    %% potentially escaped characters in a template
    {T, Line2} = unescape(?TAG_FIELD_KEY_ESC_CHARS, [?SEP, $\n], Line1, []),
    {timestamp1(T), Line2};
timestamp(Line) ->
    {undefined, Line}.

timestamp1(?NON_EMPTY = Ts) -> Ts;
timestamp1(_Ts) -> undefined.

%% Common for both tag and field keys
key(Line) ->
    {Key, Line1} = unescape(?TAG_FIELD_KEY_ESC_CHARS, [?KEY_SEP], Line, []),
    {Key, strip_l(Line1, ?KEY_SEP)}.

%% Only strip a character between pairs, don't strip it(and let it fail)
%% if the char to be stripped is at the end, e.g.: m,tag=val, field=val
strip_l([Ch, Ch1 | Str], Ch) when Ch1 =/= ?SEP ->
    [Ch1 | Str];
strip_l(Str, _Ch) ->
    Str.

unescape(EscapeChars, SepChars, [$\\, Char | T], Acc) ->
    ShouldEscapeBackslash = lists:member($\\, EscapeChars),
    Acc1 =
        case lists:member(Char, EscapeChars) of
            true -> [Char | Acc];
            false when not ShouldEscapeBackslash -> [Char, $\\ | Acc]
        end,
    unescape(EscapeChars, SepChars, T, Acc1);
unescape(EscapeChars, SepChars, [Char | T] = L, Acc) ->
    IsEscapeChar = lists:member(Char, EscapeChars),
    case lists:member(Char, SepChars) of
        true -> {lists:reverse(Acc), L};
        false when not IsEscapeChar -> unescape(EscapeChars, SepChars, T, [Char | Acc])
    end;
unescape(_EscapeChars, _SepChars, [] = L, Acc) ->
    {lists:reverse(Acc), L}.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
