%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_dashboard_swagger).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(BASE_PATH, "/api/v5").

%% API
-export([spec/1, spec/2]).
-export([namespace/0, namespace/1, fields/1]).
-export([schema_with_example/2, schema_with_examples/2]).
-export([error_codes/1, error_codes/2]).
-export([file_schema/1]).
-export([base_path/0]).
-export([relative_uri/1, get_relative_uri/1]).
-export([compose_filters/2]).
-export([validate_content_type_json/2, validate_content_type/3]).

-export([
    filter_check_request/2,
    filter_check_request_and_translate_body/2,
    gen_api_schema_json_iodata/3
]).

-ifdef(TEST).
-export([
    parse_spec_ref/3,
    components/2
]).
-endif.

-define(METHODS, [get, post, put, head, delete, patch, options, trace]).

-define(DEFAULT_FIELDS, [
    example,
    allowReserved,
    style,
    format,
    readOnly,
    explode,
    maxLength,
    allowEmptyValue,
    deprecated,
    minimum,
    maximum,
    %% is_template is a type property,
    %% but some exceptions are made for them to be field property
    %% for example, HTTP headers (which is a map type)
    is_template
]).

-define(TO_REF(_N_, _F_), iolist_to_binary([to_bin(_N_), ".", to_bin(_F_)])).
-define(TO_COMPONENTS_SCHEMA(_M_, _F_),
    iolist_to_binary([
        <<"#/components/schemas/">>,
        ?TO_REF(namespace(_M_), _F_)
    ])
).
-define(TO_COMPONENTS_PARAM(_M_, _F_),
    iolist_to_binary([
        <<"#/components/parameters/">>,
        ?TO_REF(namespace(_M_), _F_)
    ])
).

-define(NO_I18N, undefined).

-define(MAX_ROW_LIMIT, 10000).
-define(DEFAULT_ROW, 100).

-type request() :: #{bindings => map(), query_string => map(), body => map()}.
-type request_meta() :: #{
    module := module(),
    path := string(),
    method := atom(),
    %% API Operation specification override.
    %% Takes precedence over the API specification defined in the module.
    apispec => map()
}.

%% More exact types are defined in minirest.hrl, but we don't want to include it
%% because it defines a lot of types and they may clash with the types declared locally.
-type status_code() :: pos_integer().
-type error_code() :: atom() | binary().
-type error_message() :: binary().
-type response_body() :: term().
-type headers() :: map().

-type response() ::
    status_code()
    | {status_code()}
    | {status_code(), response_body()}
    | {status_code(), headers(), response_body()}
    | {status_code(), error_code(), error_message()}.

-type filter_result() :: {ok, request()} | response().
-type filter() :: emqx_maybe:t(fun((request(), request_meta()) -> filter_result())).

-type spec_opts() :: #{
    check_schema => boolean() | filter(),
    translate_body => boolean(),
    schema_converter => fun((hocon_schema:schema(), Module :: atom()) -> map()),
    i18n_lang => atom() | string() | binary(),
    filter => filter()
}.

-type route_path() :: string() | binary().
-type route_methods() :: map().
-type route_handler() :: atom().
-type route_options() :: #{filter => filter()}.

-type api_spec_entry() :: {route_path(), route_methods(), route_handler(), route_options()}.
-type api_spec_component() :: map().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @equiv spec(Module, #{check_schema => false})
-spec spec(module()) -> {list(api_spec_entry()), list(api_spec_component())}.
spec(Module) -> spec(Module, #{check_schema => false}).

-spec spec(module(), spec_opts()) -> {list(api_spec_entry()), list(api_spec_component())}.
spec(Module, Options) ->
    Paths = apply(Module, paths, []),
    {ApiSpec, AllRefs} =
        lists:foldl(
            fun(Path, {AllAcc, AllRefsAcc}) ->
                {OperationId, Specs, Refs, RouteOpts} = parse_spec_ref(Module, Path, Options),
                {
                    [{filename:join("/", Path), Specs, OperationId, RouteOpts} | AllAcc],
                    Refs ++ AllRefsAcc
                }
            end,
            {[], []},
            Paths
        ),
    {ApiSpec, components(lists:usort(AllRefs), Options)}.

validate_content_type_json(Params, Meta) ->
    validate_content_type(Params, Meta, <<"application/json">>).

%% tip: Skip content-type check if body is empty.
validate_content_type(
    #{body := Body, headers := Headers} = Params,
    #{method := Method},
    Expect
) when
    (Method =:= put orelse
        Method =:= post orelse
        method =:= patch) andalso
        Body =/= #{}
->
    ExpectSize = byte_size(Expect),
    case maps:get(<<"content-type">>, Headers, undefined) of
        <<Expect:ExpectSize/binary, _/binary>> ->
            {ok, Params};
        _ ->
            {415, 'UNSUPPORTED_MEDIA_TYPE', <<"content-type:", Expect/binary, " Required">>}
    end;
validate_content_type(Params, _Meta, _Expect) ->
    {ok, Params}.

-spec namespace() -> hocon_schema:name().
namespace() -> "public".

-spec fields(hocon_schema:name()) -> hocon_schema:fields().
fields(page) ->
    Desc = <<"Page number of the results to fetch.">>,
    Meta = #{in => query, desc => Desc, default => 1, example => 1},
    [{page, hoconsc:mk(pos_integer(), Meta)}];
fields(limit) ->
    Desc = iolist_to_binary([
        <<"Results per page(max ">>,
        integer_to_binary(?MAX_ROW_LIMIT),
        <<")">>
    ]),
    Meta = #{in => query, desc => Desc, default => ?DEFAULT_ROW, example => 50},
    [{limit, hoconsc:mk(range(1, ?MAX_ROW_LIMIT), Meta)}];
fields(cursor) ->
    Desc = <<"Opaque value representing the current iteration state.">>,
    Meta = #{required => false, in => query, desc => Desc},
    [{cursor, hoconsc:mk(binary(), Meta)}];
fields(cursor_response) ->
    Desc = <<"Opaque value representing the current iteration state.">>,
    Meta = #{desc => Desc, required => false},
    [{cursor, hoconsc:mk(binary(), Meta)}];
fields(count) ->
    Desc = <<
        "Total number of records matching the query.<br/>"
        "Note: this field is present only if the query can be optimized and does "
        "not require a full table scan."
    >>,
    Meta = #{desc => Desc, required => false},
    [{count, hoconsc:mk(non_neg_integer(), Meta)}];
fields(hasnext) ->
    Desc = <<
        "Flag indicating whether there are more results available on next pages."
    >>,
    Meta = #{desc => Desc, required => true},
    [{hasnext, hoconsc:mk(boolean(), Meta)}];
fields(position) ->
    Desc = <<
        "An opaque token that can then be in subsequent requests to get "
        " the next chunk of results: \"?position={prev_response.meta.position}\"<br/>"
        "It is used instead of \"page\" parameter to traverse highly volatile data.<br/>"
        "Can be omitted or set to \"none\" to get the first chunk of data."
    >>,
    Meta = #{
        in => query, desc => Desc, required => false, example => <<"none">>
    },
    [{position, hoconsc:mk(hoconsc:union([none, end_of_data, binary()]), Meta)}];
fields(start) ->
    Desc = <<"The position of the current first element of the data collection.">>,
    Meta = #{
        desc => Desc, required => true, example => <<"none">>
    },
    [{start, hoconsc:mk(hoconsc:union([none, binary()]), Meta)}];
fields(meta) ->
    fields(page) ++ fields(limit) ++ fields(count) ++ fields(hasnext);
fields(meta_with_cursor) ->
    fields(count) ++ fields(hasnext) ++ fields(cursor_response);
fields(continuation_meta) ->
    fields(start) ++ fields(position).

-spec schema_with_example(hocon_schema:type(), term()) -> hocon_schema:field_schema().
schema_with_example(Type, Example) ->
    hoconsc:mk(Type, #{examples => #{<<"example">> => Example}}).

-spec schema_with_examples(hocon_schema:type(), map() | list(tuple())) ->
    hocon_schema:field_schema().
schema_with_examples(Type, Examples) ->
    hoconsc:mk(Type, #{examples => #{<<"examples">> => Examples}}).

-spec error_codes(list(atom())) -> hocon_schema:fields().
error_codes(Codes) ->
    error_codes(Codes, <<"Error code to troubleshoot problems.">>).

-spec error_codes(nonempty_list(atom()), binary() | {desc, module(), term()}) ->
    hocon_schema:fields().
error_codes(Codes = [_ | _], MsgDesc) ->
    [
        {code, hoconsc:mk(hoconsc:enum(Codes))},
        {message,
            hoconsc:mk(string(), #{
                desc => MsgDesc
            })}
    ].

-spec base_path() -> uri_string:uri_string().
base_path() ->
    ?BASE_PATH.

-spec relative_uri(uri_string:uri_string()) -> uri_string:uri_string().
relative_uri(Uri) ->
    base_path() ++ Uri.

-spec get_relative_uri(uri_string:uri_string()) -> {ok, uri_string:uri_string()} | error.
get_relative_uri(<<?BASE_PATH, Path/binary>>) ->
    {ok, Path};
get_relative_uri(_Path) ->
    error.

file_schema(FileName) ->
    #{
        content => #{
            'multipart/form-data' => #{
                schema => #{
                    type => object,
                    properties => #{
                        FileName => #{type => string, format => binary}
                    }
                }
            }
        }
    }.

gen_api_schema_json_iodata(SchemaMod, SchemaInfo, Converter) ->
    {ApiSpec0, Components0} = spec(
        SchemaMod,
        #{
            schema_converter => Converter,
            i18n_lang => ?NO_I18N
        }
    ),
    ApiSpec = lists:foldl(
        fun({Path, Spec, _, _}, Acc) ->
            NewSpec = maps:fold(
                fun(Method, #{responses := Responses}, SubAcc) ->
                    case Responses of
                        #{
                            <<"200">> :=
                                #{
                                    <<"content">> := #{
                                        <<"application/json">> := #{<<"schema">> := Schema}
                                    }
                                }
                        } ->
                            SubAcc#{Method => Schema};
                        _ ->
                            SubAcc
                    end
                end,
                #{},
                Spec
            ),
            Acc#{list_to_atom(Path) => NewSpec}
        end,
        #{},
        ApiSpec0
    ),
    Components = lists:foldl(fun(M, Acc) -> maps:merge(M, Acc) end, #{}, Components0),
    emqx_utils_json:encode(
        #{
            info => SchemaInfo,
            paths => ApiSpec,
            components => #{schemas => Components}
        },
        [pretty, force_utf8]
    ).

-spec compose_filters(filter(), filter()) -> filter().
compose_filters(undefined, Filter2) ->
    Filter2;
compose_filters(Filter1, undefined) ->
    Filter1;
compose_filters(Filter1, Filter2) ->
    [Filter1, Filter2].

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------

filter_check_request_and_translate_body(Request, RequestMeta) ->
    translate_req(Request, RequestMeta, fun check_and_translate/3).

filter_check_request(Request, RequestMeta) ->
    translate_req(Request, RequestMeta, fun check_only/3).

translate_req(Request, ReqMeta = #{module := Module}, CheckFun) ->
    Spec = find_req_apispec(ReqMeta),
    try
        Params = maps:get(parameters, Spec, []),
        Body = maps:get('requestBody', Spec, []),
        {Bindings, QueryStr} = check_parameters(Request, Params, Module),
        case check_request_body(Request, Body, Module, CheckFun, hoconsc:is_schema(Body)) of
            {ok, NewBody} ->
                {ok, Request#{bindings => Bindings, query_string => QueryStr, body => NewBody}};
            Error ->
                Error
        end
    catch
        throw:HoconError ->
            Msg = hocon_error_msg(HoconError),
            {400, 'BAD_REQUEST', Msg}
    end.

find_req_apispec(#{apispec := Spec}) ->
    Spec;
find_req_apispec(#{module := Module, path := Path, method := Method}) ->
    #{Method := Spec} = apply(Module, schema, [Path]),
    Spec.

check_and_translate(Schema, Map, Opts) ->
    hocon_tconf:check_plain(Schema, Map, Opts).

check_only(Schema, Map, Opts) ->
    _ = hocon_tconf:check_plain(Schema, Map, Opts),
    Map.

filter(Options) ->
    CheckSchemaFilter = check_schema_filter(Options),
    CustomFilter = custom_filter(Options),
    compose_filters(CheckSchemaFilter, CustomFilter).

custom_filter(Options) ->
    maps:get(filter, Options, undefined).

check_schema_filter(#{check_schema := true, translate_body := true}) ->
    fun ?MODULE:filter_check_request_and_translate_body/2;
check_schema_filter(#{check_schema := true}) ->
    fun ?MODULE:filter_check_request/2;
check_schema_filter(#{check_schema := Filter}) when is_function(Filter, 2) ->
    Filter;
check_schema_filter(_) ->
    undefined.

parse_spec_ref(Module, Path, Options) ->
    Schema =
        try
            erlang:apply(Module, schema, [Path])
        catch
            Error:Reason:Stacktrace ->
                failed_to_generate_swagger_spec(Module, Path, Error, Reason, Stacktrace)
        end,
    OperationId = maps:get('operationId', Schema),
    {Specs, Refs} = maps:fold(
        fun(Method, Meta, {Acc, RefsAcc}) ->
            (not lists:member(Method, ?METHODS)) andalso
                throw({error, #{module => Module, path => Path, method => Method}}),
            {Spec, SubRefs} = meta_to_spec(Meta, Module, Options),
            {Acc#{Method => Spec}, SubRefs ++ RefsAcc}
        end,
        {#{}, []},
        maps:without(['operationId', 'filter'], Schema)
    ),
    RouteOpts = generate_route_opts(Schema, Options),
    {OperationId, Specs, Refs, RouteOpts}.

-ifdef(TEST).
-spec failed_to_generate_swagger_spec(_, _, _, _, _) -> no_return().
failed_to_generate_swagger_spec(Module, Path, Error, Reason, Stacktrace) ->
    error({failed_to_generate_swagger_spec, Module, Path, Error, Reason, Stacktrace}).
-else.
-spec failed_to_generate_swagger_spec(_, _, _, _, _) -> no_return().
failed_to_generate_swagger_spec(Module, Path, Error, Reason, Stacktrace) ->
    %% This error is intended to fail the build
    %% hence print to standard_error
    io:format(
        standard_error,
        "Failed to generate swagger for path ~p in module ~p~n"
        "error:~p~nreason:~p~n~p~n",
        [Path, Module, Error, Reason, Stacktrace]
    ),
    error({failed_to_generate_swagger_spec, Module, Path}).

-endif.
generate_route_opts(Schema, Options) ->
    #{filter => compose_filters(filter(Options), custom_filter(Schema))}.

check_parameters(Request, Spec, Module) ->
    #{bindings := Bindings, query_string := QueryStr} = Request,
    BindingsBin = maps:fold(
        fun(Key, Value, Acc) ->
            Acc#{atom_to_binary(Key) => Value}
        end,
        #{},
        Bindings
    ),
    check_parameter(Spec, BindingsBin, QueryStr, Module, #{}, #{}).

check_parameter([?REF(Fields) | Spec], Bindings, QueryStr, LocalMod, BindingsAcc, QueryStrAcc) ->
    check_parameter(
        [?R_REF(LocalMod, Fields) | Spec],
        Bindings,
        QueryStr,
        LocalMod,
        BindingsAcc,
        QueryStrAcc
    );
check_parameter(
    [?R_REF(Module, Fields) | Spec],
    Bindings,
    QueryStr,
    LocalMod,
    BindingsAcc,
    QueryStrAcc
) ->
    Params = apply(Module, fields, [Fields]),
    check_parameter(Params ++ Spec, Bindings, QueryStr, LocalMod, BindingsAcc, QueryStrAcc);
check_parameter([], _Bindings, _QueryStr, _Module, NewBindings, NewQueryStr) ->
    {NewBindings, NewQueryStr};
check_parameter([{Name, Type} | Spec], Bindings, QueryStr, Module, BindingsAcc, QueryStrAcc) ->
    case hocon_schema:field_schema(Type, in) of
        path ->
            Schema = #{roots => [{Name, Type}], fields => #{}},
            Option = #{atom_key => true},
            NewBindings = hocon_tconf:check_plain(Schema, Bindings, Option),
            NewBindingsAcc = maps:merge(BindingsAcc, NewBindings),
            check_parameter(Spec, Bindings, QueryStr, Module, NewBindingsAcc, QueryStrAcc);
        query ->
            Type1 = maybe_wrap_array_qs_param(Type),
            Schema = #{roots => [{Name, Type1}], fields => #{}},
            Option = #{},
            NewQueryStr = hocon_tconf:check_plain(Schema, QueryStr, Option),
            NewQueryStrAcc = maps:merge(QueryStrAcc, NewQueryStr),
            check_parameter(Spec, Bindings, QueryStr, Module, BindingsAcc, NewQueryStrAcc)
    end.

%% Compatibility layer for minirest 1.4.0 that parses repetitive QS params into lists.
%% Previous minirest releases dropped all but the last repetitive params.

maybe_wrap_array_qs_param(FieldSchema) ->
    Conv = hocon_schema:field_schema(FieldSchema, converter),
    Type = hocon_schema:field_schema(FieldSchema, type),
    case array_or_single_qs_param(Type, Conv) of
        any ->
            FieldSchema;
        array ->
            override_conv(FieldSchema, fun wrap_array_conv/2, Conv);
        single ->
            override_conv(FieldSchema, fun unwrap_array_conv/2, Conv)
    end.

array_or_single_qs_param(?ARRAY(_Type), undefined) ->
    array;
%% Qs field schema is an array and defines a converter:
%% don't change (wrap/unwrap) the original value, and let the converter handle it.
%% For example, it can be a CSV list.
array_or_single_qs_param(?ARRAY(_Type), _Conv) ->
    any;
array_or_single_qs_param(?UNION(Types), _Conv) ->
    HasArray = lists:any(
        fun
            (?ARRAY(_)) -> true;
            (_) -> false
        end,
        Types
    ),
    case HasArray of
        true -> any;
        false -> single
    end;
array_or_single_qs_param(_, _Conv) ->
    single.

override_conv(FieldSchema, NewConv, OldConv) ->
    Conv = compose_converters(NewConv, OldConv),
    hocon_schema:override(FieldSchema, FieldSchema#{converter => Conv}).

compose_converters(NewFun, undefined = _OldFun) ->
    NewFun;
compose_converters(NewFun, OldFun) ->
    case erlang:fun_info(OldFun, arity) of
        {_, 2} ->
            fun(V, Opts) -> OldFun(NewFun(V, Opts), Opts) end;
        {_, 1} ->
            fun(V, Opts) -> OldFun(NewFun(V, Opts)) end
    end.

wrap_array_conv(Val, _Opts) when is_list(Val); Val =:= undefined -> Val;
wrap_array_conv(SingleVal, _Opts) -> [SingleVal].

unwrap_array_conv([HVal | _], _Opts) -> HVal;
unwrap_array_conv(SingleVal, _Opts) -> SingleVal.

check_request_body(#{body := Body}, Schema, Module, CheckFun, true) ->
    %% the body was already being decoded
    %% if the content-type header specified application/json.
    case is_binary(Body) of
        false ->
            Type0 = hocon_schema:field_schema(Schema, type),
            Type =
                case Type0 of
                    ?REF(StructName) -> ?R_REF(Module, StructName);
                    _ -> Type0
                end,
            Validations =
                case hocon_schema:field_schema(Schema, validator) of
                    undefined ->
                        [];
                    Fun when is_function(Fun) ->
                        [{validator, fun(#{<<"root">> := B}) -> Fun(B) end}]
                end,
            NewSchema = #{roots => [{root, Type}], fields => #{}, validations => Validations},
            Option = #{required => false},
            #{<<"root">> := NewBody} = CheckFun(NewSchema, #{<<"root">> => Body}, Option),
            {ok, NewBody};
        true ->
            {415, 'UNSUPPORTED_MEDIA_TYPE', <<"content-type:application/json Required">>}
    end;
%% TODO not support nest object check yet, please use ref!
%% 'requestBody' = [ {per_page, mk(integer(), #{}},
%%                 {nest_object, [
%%                   {good_nest_1, mk(integer(), #{})},
%%                   {good_nest_2, mk(ref(?MODULE, good_ref), #{})}
%%                ]}
%% ]
check_request_body(#{body := Body}, Spec, _Module, CheckFun, false) when is_list(Spec) ->
    {ok,
        lists:foldl(
            fun({Name, Type}, Acc) ->
                Schema = #{roots => [{Name, Type}], fields => #{}},
                maps:merge(Acc, CheckFun(Schema, Body, #{}))
            end,
            #{},
            Spec
        )};
%% requestBody => #{content => #{ 'application/octet-stream' =>
%% #{schema => #{ type => string, format => binary}}}
check_request_body(#{body := Body}, Spec, _Module, _CheckFun, false) when is_map(Spec) ->
    {ok, Body}.

%% tags, description, summary, security, deprecated
meta_to_spec(Meta, Module, Options) ->
    {Params, Refs1} = parameters(maps:get(parameters, Meta, []), Module, Options),
    {RequestBody, Refs2} = request_body(maps:get('requestBody', Meta, []), Module, Options),
    {Responses, Refs3} = responses(maps:get(responses, Meta, #{}), Module, Options),
    {
        generate_method_desc(to_spec(Meta, Params, RequestBody, Responses), Options),
        lists:usort(Refs1 ++ Refs2 ++ Refs3)
    }.

to_spec(Meta, Params, [], Responses) ->
    Spec = maps:without([parameters, 'requestBody', responses], Meta),
    Spec#{parameters => Params, responses => Responses};
to_spec(Meta, Params, RequestBody, Responses) ->
    Spec = to_spec(Meta, Params, [], Responses),
    maps:put('requestBody', RequestBody, Spec).

generate_method_desc(Spec = #{desc := _Desc}, Options) ->
    Spec1 = trans_description(maps:remove(desc, Spec), Spec, Options),
    trans_tags(Spec1);
generate_method_desc(Spec = #{description := _Desc}, Options) ->
    Spec1 = trans_description(Spec, Spec, Options),
    trans_tags(Spec1);
generate_method_desc(Spec, _Options) ->
    trans_tags(Spec).

trans_tags(Spec = #{tags := Tags}) ->
    Spec#{tags => [string:titlecase(to_bin(Tag)) || Tag <- Tags]};
trans_tags(Spec) ->
    Spec.

parameters(Params, Module, Options) ->
    {SpecList, AllRefs} =
        lists:foldl(
            fun(Param, {Acc, RefsAcc}) ->
                case Param of
                    ?REF(StructName) ->
                        to_ref(Module, StructName, Acc, RefsAcc);
                    ?R_REF(RModule, StructName) ->
                        to_ref(RModule, StructName, Acc, RefsAcc);
                    {Name, Type} ->
                        In = hocon_schema:field_schema(Type, in),
                        In =:= undefined andalso
                            throw({error, <<"missing in:path/query field in parameters">>}),
                        Required = hocon_schema:field_schema(Type, required),
                        Default = hocon_schema:field_schema(Type, default),
                        HoconType = hocon_schema:field_schema(Type, type),
                        SchemaExtras = hocon_extract_map([enum, default], Type),
                        Meta = init_meta(Default),
                        {ParamType, Refs} = hocon_schema_to_spec(HoconType, Module),
                        Schema = maps:merge(maps:merge(ParamType, Meta), SchemaExtras),
                        Spec0 = init_prop(
                            [required | ?DEFAULT_FIELDS],
                            #{schema => Schema, name => Name, in => In},
                            Type
                        ),
                        Spec1 = trans_required(Spec0, Required, In),
                        Spec2 = trans_description(Spec1, Type, Options),
                        {[Spec2 | Acc], Refs ++ RefsAcc}
                end
            end,
            {[], []},
            Params
        ),
    {lists:reverse(SpecList), AllRefs}.

hocon_extract_map(Keys, Type) ->
    lists:foldl(
        fun(K, M) ->
            case hocon_schema:field_schema(Type, K) of
                undefined -> M;
                V -> M#{K => V}
            end
        end,
        #{},
        Keys
    ).

init_meta(undefined) -> #{};
init_meta(Default) -> #{default => Default}.

init_prop(Keys, Init, Type) ->
    lists:foldl(
        fun(Key, Acc) ->
            case hocon_schema:field_schema(Type, Key) of
                undefined -> Acc;
                Schema -> Acc#{Key => format_prop(Key, Schema)}
            end
        end,
        Init,
        Keys
    ).

format_prop(deprecated, Value) when is_boolean(Value) -> Value;
format_prop(deprecated, _) -> true;
format_prop(default, []) -> [];
format_prop(_, Schema) -> to_bin(Schema).

trans_required(Spec, true, _) -> Spec#{required => true};
trans_required(Spec, _, path) -> Spec#{required => true};
trans_required(Spec, _, _) -> Spec.

trans_description(Spec, Hocon, Options) ->
    Desc =
        case desc_struct(Hocon) of
            undefined -> undefined;
            ?DESC(_, _) = Struct -> get_i18n(<<"desc">>, Struct, undefined, Options);
            Text -> to_bin(Text)
        end,
    case Desc =:= undefined of
        true ->
            Spec;
        false ->
            Desc1 = binary:replace(Desc, [<<"\n">>], <<"<br/>">>, [global]),
            Spec#{description => Desc1}
    end.

get_i18n(Tag, ?DESC(Namespace, Id), Default, Options) ->
    Lang = get_lang(Options),
    case Lang of
        ?NO_I18N ->
            undefined;
        _ ->
            get_i18n_text(Lang, Namespace, Id, Tag, Default)
    end.

get_i18n_text(Lang, Namespace, Id, Tag, Default) ->
    case emqx_dashboard_desc_cache:lookup(Lang, Namespace, Id, Tag) of
        undefined ->
            Default;
        Text ->
            Text
    end.

%% So far i18n_lang in options is only used at build time.
%% At runtime, it's still the global config which controls the language.
get_lang(#{i18n_lang := Lang}) -> Lang;
get_lang(_) -> emqx:get_config([dashboard, i18n_lang]).

desc_struct(Hocon) ->
    R =
        case hocon_schema:field_schema(Hocon, desc) of
            undefined ->
                case hocon_schema:field_schema(Hocon, description) of
                    undefined -> get_ref_desc(Hocon);
                    Struct1 -> Struct1
                end;
            Struct ->
                Struct
        end,
    ensure_bin(R).

ensure_bin(undefined) -> undefined;
ensure_bin(?DESC(_Namespace, _Id) = Desc) -> Desc;
ensure_bin(Text) -> to_bin(Text).

get_ref_desc(?R_REF(Mod, Name)) ->
    case erlang:function_exported(Mod, desc, 1) of
        true -> Mod:desc(Name);
        false -> undefined
    end;
get_ref_desc(_) ->
    undefined.

request_body(#{content := _} = Content, _Module, _Options) ->
    {Content, []};
request_body([], _Module, _Options) ->
    {[], []};
request_body(Schema, Module, Options) ->
    {{Props, Refs}, Examples} =
        case hoconsc:is_schema(Schema) of
            true ->
                HoconSchema = hocon_schema:field_schema(Schema, type),
                SchemaExamples = hocon_schema:field_schema(Schema, examples),
                {hocon_schema_to_spec(HoconSchema, Module), SchemaExamples};
            false ->
                {parse_object(Schema, Module, Options), undefined}
        end,
    {#{<<"content">> => content(Props, Examples)}, Refs}.

responses(Responses, Module, Options) ->
    {Spec, Refs, _, _} = maps:fold(fun response/3, {#{}, [], Module, Options}, Responses),
    {Spec, Refs}.

response(Status, ?DESC(_Mod, _Id) = Schema, {Acc, RefsAcc, Module, Options}) ->
    Desc = trans_description(#{}, #{desc => Schema}, Options),
    {Acc#{integer_to_binary(Status) => Desc}, RefsAcc, Module, Options};
response(Status, Bin, {Acc, RefsAcc, Module, Options}) when is_binary(Bin) ->
    {Acc#{integer_to_binary(Status) => #{description => Bin}}, RefsAcc, Module, Options};
%% Support swagger raw object(file download).
%% TODO: multi type response(i.e. Support both 'application/json' and 'plain/text')
response(Status, #{content := _} = Content, {Acc, RefsAcc, Module, Options}) ->
    {Acc#{integer_to_binary(Status) => Content}, RefsAcc, Module, Options};
response(Status, ?REF(StructName), {Acc, RefsAcc, Module, Options}) ->
    response(Status, ?R_REF(Module, StructName), {Acc, RefsAcc, Module, Options});
response(Status, ?R_REF(_Mod, _Name) = RRef, {Acc, RefsAcc, Module, Options}) ->
    SchemaToSpec = get_schema_converter(Options),
    {Spec, Refs} = SchemaToSpec(RRef, Module),
    Content = content(Spec),
    {
        Acc#{
            integer_to_binary(Status) =>
                #{<<"content">> => Content}
        },
        Refs ++ RefsAcc,
        Module,
        Options
    };
response(Status, Schema, {Acc, RefsAcc, Module, Options}) ->
    case hoconsc:is_schema(Schema) of
        true ->
            Hocon = hocon_schema:field_schema(Schema, type),
            Examples = hocon_schema:field_schema(Schema, examples),
            {Spec, Refs} = hocon_schema_to_spec(Hocon, Module),
            Init = trans_description(#{}, Schema, Options),
            Content = content(Spec, Examples),
            {
                Acc#{integer_to_binary(Status) => Init#{<<"content">> => Content}},
                Refs ++ RefsAcc,
                Module,
                Options
            };
        false ->
            {Props, Refs} = parse_object(Schema, Module, Options),
            Init = trans_description(#{}, Schema, Options),
            Content = Init#{<<"content">> => content(Props)},
            {Acc#{integer_to_binary(Status) => Content}, Refs ++ RefsAcc, Module, Options}
    end.

components(Refs, Options) ->
    lists:sort(
        maps:fold(
            fun(K, V, Acc) -> [#{K => V} | Acc] end,
            [],
            components(Options, Refs, #{}, [])
        )
    ).

components(_Options, [], SpecAcc, []) ->
    SpecAcc;
components(Options, [], SpecAcc, SubRefAcc) ->
    components(Options, SubRefAcc, SpecAcc, []);
components(Options, [{Module, Field} | Refs], SpecAcc, SubRefsAcc) ->
    Props = hocon_schema_fields(Module, Field),
    Namespace = namespace(Module),
    {Object, SubRefs} = parse_object(Props, Module, Options),
    NewSpecAcc = SpecAcc#{?TO_REF(Namespace, Field) => Object},
    components(Options, Refs, NewSpecAcc, SubRefs ++ SubRefsAcc);
%% parameters in ref only have one value, not array
components(Options, [{Module, Field, parameter} | Refs], SpecAcc, SubRefsAcc) ->
    Props = hocon_schema_fields(Module, Field),
    {[Param], SubRefs} = parameters(Props, Module, Options),
    Namespace = namespace(Module),
    NewSpecAcc = SpecAcc#{?TO_REF(Namespace, Field) => Param},
    components(Options, Refs, NewSpecAcc, SubRefs ++ SubRefsAcc).

hocon_schema_fields(Module, StructName) ->
    case apply(Module, fields, [StructName]) of
        #{fields := Fields, desc := _} ->
            %% evil here, as it's match hocon_schema's internal representation

            %% TODO: make use of desc ?
            Fields;
        Other ->
            Other
    end.

%% Semantic error at components.schemas.xxx:xx:xx
%% Component names can only contain the characters A-Z a-z 0-9 - . _
%% So replace ':' by '-'.
namespace(Module) ->
    case hocon_schema:namespace(Module) of
        undefined -> Module;
        NameSpace -> re:replace(to_bin(NameSpace), ":", "-", [global])
    end.

hocon_schema_to_spec(?R_REF(Module, StructName), _LocalModule) ->
    {#{<<"$ref">> => ?TO_COMPONENTS_SCHEMA(Module, StructName)}, [{Module, StructName}]};
hocon_schema_to_spec(?REF(StructName), LocalModule) ->
    {#{<<"$ref">> => ?TO_COMPONENTS_SCHEMA(LocalModule, StructName)}, [{LocalModule, StructName}]};
hocon_schema_to_spec(Type, LocalModule) when ?IS_TYPEREFL(Type) ->
    {typename_to_spec(lists:flatten(typerefl:name(Type)), LocalModule), []};
hocon_schema_to_spec(?ARRAY(Item), LocalModule) ->
    {Schema, Refs} = hocon_schema_to_spec(Item, LocalModule),
    {#{type => array, items => Schema}, Refs};
hocon_schema_to_spec(?ENUM(Items), _LocalModule) ->
    {#{type => string, enum => Items}, []};
hocon_schema_to_spec(?MAP(Name, Type), LocalModule) ->
    {Schema, SubRefs} = hocon_schema_to_spec(Type, LocalModule),
    {
        #{
            <<"type">> => object,
            <<"properties">> => #{<<"$", (to_bin(Name))/binary>> => Schema}
        },
        SubRefs
    };
hocon_schema_to_spec(?UNION(Types, _DisplayName), LocalModule) ->
    {OneOf, Refs} = lists:foldl(
        fun(Type, {Acc, RefsAcc}) ->
            {Schema, SubRefs} = hocon_schema_to_spec(Type, LocalModule),
            {[Schema | Acc], SubRefs ++ RefsAcc}
        end,
        {[], []},
        hoconsc:union_members(Types)
    ),
    {#{<<"oneOf">> => OneOf}, Refs};
hocon_schema_to_spec(Atom, _LocalModule) when is_atom(Atom) ->
    {#{type => string, enum => [Atom]}, []}.

typename_to_spec(TypeStr, Module) ->
    emqx_conf_schema_types:readable_swagger(Module, TypeStr).

to_bin(List) when is_list(List) ->
    case io_lib:printable_list(List) of
        true -> unicode:characters_to_binary(List);
        false -> List
    end;
to_bin(Boolean) when is_boolean(Boolean) -> Boolean;
to_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
to_bin({Type, Args}) ->
    unicode:characters_to_binary(io_lib:format("~ts-~p", [Type, Args]));
to_bin(X) ->
    X.

parse_object(PropList = [_ | _], Module, Options) when is_list(PropList) ->
    {Props, Required, Refs} = parse_object_loop(PropList, Module, Options),
    Object = #{<<"type">> => object, <<"properties">> => fix_empty_props(Props)},
    case Required of
        [] -> {Object, Refs};
        _ -> {maps:put(required, Required, Object), Refs}
    end;
parse_object(Other, Module, Options) ->
    erlang:throw(
        {error, #{
            msg => <<"Object only supports non-empty fields list">>,
            args => Other,
            module => Module,
            options => Options
        }}
    ).

parse_object_loop(PropList0, Module, Options) ->
    PropList = filter_hidden_key(PropList0, Module),
    parse_object_loop(PropList, Module, Options, _Props = [], _Required = [], _Refs = []).

filter_hidden_key(PropList0, Module) ->
    {PropList1, _} = lists:foldr(
        fun({Key, Hocon} = Prop, {PropAcc, KeyAcc}) ->
            NewKeyAcc = assert_no_duplicated_key(Key, KeyAcc, Module),
            case hoconsc:is_schema(Hocon) andalso is_hidden(Hocon) of
                true -> {PropAcc, NewKeyAcc};
                false -> {[Prop | PropAcc], NewKeyAcc}
            end
        end,
        {[], []},
        PropList0
    ),
    PropList1.

assert_no_duplicated_key(Key, Keys, Module) ->
    KeyBin = emqx_utils_conv:bin(Key),
    case lists:member(KeyBin, Keys) of
        true -> throw({duplicated_key, #{module => Module, key => KeyBin, keys => Keys}});
        false -> [KeyBin | Keys]
    end.

parse_object_loop([], _Module, _Options, Props, Required, Refs) ->
    {lists:reverse(Props), lists:usort(Required), Refs};
parse_object_loop([{Name, Hocon} | Rest], Module, Options, Props, Required, Refs) ->
    NameBin = to_bin(Name),
    case hoconsc:is_schema(Hocon) of
        true ->
            HoconType = hocon_schema:field_schema(Hocon, type),
            Init0 = init_prop([default | ?DEFAULT_FIELDS], #{}, Hocon),
            SchemaToSpec = get_schema_converter(Options),
            Init = maps:remove(
                summary,
                trans_description(Init0, Hocon, Options)
            ),
            {Prop, Refs1} = SchemaToSpec(HoconType, Module),
            NewRequiredAcc =
                case is_required(Hocon) of
                    true -> [NameBin | Required];
                    false -> Required
                end,
            parse_object_loop(
                Rest,
                Module,
                Options,
                [{NameBin, maps:merge(Prop, Init)} | Props],
                NewRequiredAcc,
                Refs1 ++ Refs
            );
        false ->
            %% TODO: there is only a handful of such
            %% refactor the schema to unify the two cases
            {SubObject, SubRefs} = parse_object(Hocon, Module, Options),
            parse_object_loop(
                Rest, Module, Options, [{NameBin, SubObject} | Props], Required, SubRefs ++ Refs
            )
    end.

%% return true if the field has 'importance' set to 'hidden'
is_hidden(Hocon) ->
    hocon_schema:is_hidden(Hocon, #{include_importance_up_from => ?IMPORTANCE_NO_DOC}).

is_required(Hocon) ->
    hocon_schema:field_schema(Hocon, required) =:= true.

fix_empty_props([]) ->
    #{};
fix_empty_props(Props) ->
    Props.

content(ApiSpec) ->
    content(ApiSpec, undefined).

content(ApiSpec, undefined) ->
    #{<<"application/json">> => #{<<"schema">> => ApiSpec}};
content(ApiSpec, Examples) when is_map(Examples) ->
    #{<<"application/json">> => Examples#{<<"schema">> => ApiSpec}}.

to_ref(Mod, StructName, Acc, RefsAcc) ->
    Ref = #{<<"$ref">> => ?TO_COMPONENTS_PARAM(Mod, StructName)},
    {[Ref | Acc], [{Mod, StructName, parameter} | RefsAcc]}.

get_schema_converter(Options) ->
    maps:get(schema_converter, Options, fun hocon_schema_to_spec/2).

hocon_error_msg(Reason) ->
    emqx_utils:readable_error_msg(Reason).
