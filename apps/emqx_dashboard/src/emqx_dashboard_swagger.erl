-module(emqx_dashboard_swagger).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% API
-export([spec/1, spec/2]).
-export([namespace/0, fields/1]).
-export([schema_with_example/2, schema_with_examples/2]).
-export([error_codes/1, error_codes/2]).

-export([filter_check_request/2, filter_check_request_and_translate_body/2]).

-ifdef(TEST).
-export([ parse_spec_ref/2
        , components/1
        ]).
-endif.

-define(METHODS, [get, post, put, head, delete, patch, options, trace]).

-define(DEFAULT_FIELDS, [example, allowReserved, style,
    explode, maxLength, allowEmptyValue, deprecated, minimum, maximum]).

-define(INIT_SCHEMA, #{fields => #{}, translations => #{},
                       validations => [], namespace => undefined}).

-define(TO_REF(_N_, _F_), iolist_to_binary([to_bin(_N_), ".", to_bin(_F_)])).
-define(TO_COMPONENTS_SCHEMA(_M_, _F_), iolist_to_binary([<<"#/components/schemas/">>,
                                                         ?TO_REF(namespace(_M_), _F_)])).
-define(TO_COMPONENTS_PARAM(_M_, _F_), iolist_to_binary([<<"#/components/parameters/">>,
                                                         ?TO_REF(namespace(_M_), _F_)])).

-define(MAX_ROW_LIMIT, 1000).
-define(DEFAULT_ROW, 100).

-type(request() :: #{bindings => map(), query_string => map(), body => map()}).
-type(request_meta() :: #{module => module(), path => string(), method => atom()}).

-type(filter_result() :: {ok, request()} | {400, 'BAD_REQUEST', binary()}).
-type(filter() :: fun((request(), request_meta()) -> filter_result())).

-type(spec_opts() :: #{check_schema => boolean() | filter(), translate_body => boolean()}).

-type(route_path() :: string() | binary()).
-type(route_methods() :: map()).
-type(route_handler() :: atom()).
-type(route_options() :: #{filter => filter() | undefined}).

-type(api_spec_entry() :: {route_path(), route_methods(), route_handler(), route_options()}).
-type(api_spec_component() :: map()).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @equiv spec(Module, #{check_schema => false})
-spec(spec(module()) -> {list(api_spec_entry()), list(api_spec_component())}).
spec(Module) -> spec(Module, #{check_schema => false}).

-spec(spec(module(), spec_opts()) -> {list(api_spec_entry()), list(api_spec_component())}).
spec(Module, Options) ->
    Paths = apply(Module, paths, []),
    {ApiSpec, AllRefs} =
        lists:foldl(fun(Path, {AllAcc, AllRefsAcc}) ->
            {OperationId, Specs, Refs} = parse_spec_ref(Module, Path),
            CheckSchema = support_check_schema(Options),
            {[{Path, Specs, OperationId, CheckSchema} | AllAcc],
                    Refs ++ AllRefsAcc}
                    end, {[], []}, Paths),
    {ApiSpec, components(lists:usort(AllRefs))}.

-spec(namespace() -> hocon_schema:name()).
namespace() -> "public".

-spec(fields(hocon_schema:name()) -> hocon_schema:fields()).
fields(page) ->
    Desc = <<"Page number of the results to fetch.">>,
    Meta = #{in => query, desc => Desc, default => 1, example => 1},
    [{page, hoconsc:mk(integer(), Meta)}];
fields(limit) ->
    Desc = iolist_to_binary([<<"Results per page(max ">>,
        integer_to_binary(?MAX_ROW_LIMIT), <<")">>]),
    Meta = #{in => query, desc => Desc, default => ?DEFAULT_ROW, example => 50},
    [{limit, hoconsc:mk(range(1, ?MAX_ROW_LIMIT), Meta)}].

-spec(schema_with_example(hocon_schema:type(), term()) -> hocon_schema:field_schema_map()).
schema_with_example(Type, Example) ->
    hoconsc:mk(Type, #{examples => #{<<"example">> => Example}}).

-spec(schema_with_examples(hocon_schema:type(), map()) -> hocon_schema:field_schema_map()).
schema_with_examples(Type, Examples) ->
    hoconsc:mk(Type, #{examples => #{<<"examples">> => Examples}}).

-spec(error_codes(list(atom())) -> hocon_schema:fields()).
error_codes(Codes) ->
    error_codes(Codes, <<"Error code to troubleshoot problems.">>).

-spec(error_codes(nonempty_list(atom()), binary()) -> hocon_schema:fields()).
error_codes(Codes = [_ | _], MsgExample) ->
    [
        {code, hoconsc:mk(hoconsc:enum(Codes))},
        {message, hoconsc:mk(string(), #{
            desc => <<"Details description of the error.">>,
            example => MsgExample
        })}
    ].

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------

filter_check_request_and_translate_body(Request, RequestMeta) ->
    translate_req(Request, RequestMeta, fun check_and_translate/3).

filter_check_request(Request, RequestMeta) ->
    translate_req(Request, RequestMeta, fun check_only/3).

translate_req(Request, #{module := Module, path := Path, method := Method}, CheckFun) ->
    #{Method := Spec} = apply(Module, schema, [Path]),
    try
        Params = maps:get(parameters, Spec, []),
        Body = maps:get('requestBody', Spec, []),
        {Bindings, QueryStr} = check_parameters(Request, Params, Module),
        NewBody = check_request_body(Request, Body, Module, CheckFun, hoconsc:is_schema(Body)),
        {ok, Request#{bindings => Bindings, query_string => QueryStr, body => NewBody}}
    catch throw:{_, ValidErrors} ->
        Msg = [io_lib:format("~ts : ~p", [Key, Reason]) ||
            {validation_error, #{path := Key, reason := Reason}} <- ValidErrors],
        {400, 'BAD_REQUEST', iolist_to_binary(string:join(Msg, ","))}
    end.

check_and_translate(Schema, Map, Opts) ->
    hocon_schema:check_plain(Schema, Map, Opts).

check_only(Schema, Map, Opts) ->
    _ = hocon_schema:check_plain(Schema, Map, Opts),
    Map.

support_check_schema(#{check_schema := true, translate_body := true}) ->
    #{filter => fun ?MODULE:filter_check_request_and_translate_body/2};
support_check_schema(#{check_schema := true}) ->
    #{filter => fun ?MODULE:filter_check_request/2};
support_check_schema(#{check_schema := Filter}) when is_function(Filter, 2) ->
    #{filter => Filter};
support_check_schema(_) ->
    #{filter => undefined}.

parse_spec_ref(Module, Path) ->
    Schema =
        try
            erlang:apply(Module, schema, [Path])
        catch error: Reason -> %% better error message
            throw({error, #{mfa => {Module, schema, [Path]}, reason => Reason}})
        end,
    {Specs, Refs} = maps:fold(fun(Method, Meta, {Acc, RefsAcc}) ->
        (not lists:member(Method, ?METHODS))
            andalso throw({error, #{module => Module, path => Path, method => Method}}),
        {Spec, SubRefs} = meta_to_spec(Meta, Module),
        {Acc#{Method => Spec}, SubRefs ++ RefsAcc}
                              end, {#{}, []},
        maps:without(['operationId'], Schema)),
    {maps:get('operationId', Schema), Specs, Refs}.

check_parameters(Request, Spec, Module) ->
    #{bindings := Bindings, query_string := QueryStr} = Request,
    BindingsBin = maps:fold(fun(Key, Value, Acc) ->
        Acc#{atom_to_binary(Key) => Value}
                            end, #{}, Bindings),
    check_parameter(Spec, BindingsBin, QueryStr, Module, #{}, #{}).

check_parameter([?REF(Fields) | Spec], Bindings, QueryStr, LocalMod, BindingsAcc, QueryStrAcc) ->
    check_parameter([?R_REF(LocalMod, Fields) | Spec],
        Bindings, QueryStr, LocalMod, BindingsAcc, QueryStrAcc);
check_parameter([?R_REF(Module, Fields) | Spec],
    Bindings, QueryStr, LocalMod, BindingsAcc, QueryStrAcc) ->
    Params = apply(Module, fields, [Fields]),
    check_parameter(Params ++ Spec, Bindings, QueryStr, LocalMod, BindingsAcc, QueryStrAcc);
check_parameter([], _Bindings, _QueryStr, _Module, NewBindings, NewQueryStr) ->
    {NewBindings, NewQueryStr};
check_parameter([{Name, Type} | Spec], Bindings, QueryStr, Module, BindingsAcc, QueryStrAcc) ->
    Schema = ?INIT_SCHEMA#{roots => [{Name, Type}]},
    case hocon_schema:field_schema(Type, in) of
        path ->
            Option = #{atom_key => true, override_env => false},
            NewBindings = hocon_schema:check_plain(Schema, Bindings, Option),
            NewBindingsAcc = maps:merge(BindingsAcc, NewBindings),
            check_parameter(Spec, Bindings, QueryStr, Module, NewBindingsAcc, QueryStrAcc);
        query ->
            Option = #{override_env => false},
            NewQueryStr = hocon_schema:check_plain(Schema, QueryStr, Option),
            NewQueryStrAcc = maps:merge(QueryStrAcc, NewQueryStr),
            check_parameter(Spec, Bindings, QueryStr, Module,BindingsAcc, NewQueryStrAcc)
    end.

check_request_body(#{body := Body}, Schema, Module, CheckFun, true) ->
    Type0 = hocon_schema:field_schema(Schema, type),
    Type =
        case Type0 of
            ?REF(StructName) -> ?R_REF(Module, StructName);
            _ -> Type0
        end,
    NewSchema = ?INIT_SCHEMA#{roots => [{root, Type}]},
    Option = #{override_env => false, nullable => true},
    #{<<"root">> := NewBody} = CheckFun(NewSchema, #{<<"root">> => Body}, Option),
    NewBody;
%% TODO not support nest object check yet, please use ref!
%% 'requestBody' = [ {per_page, mk(integer(), #{}},
%%                 {nest_object, [
%%                   {good_nest_1, mk(integer(), #{})},
%%                   {good_nest_2, mk(ref(?MODULE, good_ref), #{})}
%%                ]}
%% ]
check_request_body(#{body := Body}, Spec, _Module, CheckFun, false) ->
    lists:foldl(fun({Name, Type}, Acc) ->
        Schema = ?INIT_SCHEMA#{roots => [{Name, Type}]},
        maps:merge(Acc, CheckFun(Schema, Body, #{override_env => false}))
                end, #{}, Spec).

%% tags, description, summary, security, deprecated
meta_to_spec(Meta, Module) ->
    {Params, Refs1} = parameters(maps:get(parameters, Meta, []), Module),
    {RequestBody, Refs2} = request_body(maps:get('requestBody', Meta, []), Module),
    {Responses, Refs3} = responses(maps:get(responses, Meta, #{}), Module),
    {
        to_spec(Meta, Params, RequestBody, Responses),
        lists:usort(Refs1 ++ Refs2 ++ Refs3)
    }.

to_spec(Meta, Params, [], Responses) ->
    Spec = maps:without([parameters, 'requestBody', responses], Meta),
    Spec#{parameters => Params, responses => Responses};
to_spec(Meta, Params, RequestBody, Responses) ->
    Spec = to_spec(Meta, Params, [], Responses),
    maps:put('requestBody', RequestBody, Spec).

parameters(Params, Module) ->
    {SpecList, AllRefs} =
        lists:foldl(fun(Param, {Acc, RefsAcc}) ->
            case Param of
                ?REF(StructName) -> to_ref(Module, StructName, Acc, RefsAcc);
                ?R_REF(RModule, StructName) -> to_ref(RModule, StructName, Acc, RefsAcc);
                {Name, Type} ->
                    In = hocon_schema:field_schema(Type, in),
                    In =:= undefined andalso
                        throw({error, <<"missing in:path/query field in parameters">>}),
                    Nullable = hocon_schema:field_schema(Type, nullable),
                    Default = hocon_schema:field_schema(Type, default),
                    HoconType = hocon_schema:field_schema(Type, type),
                    Meta = init_meta(Nullable, Default),
                    {ParamType, Refs} = hocon_schema_to_spec(HoconType, Module),
                    Spec0 = init_prop([required | ?DEFAULT_FIELDS],
                        #{schema => maps:merge(ParamType, Meta), name => Name, in => In}, Type),
                    Spec1 = trans_required(Spec0, Nullable, In),
                    Spec2 = trans_desc(Spec1, Type),
                    {[Spec2 | Acc], Refs ++ RefsAcc}
            end
                    end, {[], []}, Params),
    {lists:reverse(SpecList), AllRefs}.

init_meta(Nullable, Default) ->
    Init =
        case Nullable of
            true -> #{nullable => true};
            _ -> #{}
        end,
    case Default =:= undefined of
        true -> Init;
        false -> Init#{default => Default}
    end.

init_prop(Keys, Init, Type) ->
    lists:foldl(fun(Key, Acc) ->
        case hocon_schema:field_schema(Type, Key) of
            undefined -> Acc;
            Schema -> Acc#{Key => to_bin(Schema)}
        end
                end, Init, Keys).

trans_required(Spec, false, _) -> Spec#{required => true};
trans_required(Spec, _, path) -> Spec#{required => true};
trans_required(Spec, _, _) -> Spec.

trans_desc(Spec, Hocon) ->
    case hocon_schema:field_schema(Hocon, desc) of
        undefined -> Spec;
        Desc -> Spec#{description => to_bin(Desc)}
    end.

request_body([], _Module) -> {[], []};
request_body(Schema, Module) ->
    {{Props, Refs}, Examples} =
        case hoconsc:is_schema(Schema) of
            true ->
                HoconSchema = hocon_schema:field_schema(Schema, type),
                SchemaExamples = hocon_schema:field_schema(Schema, examples),
                {hocon_schema_to_spec(HoconSchema, Module), SchemaExamples};
            false -> {parse_object(Schema, Module), undefined}
        end,
    {#{<<"content">> => content(Props, Examples)},
        Refs}.

responses(Responses, Module) ->
    {Spec, Refs, _} = maps:fold(fun response/3, {#{}, [], Module}, Responses),
    {Spec, Refs}.

response(Status, Bin, {Acc, RefsAcc, Module}) when is_binary(Bin) ->
    {Acc#{integer_to_binary(Status) => #{description => Bin}}, RefsAcc, Module};
response(Status, ?REF(StructName), {Acc, RefsAcc, Module}) ->
    response(Status, ?R_REF(Module, StructName), {Acc, RefsAcc, Module});
response(Status, ?R_REF(_Mod, _Name) = RRef, {Acc, RefsAcc, Module}) ->
    {Spec, Refs} = hocon_schema_to_spec(RRef, Module),
    Content = content(Spec),
    {Acc#{integer_to_binary(Status) => #{<<"content">> => Content}}, Refs ++ RefsAcc, Module};
response(Status, Schema, {Acc, RefsAcc, Module}) ->
    case hoconsc:is_schema(Schema) of
        true ->
            Hocon = hocon_schema:field_schema(Schema, type),
            Examples = hocon_schema:field_schema(Schema, examples),
            {Spec, Refs} = hocon_schema_to_spec(Hocon, Module),
            Init = trans_desc(#{}, Schema),
            Content = content(Spec, Examples),
            {
                Acc#{integer_to_binary(Status) => Init#{<<"content">> => Content}},
                    Refs ++ RefsAcc, Module
            };
        false ->
            {Props, Refs} = parse_object(Schema, Module),
            Content = #{<<"content">> => content(Props)},
            {Acc#{integer_to_binary(Status) => Content}, Refs ++ RefsAcc, Module}
    end.

components(Refs) ->
    lists:sort(maps:fold(fun(K, V, Acc) -> [#{K => V} | Acc] end, [],
        components(Refs, #{}, []))).

components([], SpecAcc, []) -> SpecAcc;
components([], SpecAcc, SubRefAcc) -> components(SubRefAcc, SpecAcc, []);
components([{Module, Field} | Refs], SpecAcc, SubRefsAcc) ->
    Props = apply(Module, fields, [Field]),
    Namespace = namespace(Module),
    {Object, SubRefs} = parse_object(Props, Module),
    NewSpecAcc = SpecAcc#{?TO_REF(Namespace, Field) => Object},
    components(Refs, NewSpecAcc, SubRefs ++ SubRefsAcc);
%% parameters in ref only have one value, not array
components([{Module, Field, parameter} | Refs], SpecAcc, SubRefsAcc) ->
    Props = apply(Module, fields, [Field]),
    {[Param], SubRefs} = parameters(Props, Module),
    Namespace = namespace(Module),
    NewSpecAcc = SpecAcc#{?TO_REF(Namespace, Field) => Param},
    components(Refs, NewSpecAcc, SubRefs ++ SubRefsAcc).

%% Semantic error at components.schemas.xxx:xx:xx
%% Component names can only contain the characters A-Z a-z 0-9 - . _
%% So replace ':' by '-'.
namespace(Module) ->
    case hocon_schema:namespace(Module) of
        undefined -> Module;
        NameSpace -> re:replace(to_bin(NameSpace), ":","-",[global])
    end.

hocon_schema_to_spec(?R_REF(Module, StructName), _LocalModule) ->
    {#{<<"$ref">> => ?TO_COMPONENTS_SCHEMA(Module, StructName)},
        [{Module, StructName}]};
hocon_schema_to_spec(?REF(StructName), LocalModule) ->
    {#{<<"$ref">> => ?TO_COMPONENTS_SCHEMA(LocalModule, StructName)},
        [{LocalModule, StructName}]};
hocon_schema_to_spec(Type, LocalModule) when ?IS_TYPEREFL(Type) ->
    {typename_to_spec(typerefl:name(Type), LocalModule), []};
hocon_schema_to_spec(?ARRAY(Item), LocalModule) ->
    {Schema, Refs} = hocon_schema_to_spec(Item, LocalModule),
    {#{type => array, items => Schema}, Refs};
hocon_schema_to_spec(?LAZY(Item), LocalModule) ->
    hocon_schema_to_spec(Item, LocalModule);
hocon_schema_to_spec(?ENUM(Items), _LocalModule) ->
    {#{type => string, enum => Items}, []};
hocon_schema_to_spec(?MAP(Name, Type), LocalModule) ->
    {Schema, SubRefs} = hocon_schema_to_spec(Type, LocalModule),
    {#{<<"type">> => object,
        <<"properties">> => #{<<"$", (to_bin(Name))/binary>> => Schema}},
        SubRefs};
hocon_schema_to_spec(?UNION(Types), LocalModule) ->
    {OneOf, Refs} = lists:foldl(fun(Type, {Acc, RefsAcc}) ->
        {Schema, SubRefs} = hocon_schema_to_spec(Type, LocalModule),
        {[Schema | Acc], SubRefs ++ RefsAcc}
                                end, {[], []}, Types),
    {#{<<"oneOf">> => OneOf}, Refs};
hocon_schema_to_spec(Atom, _LocalModule) when is_atom(Atom) ->
    {#{type => string, enum => [Atom]}, []}.

%% todo: Find a way to fetch enum value from user_id_type().
typename_to_spec("user_id_type()", _Mod) -> #{type => string, enum => [clientid, username]};
typename_to_spec("term()", _Mod) -> #{type => string, example => "term"};
typename_to_spec("boolean()", _Mod) -> #{type => boolean, example => true};
typename_to_spec("binary()", _Mod) -> #{type => string, example => <<"binary-example">>};
typename_to_spec("float()", _Mod) -> #{type => number, example => 3.14159};
typename_to_spec("integer()", _Mod) -> #{type => integer, example => 100};
typename_to_spec("non_neg_integer()", _Mod) -> #{type => integer, minimum => 1, example => 100};
typename_to_spec("number()", _Mod) -> #{type => number, example => 42};
typename_to_spec("string()", _Mod) -> #{type => string, example => <<"string-example">>};
typename_to_spec("atom()", _Mod) -> #{type => string, example => atom};
typename_to_spec("duration()", _Mod) -> #{type => string, example => <<"12m">>};
typename_to_spec("duration_s()", _Mod) -> #{type => string, example => <<"1h">>};
typename_to_spec("duration_ms()", _Mod) -> #{type => string, example => <<"32s">>};
typename_to_spec("percent()", _Mod) -> #{type => number, example => <<"12%">>};
typename_to_spec("file()", _Mod) -> #{type => string, example => <<"/path/to/file">>};
typename_to_spec("ip_port()", _Mod) -> #{type => string, example => <<"127.0.0.1:80">>};
typename_to_spec("url()", _Mod) -> #{type => string, example => <<"http://127.0.0.1">>};
typename_to_spec("server()", Mod) -> typename_to_spec("ip_port()", Mod);
typename_to_spec("connect_timeout()", Mod) -> typename_to_spec("timeout()", Mod);
typename_to_spec("timeout()", _Mod) -> #{<<"oneOf">> => [#{type => string, example => infinity},
    #{type => integer, example => 100}], example => infinity};
typename_to_spec("bytesize()", _Mod) -> #{type => string, example => <<"32MB">>};
typename_to_spec("wordsize()", _Mod) -> #{type => string, example => <<"1024KB">>};
typename_to_spec("map()", _Mod) -> #{type => object, example => #{}};
typename_to_spec("comma_separated_list()", _Mod) ->
    #{type => string, example => <<"item1,item2">>};
typename_to_spec("comma_separated_atoms()", _Mod) ->
    #{type => string, example => <<"item1,item2">>};
typename_to_spec("pool_type()", _Mod) ->
    #{type => string, enum => [random, hash], example => hash};
typename_to_spec("log_level()", _Mod) ->
    #{ type => string,
       enum => [debug, info, notice, warning, error, critical, alert, emergency, all]
    };
typename_to_spec("rate()", _Mod) ->
    #{type => string, example => <<"10M/s">>};
typename_to_spec("bucket_rate()", _Mod) ->
    #{type => string, example => <<"10M/s, 100M">>};
typename_to_spec(Name, Mod) ->
    Spec = range(Name),
    Spec1 = remote_module_type(Spec, Name, Mod),
    Spec2 = typerefl_array(Spec1, Name, Mod),
    Spec3 = integer(Spec2, Name),
    Spec3 =:= nomatch andalso
                 throw({error, #{msg => <<"Unsupport Type">>, type => Name, module => Mod}}),
             Spec3.

range(Name) ->
    case string:split(Name, "..") of
        [MinStr, MaxStr] -> %% 1..10 1..inf -inf..10
            Schema = #{type => integer},
            Schema1 = add_integer_prop(Schema, minimum, MinStr),
            add_integer_prop(Schema1, maximum, MaxStr);
        _ -> nomatch
    end.

%% Module:Type
remote_module_type(nomatch, Name, Mod) ->
    case string:split(Name, ":") of
        [_Module, Type] -> typename_to_spec(Type, Mod);
        _ -> nomatch
    end;
remote_module_type(Spec, _Name, _Mod) -> Spec.

%% [string()] or [integer()] or [xxx].
typerefl_array(nomatch, Name, Mod) ->
    case string:trim(Name, leading, "[") of
        Name -> nomatch;
        Name1 ->
            case string:trim(Name1, trailing, "]") of
                Name1 -> notmatch;
                Name2 ->
                    Schema = typename_to_spec(Name2, Mod),
                    #{type => array, items => Schema}
            end
    end;
typerefl_array(Spec, _Name, _Mod) -> Spec.

%% integer(1)
integer(nomatch, Name) ->
    case string:to_integer(Name) of
        {Int, []} -> #{type => integer, enum => [Int], example => Int, default => Int};
        _ -> nomatch
    end;
integer(Spec, _Name) -> Spec.

add_integer_prop(Schema, Key, Value) ->
    case string:to_integer(Value) of
        {error, no_integer} -> Schema;
        {Int, []}when Key =:= minimum -> Schema#{Key => Int, example => Int};
        {Int, []} -> Schema#{Key => Int}
    end.

to_bin(List) when is_list(List) ->
    case io_lib:printable_list(List) of
        true -> unicode:characters_to_binary(List);
        false -> List
    end;
to_bin(Boolean) when is_boolean(Boolean) -> Boolean;
to_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
to_bin(X) -> X.

parse_object(PropList = [_ | _], Module) when is_list(PropList) ->
    {Props, Required, Refs} =
        lists:foldl(fun({Name, Hocon}, {Acc, RequiredAcc, RefsAcc}) ->
            NameBin = to_bin(Name),
            case hoconsc:is_schema(Hocon) of
                true ->
                    HoconType = hocon_schema:field_schema(Hocon, type),
                    Init0 = init_prop([default | ?DEFAULT_FIELDS], #{}, Hocon),
                    Init = trans_desc(Init0, Hocon),
                    {Prop, Refs1} = hocon_schema_to_spec(HoconType, Module),
                    NewRequiredAcc =
                        case is_required(Hocon) of
                            true -> [NameBin | RequiredAcc];
                            false -> RequiredAcc
                        end,
                    {[{NameBin, maps:merge(Prop, Init)} | Acc], NewRequiredAcc, Refs1 ++ RefsAcc};
                false ->
                    {SubObject, SubRefs} = parse_object(Hocon, Module),
                    {[{NameBin, SubObject} | Acc], RequiredAcc, SubRefs ++ RefsAcc}
            end
                    end, {[], [], []}, PropList),
    Object = #{<<"type">> => object, <<"properties">> => lists:reverse(Props)},
    case Required of
        [] -> {Object, Refs};
        _ -> {maps:put(required, Required, Object), Refs}
    end;
parse_object(Other, Module) ->
    erlang:throw({error,
        #{msg => <<"Object only supports not empty proplists">>,
            args => Other, module => Module}}).

is_required(Hocon) ->
    hocon_schema:field_schema(Hocon, required) =:= true orelse
        hocon_schema:field_schema(Hocon, nullable) =:= false.

content(ApiSpec) ->
    content(ApiSpec, undefined).

content(ApiSpec, undefined) ->
    #{<<"application/json">> => #{<<"schema">> => ApiSpec}};
content(ApiSpec, Examples) when is_map(Examples) ->
    #{<<"application/json">> => Examples#{<<"schema">> => ApiSpec}}.

to_ref(Mod, StructName, Acc, RefsAcc) ->
    Ref = #{<<"$ref">> => ?TO_COMPONENTS_PARAM(Mod, StructName)},
    {[Ref | Acc], [{Mod, StructName, parameter} | RefsAcc]}.
