-module(emqx_dashboard_swagger).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% API
-export([spec/1, spec/2]).
-export([translate_req/2]).
-export([namespace/0, fields/1]).
-export([error_codes/1, error_codes/2]).
-define(MAX_ROW_LIMIT, 100).

%% API

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(METHODS, [get, post, put, head, delete, patch, options, trace]).

-define(DEFAULT_FIELDS, [example, allowReserved, style,
    explode, maxLength, allowEmptyValue, deprecated, minimum, maximum]).

-define(DEFAULT_FILTER, #{filter => fun ?MODULE:translate_req/2}).

-define(INIT_SCHEMA, #{fields => #{}, translations => #{}, validations => [], namespace => undefined}).

-define(TO_REF(_N_, _F_), iolist_to_binary([to_bin(_N_), ".", to_bin(_F_)])).
-define(TO_COMPONENTS_SCHEMA(_M_, _F_), iolist_to_binary([<<"#/components/schemas/">>, ?TO_REF(namespace(_M_), _F_)])).
-define(TO_COMPONENTS_PARAM(_M_, _F_), iolist_to_binary([<<"#/components/parameters/">>, ?TO_REF(namespace(_M_), _F_)])).

%% @equiv spec(Module, #{check_schema => false})
-spec(spec(module()) ->
    {list({Path, Specs, OperationId, Options}), list(Component)} when
    Path :: string()|binary(),
    Specs :: map(),
    OperationId :: atom(),
    Options :: #{filter => fun((map(),
    #{module => module(), path => string(), method => atom()}) -> map())},
    Component :: map()).
spec(Module) -> spec(Module, #{check_schema => false}).

-spec(spec(module(), #{check_schema => boolean()}) ->
    {list({Path, Specs, OperationId, Options}), list(Component)} when
    Path :: string()|binary(),
    Specs :: map(),
    OperationId :: atom(),
    Options :: #{filter => fun((map(),
    #{module => module(), path => string(), method => atom()}) -> map())},
    Component :: map()).
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

-spec(translate_req(#{binding => list(), query_string => list(), body => map()},
    #{module => module(), path => string(), method => atom()}) ->
    {ok, #{binding => list(), query_string => list(), body => map()}}|
    {400, 'BAD_REQUEST', binary()}).
translate_req(Request, #{module := Module, path := Path, method := Method}) ->
    #{Method := Spec} = apply(Module, schema, [Path]),
    try
        Params = maps:get(parameters, Spec, []),
        Body = maps:get(requestBody, Spec, []),
        {Bindings, QueryStr} = check_parameters(Request, Params, Module),
        NewBody = check_requestBody(Request, Body, Module, hoconsc:is_schema(Body)),
        {ok, Request#{bindings => Bindings, query_string => QueryStr, body => NewBody}}
    catch throw:Error ->
        {_, [{validation_error, ValidErr}]} = Error,
        #{path := Key, reason := Reason} = ValidErr,
        {400, 'BAD_REQUEST', iolist_to_binary(io_lib:format("~s : ~p", [Key, Reason]))}
    end.

namespace() -> "public".

fields(page) ->
    Desc = <<"Page number of the results to fetch.">>,
    Meta = #{in => query, desc => Desc, default => 1, example => 1},
    [{page, hoconsc:mk(integer(), Meta)}];
fields(limit) ->
    Desc = iolist_to_binary([<<"Results per page(max ">>,
        integer_to_binary(?MAX_ROW_LIMIT), <<")">>]),
    Meta = #{in => query, desc => Desc, default => ?MAX_ROW_LIMIT, example => 50},
    [{limit, hoconsc:mk(range(1, ?MAX_ROW_LIMIT), Meta)}].

error_codes(Codes) ->
    error_codes(Codes, <<"Error code to troubleshoot problems.">>).

error_codes(Codes = [_ | _], MsgExample) ->
    [
        {code, hoconsc:mk(hoconsc:enum(Codes))},
        {message, hoconsc:mk(string(), #{
            desc => <<"Details description of the error.">>,
            example => MsgExample
        })}
    ].

support_check_schema(#{check_schema := true}) -> ?DEFAULT_FILTER;
support_check_schema(#{check_schema := Func}) when is_function(Func, 2) -> #{filter => Func};
support_check_schema(_) -> #{filter => undefined}.

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
        maps:without([operationId], Schema)),
    {maps:get(operationId, Schema), Specs, Refs}.

check_parameters(Request, Spec, Module) ->
    #{bindings := Bindings, query_string := QueryStr} = Request,
    BindingsBin = maps:fold(fun(Key, Value, Acc) -> Acc#{atom_to_binary(Key) => Value} end, #{}, Bindings),
    check_parameter(Spec, BindingsBin, QueryStr, Module, #{}, #{}).

check_parameter([?REF(Fields) | Spec], Bindings, QueryStr, LocalMod, BindingsAcc, QueryStrAcc) ->
    check_parameter([?R_REF(LocalMod, Fields) | Spec], Bindings, QueryStr, LocalMod, BindingsAcc, QueryStrAcc);
check_parameter([?R_REF(Module, Fields) | Spec], Bindings, QueryStr, LocalMod, BindingsAcc, QueryStrAcc) ->
    Params = apply(Module, fields, [Fields]),
    check_parameter(Params ++ Spec, Bindings, QueryStr, LocalMod, BindingsAcc, QueryStrAcc);
check_parameter([], _Bindings, _QueryStr, _Module, NewBindings, NewQueryStr) -> {NewBindings, NewQueryStr};
check_parameter([{Name, Type} | Spec], Bindings, QueryStr, Module, BindingsAcc, QueryStrAcc) ->
    Schema = ?INIT_SCHEMA#{roots => [{Name, Type}]},
    case hocon_schema:field_schema(Type, in) of
        path ->
            NewBindings = hocon_schema:check_plain(Schema, Bindings, #{atom_key => true, override_env => false}),
            NewBindingsAcc = maps:merge(BindingsAcc, NewBindings),
            check_parameter(Spec, Bindings, QueryStr, Module, NewBindingsAcc, QueryStrAcc);
        query ->
            NewQueryStr = hocon_schema:check_plain(Schema, QueryStr, #{override_env => false}),
            NewQueryStrAcc = maps:merge(QueryStrAcc, NewQueryStr),
            check_parameter(Spec, Bindings, QueryStr, Module, BindingsAcc, NewQueryStrAcc)
    end.

check_requestBody(#{body := Body}, Schema, Module, true) ->
    Type0 = hocon_schema:field_schema(Schema, type),
    Type =
        case Type0 of
            ?REF(StructName) -> ?R_REF(Module, StructName);
            _ -> Type0
        end,
    NewSchema = ?INIT_SCHEMA#{roots => [{root, Type}]},
    #{<<"root">> := NewBody} = hocon_schema:check_plain(NewSchema, #{<<"root">> => Body}, #{override_env => false}),
    NewBody;
%% TODO not support nest object check yet, please use ref!
%% RequestBody = [ {per_page, mk(integer(), #{}},
%%                 {nest_object, [
%%                   {good_nest_1, mk(integer(), #{})},
%%                   {good_nest_2, mk(ref(?MODULE, good_ref), #{})}
%%                ]}
%% ]
check_requestBody(#{body := Body}, Spec, _Module, false) ->
    lists:foldl(fun({Name, Type}, Acc) ->
        Schema = ?INIT_SCHEMA#{roots => [{Name, Type}]},
        maps:merge(Acc, hocon_schema:check_plain(Schema, Body))
                end, #{}, Spec).

%% tags, description, summary, security, deprecated
meta_to_spec(Meta, Module) ->
    {Params, Refs1} = parameters(maps:get(parameters, Meta, []), Module),
    {RequestBody, Refs2} = requestBody(maps:get(requestBody, Meta, []), Module),
    {Responses, Refs3} = responses(maps:get(responses, Meta, #{}), Module),
    {
        to_spec(Meta, Params, RequestBody, Responses),
        lists:usort(Refs1 ++ Refs2 ++ Refs3)
    }.

to_spec(Meta, Params, [], Responses) ->
    Spec = maps:without([parameters, requestBody, responses], Meta),
    Spec#{parameters => Params, responses => Responses};
to_spec(Meta, Params, RequestBody, Responses) ->
    Spec = to_spec(Meta, Params, [], Responses),
    maps:put(requestBody, RequestBody, Spec).

parameters(Params, Module) ->
    {SpecList, AllRefs} =
        lists:foldl(fun(Param, {Acc, RefsAcc}) ->
            case Param of
                ?REF(StructName) ->
                    {[#{<<"$ref">> => ?TO_COMPONENTS_PARAM(Module, StructName)} | Acc],
                        [{Module, StructName, parameter} | RefsAcc]};
                ?R_REF(RModule, StructName) ->
                    {[#{<<"$ref">> => ?TO_COMPONENTS_PARAM(RModule, StructName)} | Acc],
                        [{RModule, StructName, parameter} | RefsAcc]};
                {Name, Type} ->
                    In = hocon_schema:field_schema(Type, in),
                    In =:= undefined andalso throw({error, <<"missing in:path/query field in parameters">>}),
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

requestBody([], _Module) -> {[], []};
requestBody(Schema, Module) ->
    {Props, Refs} =
        case hoconsc:is_schema(Schema) of
            true ->
                HoconSchema = hocon_schema:field_schema(Schema, type),
                hocon_schema_to_spec(HoconSchema, Module);
            false -> parse_object(Schema, Module)
        end,
    {#{<<"content">> => #{<<"application/json">> => #{<<"schema">> => Props}}},
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
    Content = #{<<"application/json">> => #{<<"schema">> => Spec}},
    {Acc#{integer_to_binary(Status) => #{<<"content">> => Content}}, Refs ++ RefsAcc, Module};
response(Status, Schema, {Acc, RefsAcc, Module}) ->
    case hoconsc:is_schema(Schema) of
        true ->
            Hocon = hocon_schema:field_schema(Schema, type),
            {Spec, Refs} = hocon_schema_to_spec(Hocon, Module),
            Init = trans_desc(#{}, Schema),
            Content = #{<<"application/json">> => #{<<"schema">> => Spec}},
            {Acc#{integer_to_binary(Status) => Init#{<<"content">> => Content}}, Refs ++ RefsAcc, Module};
        false ->
            {Props, Refs} = parse_object(Schema, Module),
            Content = #{<<"content">> => #{<<"application/json">> => #{<<"schema">> => Props}}},
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
typename_to_spec("map()", _Mod) -> #{type => string, example => <<>>};
typename_to_spec("comma_separated_list()", _Mod) -> #{type => string, example => <<"item1,item2">>};
typename_to_spec("comma_separated_atoms()", _Mod) -> #{type => string, example => <<"item1,item2">>};
typename_to_spec("pool_type()", _Mod) -> #{type => string, enum => [random, hash], example => hash};
typename_to_spec("log_level()", _Mod) ->
    #{type => string, enum => [debug, info, notice, warning, error, critical, alert, emergency, all]};
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

to_bin([Atom | _] = List) when is_atom(Atom) -> iolist_to_binary(io_lib:format("~p", [List]));
to_bin(List) when is_list(List) -> unicode:characters_to_binary(List);
to_bin(B) when is_boolean(B) -> B;
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
