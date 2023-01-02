%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_validator).

-include("rule_engine.hrl").

-export([ validate_params/2
        , validate_spec/1
        ]).

-type name() :: atom().

-type spec() :: #{
    type := data_type(),
    required => boolean(),
    default => term(),
    enum => list(term()),
    schema => spec()
}.

-type data_type() :: string | password | number | boolean
    | object | array | file | binary_file | cfgselect.

-type params_spec() :: #{name() => spec()} | any.
-type params() :: #{binary() => term()}.

-define(DATA_TYPES,
        [ string
        , password %% TODO: [5.0] remove this, use string instead
        , number
        , boolean
        , object
        , array
        , file
        , binary_file
        , cfgselect %% TODO: [5.0] refactor this
        ]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

%% Validate the params according to the spec.
%% Some keys will be added into the result if they have default values in spec.
%% Note that this function throws exception in case of validation failure.
-spec(validate_params(params(), params_spec()) -> params()).
validate_params(Params, any) -> Params;
validate_params(Params, ParamsSepc) ->
    maps:fold(fun(Name, Spec, Params0) ->
        IsRequired = maps:get(required, Spec, false),
        BinName = bin(Name),
        find_field(Name, Params,
            fun (not_found) when IsRequired =:= true ->
                    throw({required_field_missing, BinName});
                (not_found) when IsRequired =:= false ->
                    case maps:find(default, Spec) of
                        {ok, Default} -> Params0#{BinName => Default};
                        error -> Params0
                    end;
                (Val) ->
                    Params0#{BinName => validate_value(Val, Spec)}
            end)
    end, Params, ParamsSepc).

-spec(validate_spec(params_spec()) -> ok).
validate_spec(any) -> ok;
validate_spec(ParamsSepc) ->
    map_foreach(fun do_validate_spec/2, ParamsSepc).

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

validate_value(Val, #{type := Union} = Spec) when is_list(Union) ->
    validate_union(Val, Union, Spec);
validate_value(Val, #{enum := Enum}) ->
    validate_enum(Val, Enum);
validate_value(Val, #{type := object} = Spec) ->
    validate_params(Val, maps:get(schema, Spec, any));
validate_value(Val, #{type := Type} = Spec) ->
    validate_type(Val, Type, Spec).

validate_union(Val, Union, _Spec) ->
    do_validate_union(Val, Union, Union, _Spec).

do_validate_union(Val, Union, [], _Spec) ->
    throw({invalid_data_type, {union, {Val, Union}}});
do_validate_union(Val, Union, [Type | Types], Spec) ->
    try
        validate_type(Val, Type, Spec)
    catch _:_ ->
        do_validate_union(Val, Union, Types, Spec)
    end.

validate_type(Val, file, _Spec) ->
    validate_file(Val);
validate_type(Val, binary_file, _Spec) ->
    validate_binary_file(Val);
validate_type(Val, String, Spec) when String =:= string;
                                      String =:= password ->
    validate_string(Val, reg_exp(maps:get(format, Spec, any)));
validate_type(Val, number, Spec) ->
    validate_number(Val, maps:get(range, Spec, any));
validate_type(Val, boolean, _Spec) ->
    validate_boolean(Val);
validate_type(Val, array, Spec) ->
    ItemsSpec = maps:get(items, Spec),
    [validate_value(V, ItemsSpec) || V <- Val];
validate_type(Val, cfgselect, _Spec) ->
    %% TODO: [5.0] refactor this.
    Val.

validate_enum(Val, BoolEnum) when BoolEnum == [true, false]; BoolEnum == [false, true] ->
    validate_boolean(Val);
validate_enum(Val, Enum) ->
    case lists:member(Val, Enum) of
        true -> Val;
        false -> throw({invalid_data_type, {enum, {Val, Enum}}})
    end.

validate_string(Val, RegExp) ->
    try re:run(Val, RegExp) of
        nomatch -> throw({invalid_data_type, {string, Val}});
        _Match -> Val
    catch
        _:_ -> throw({invalid_data_type, {string, Val}})
    end.

validate_number(Val, any) when is_integer(Val); is_float(Val) ->
    Val;
validate_number(Val, _Range = [Min, Max])
        when (is_integer(Val) orelse is_float(Val)),
             (Val >= Min andalso Val =< Max) ->
    Val;
validate_number(Val, Range) ->
    throw({invalid_data_type, {number, {Val, Range}}}).

validate_boolean(true) -> true;
validate_boolean(<<"true">>) -> true;
validate_boolean(false) -> false;
validate_boolean(<<"false">>) -> false;
validate_boolean(Val) -> throw({invalid_data_type, {boolean, Val}}).

validate_binary_file(#{<<"file">> := _, <<"filename">> := _} = Val) ->
    Val;
validate_binary_file(Val) ->
    throw({invalid_data_type, {binary_file, Val}}).
validate_file(Val) when is_map(Val) -> Val;
validate_file(Val) when is_list(Val) -> Val;
validate_file(Val) when is_binary(Val) -> Val;
validate_file(Val) -> throw({invalid_data_type, {file, Val}}).

reg_exp(url) -> "^https?://\\w+(\.\\w+)*(:[0-9]+)?";
reg_exp(topic) -> "^/?(\\w|\\#|\\+)+(/?(\\w|\\#|\\+))*/?$";
reg_exp(resource_type) -> "[a-zA-Z0-9_:-]";
reg_exp(any) -> ".*";
reg_exp(RegExp) -> RegExp.

do_validate_spec(Name, #{type := object} = Spec) ->
    find_field(schema, Spec,
        fun (not_found) -> throw({required_field_missing, {schema, {in, Name}}});
            (Schema) -> validate_spec(Schema)
        end);
do_validate_spec(Name, #{type := cfgselect} = Spec) ->
    find_field(items, Spec,
        fun (not_found) -> throw({required_field_missing, {items, {in, Name}}});
            (Items) ->
                maps:map(fun(_K, Schema) ->
                        validate_spec(Schema)
                    end, Items)
        end);
do_validate_spec(Name, #{type := array} = Spec) ->
    find_field(items, Spec,
        fun (not_found) -> throw({required_field_missing, {items, {in, Name}}});
            (Items) -> do_validate_spec(Name, Items)
        end);
do_validate_spec(_Name, #{type := Types}) when is_list(Types) ->
    _ = [ok = supported_data_type(Type, ?DATA_TYPES) || Type <- Types],
    ok;
do_validate_spec(_Name, #{type := Type}) ->
    _ = supported_data_type(Type, ?DATA_TYPES);

do_validate_spec(Name, _Spec) ->
    throw({required_field_missing, {type, {in, Name}}}).

supported_data_type(Type, Supported) ->
    case lists:member(Type, Supported) of
        false -> throw({unsupported_data_types, Type});
        true -> ok
    end.

map_foreach(Fun, Map) ->
    Iterator = maps:iterator(Map),
    map_foreach_loop(Fun, maps:next(Iterator)).

map_foreach_loop(_Fun, none) -> ok;
map_foreach_loop(Fun, {Key, Value, Iterator}) ->
    _ = Fun(Key, Value),
    map_foreach_loop(Fun, maps:next(Iterator)).

find_field(Field, Spec, Func) ->
    do_find_field([bin(Field), Field], Spec, Func).

do_find_field([], _Spec, Func) ->
    Func(not_found);
do_find_field([F | Fields], Spec, Func) ->
    case maps:find(F, Spec) of
        {ok, Value} -> Func(Value);
        error ->
            do_find_field(Fields, Spec, Func)
    end.

bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
bin(Str) when is_list(Str) -> iolist_to_binary(Str);
bin(Bin) when is_binary(Bin) -> Bin.
