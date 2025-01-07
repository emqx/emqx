%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_conf_schema_types).

-include_lib("hocon/include/hocon_types.hrl").

-export([readable/2]).
-export([readable_swagger/2, readable_dashboard/2, readable_docgen/2]).

%% Takes a typerefl name or hocon schema's display name and returns
%% a map of different flavors of more readable type specs.
%% - swagger: for swagger spec
%% - dashboard: to facilitate the dashboard UI rendering
%% - docgen: for documenation generation
readable(Module, TypeStr) when is_binary(TypeStr) ->
    readable(Module, binary_to_list(TypeStr));
readable(Module, TypeStr) when is_list(TypeStr) ->
    try
        %% Module is ignored so far as all types are distinguished by their names
        readable(TypeStr)
    catch
        throw:Reason ->
            throw(#{
                reason => Reason,
                type => TypeStr,
                module => Module
            });
        error:Reason:Stacktrace ->
            throw(#{
                reason => Reason,
                stacktrace => Stacktrace,
                type => TypeStr,
                module => Module
            })
    end.

readable_swagger(Module, TypeStr) ->
    get_readable(Module, TypeStr, swagger).

readable_dashboard(Module, TypeStr) ->
    get_readable(Module, TypeStr, dashboard).

readable_docgen(Module, TypeStr) ->
    get_readable(Module, TypeStr, docgen).

get_readable(Module, TypeStr, Flavor) ->
    Map = readable(Module, TypeStr),
    case maps:get(Flavor, Map, undefined) of
        undefined -> throw(#{reason => unknown_type, module => Module, type => TypeStr});
        Value -> Value
    end.

readable("boolean()") ->
    #{
        swagger => #{type => boolean},
        dashboard => #{type => boolean},
        docgen => #{type => "Boolean"}
    };
readable("template()") ->
    #{
        swagger => #{type => string},
        dashboard => #{type => string, is_template => true},
        docgen => #{type => "String", desc => ?DESC(template)}
    };
readable("template_str()") ->
    #{
        swagger => #{type => string},
        dashboard => #{type => string, is_template => true},
        docgen => #{type => "String", desc => ?DESC(template)}
    };
readable("binary()") ->
    #{
        swagger => #{type => string},
        dashboard => #{type => string},
        docgen => #{type => "String"}
    };
readable("float()") ->
    #{
        swagger => #{type => number},
        dashboard => #{type => number},
        docgen => #{type => "Float"}
    };
readable("integer()") ->
    #{
        swagger => #{type => integer},
        dashboard => #{type => integer},
        docgen => #{type => "Integer"}
    };
readable("non_neg_integer()") ->
    #{
        swagger => #{type => integer, minimum => 0},
        dashboard => #{type => integer, minimum => 0},
        docgen => #{type => "Integer(0..+inf)"}
    };
readable("pos_integer()") ->
    #{
        swagger => #{type => integer, minimum => 1},
        dashboard => #{type => integer, minimum => 1},
        docgen => #{type => "Integer(1..+inf)"}
    };
readable("number()") ->
    #{
        swagger => #{type => number},
        dashboard => #{type => number},
        docgen => #{type => "Number"}
    };
readable("string()") ->
    #{
        swagger => #{type => string},
        dashboard => #{type => string},
        docgen => #{type => "String"}
    };
readable("atom()") ->
    #{
        swagger => #{type => string},
        dashboard => #{type => string},
        docgen => #{type => "String"}
    };
readable("epoch_second()") ->
    %% only for swagger
    #{
        swagger => #{
            <<"oneOf">> => [
                #{type => integer, example => 1640995200, description => <<"epoch-second">>},
                #{
                    type => string,
                    example => <<"2022-01-01T00:00:00.000Z">>,
                    format => <<"date-time">>
                }
            ]
        }
    };
readable("epoch_millisecond()") ->
    %% only for swagger
    #{
        swagger => #{
            <<"oneOf">> => [
                #{
                    type => integer,
                    example => 1640995200000,
                    description => <<"epoch-millisecond">>
                },
                #{
                    type => string,
                    example => <<"2022-01-01T00:00:00.000Z">>,
                    format => <<"date-time">>
                }
            ]
        }
    };
readable("epoch_microsecond()") ->
    %% only for swagger
    #{
        swagger => #{
            <<"oneOf">> => [
                #{
                    type => integer,
                    example => 1640995200000000,
                    description => <<"epoch-microsecond">>
                },
                #{
                    type => string,
                    example => <<"2022-01-01T00:00:00.000000Z">>,
                    format => <<"date-time">>
                }
            ]
        }
    };
readable("duration()") ->
    #{
        swagger => #{type => string, example => <<"12m">>},
        dashboard => #{type => duration},
        docgen => #{type => "Duration", example => <<"12m">>, desc => ?DESC(duration)}
    };
readable("duration_s()") ->
    #{
        swagger => #{type => string, example => <<"1h">>},
        dashboard => #{type => duration, minimum => <<"1s">>},
        docgen => #{type => "Duration(s)", example => <<"1h">>, desc => ?DESC(duration)}
    };
readable("duration_ms()") ->
    #{
        swagger => #{type => string, example => <<"32s">>},
        dashboard => #{type => duration},
        docgen => #{type => "Duration", example => <<"32s">>, desc => ?DESC(duration)}
    };
readable("timeout_duration()") ->
    #{
        swagger => #{type => string, example => <<"12m">>},
        dashboard => #{type => duration},
        docgen => #{type => "Duration", example => <<"12m">>, desc => ?DESC(duration)}
    };
readable("timeout_duration_s()") ->
    #{
        swagger => #{type => string, example => <<"1h">>},
        dashboard => #{type => duration, minimum => <<"1s">>},
        docgen => #{type => "Duration(s)", example => <<"1h">>, desc => ?DESC(duration)}
    };
readable("timeout_duration_ms()") ->
    #{
        swagger => #{type => string, example => <<"32s">>},
        dashboard => #{type => duration},
        docgen => #{type => "Duration", example => <<"32s">>, desc => ?DESC(duration)}
    };
readable("percent()") ->
    #{
        swagger => #{type => string, example => <<"12%">>},
        dashboard => #{type => percent},
        docgen => #{type => "String", example => <<"12%">>}
    };
readable("ip_port()") ->
    #{
        swagger => #{type => string, example => <<"127.0.0.1:80">>},
        dashboard => #{type => ip_port},
        docgen => #{type => "String", example => <<"127.0.0.1:80">>}
    };
readable("url()") ->
    #{
        swagger => #{type => string, example => <<"http://127.0.0.1">>},
        dashboard => #{type => url},
        docgen => #{type => "String", example => <<"http://127.0.0.1">>}
    };
readable("bytesize()") ->
    #{
        swagger => #{type => string, example => <<"32MB">>},
        dashboard => #{type => 'byteSize'},
        docgen => #{type => "Bytesize", example => <<"32MB">>, desc => ?DESC(bytesize)}
    };
readable("wordsize()") ->
    #{
        swagger => #{type => string, example => <<"1024KB">>},
        dashboard => #{type => 'wordSize'},
        docgen => #{type => "Bytesize", example => <<"1024KB">>, desc => ?DESC(bytesize)}
    };
readable("map(" ++ Map) ->
    [$) | _MapArgs] = lists:reverse(Map),
    %% TODO: for docgen, parse map args. e.g. Map(String,String)
    #{
        swagger => #{type => object, example => #{}},
        dashboard => #{type => object},
        docgen => #{type => "Map", example => #{}}
    };
readable("qos()") ->
    #{
        swagger => #{type => integer, minimum => 0, maximum => 2, example => 0},
        dashboard => #{type => enum, symbols => [0, 1, 2]},
        docgen => #{type => "Integer(0..2)", example => 0}
    };
readable("comma_separated_list()") ->
    #{
        swagger => #{type => string, example => <<"item1,item2">>},
        dashboard => #{type => comma_separated_string},
        docgen => #{type => "String", example => <<"item1,item2">>}
    };
readable("comma_separated_binary()") ->
    #{
        swagger => #{type => string, example => <<"item1,item2">>},
        dashboard => #{type => comma_separated_string},
        docgen => #{type => "String", example => <<"item1,item2">>}
    };
readable("comma_separated_atoms()") ->
    #{
        swagger => #{type => string, example => <<"item1,item2">>},
        dashboard => #{type => comma_separated_string},
        docgen => #{type => "String", example => <<"item1,item2">>}
    };
readable("json_binary()") ->
    #{
        swagger => #{type => string, example => <<"{\"a\": [1,true]}">>},
        dashboard => #{type => string},
        docgen => #{type => "String", example => <<"{\"a\": [1,true]}">>}
    };
readable("port_number()") ->
    Result = try_range("1..65535"),
    true = is_map(Result),
    Result;
readable("secret()") ->
    #{
        swagger => #{type => string, example => <<"R4ND0M/S∃CЯ∃T"/utf8>>},
        dashboard => #{type => string},
        docgen => #{
            type => "Secret",
            example => <<"R4ND0M/S∃CЯ∃T"/utf8>>,
            desc => ?DESC(secret)
        }
    };
readable(TypeStr0) ->
    case string:split(TypeStr0, ":") of
        [ModuleStr, TypeStr] ->
            Module = list_to_existing_atom(ModuleStr),
            readable(Module, TypeStr);
        _ ->
            parse(TypeStr0)
    end.

parse(TypeStr) ->
    try_parse(TypeStr, [
        fun try_typerefl_array/1,
        fun try_range/1
    ]).

try_parse(_TypeStr, []) ->
    throw(unknown_type);
try_parse(TypeStr, [ParseFun | More]) ->
    case ParseFun(TypeStr) of
        nomatch ->
            try_parse(TypeStr, More);
        Result ->
            Result
    end.

%% [string()] or [integer()] or [xxx] or [xxx,...]
try_typerefl_array(Name) ->
    case string:trim(Name, leading, "[") of
        Name ->
            nomatch;
        Name1 ->
            case string:trim(Name1, trailing, ",.]") of
                Name1 ->
                    notmatch;
                Name2 ->
                    Flavors = readable(Name2),
                    DocgenSpec = maps:get(docgen, Flavors),
                    DocgenType = maps:get(type, DocgenSpec),
                    #{
                        swagger => #{type => array, items => maps:get(swagger, Flavors)},
                        dashboard => #{type => array, items => maps:get(dashboard, Flavors)},
                        docgen => #{type => "Array(" ++ DocgenType ++ ")"}
                    }
            end
    end.

try_range(Name) ->
    case string:split(Name, "..") of
        %% 1..10 1..inf -inf..10
        [MinStr, MaxStr] ->
            Schema0 = #{type => integer},
            Schema1 = add_integer_prop(Schema0, minimum, MinStr),
            Schema = add_integer_prop(Schema1, maximum, MaxStr),
            #{
                swagger => Schema,
                dashboard => Schema,
                docgen => #{type => "Integer(" ++ MinStr ++ ".." ++ MaxStr ++ ")"}
            };
        _ ->
            nomatch
    end.

add_integer_prop(Schema, Key, Value) ->
    case string:to_integer(Value) of
        {error, no_integer} -> Schema;
        {Int, []} -> Schema#{Key => Int}
    end.
