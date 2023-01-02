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

-module(emqx_rule_validator_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-define(VALID_SPEC,
    #{
        string_required => #{
            type => string,
            required => true
        },
        string_optional_with_default => #{
            type => string,
            required => false,
            default => <<"a/b">>
        },
        string_optional_without_default_0 => #{
            type => string,
            required => false
        },
        string_optional_without_default_1 => #{
            type => string
        },
        type_number => #{
            type => number,
            required => true
        },
        type_boolean => #{
            type => boolean,
            required => true
        },
        type_enum_number => #{
            type => number,
            enum => [-1, 0, 1, 2],
            required => true
        },
        type_file => #{
            type => file,
            required => true
        },
        type_object => #{
            type => object,
            required => true,
            schema => #{
                string_required => #{
                    type => string,
                    required => true
                },
                type_number => #{
                    type => number,
                    required => true
                }
            }
        },
        type_cfgselect => #{
            type => cfgselect,
            enum => [<<"upload">>, <<"path">>],
            default => <<"upload">>,
            items =>
                #{
                  upload =>
                      #{
                        kerberos_keytab =>
                            #{
                              order => 6,
                              type => binary_file,
                              default => #{file => <<"">>, filename => <<"no_keytab.key">>}
                             }
                       },
                  path =>
                      #{
                        kerberos_keytab_path =>
                            #{
                              order => 7,
                              type => string,
                              default => <<"">>
                             }
                       }
                 }
        },
        type_array => #{
            type => array,
            required => true,
            items => #{
                type => string,
                required => true
            }
        }
    }).

all() -> emqx_ct:all(?MODULE).

t_validate_spec_the_complex(_) ->
    ok = emqx_rule_validator:validate_spec(?VALID_SPEC).

t_validate_spec_invalid_1(_) ->
    ?assertThrow({required_field_missing, {type, _}},
        emqx_rule_validator:validate_spec(#{
            a => #{
                required => true
            }
        })).

t_validate_spec_invalid_2(_) ->
    ?assertThrow({required_field_missing, {schema, _}},
        emqx_rule_validator:validate_spec(#{
            a => #{
                type => object
            }
        })).

t_validate_spec_invalid_2_1(_) ->
    ?assertThrow({required_field_missing, {items, _}},
        emqx_rule_validator:validate_spec(#{
            a => #{
                type => cfgselect
            }
        })).

t_validate_spec_invalid_3(_) ->
    ?assertThrow({required_field_missing, {items, _}},
        emqx_rule_validator:validate_spec(#{
            a => #{
                type => array
            }
        })).

t_validate_params_0(_) ->
    Params = #{<<"eee">> => <<"eee">>},
    Specs = #{<<"eee">> => #{
                type => string,
                required => true
             }},
    ?assertEqual(Params,
        emqx_rule_validator:validate_params(Params, Specs)).

t_validate_params_1(_) ->
    Params = #{<<"eee">> => 1},
    Specs = #{<<"eee">> => #{
                type => string,
                required => true
             }},
    ?assertThrow({invalid_data_type, {string, 1}},
        emqx_rule_validator:validate_params(Params, Specs)).

t_validate_params_2(_) ->
    ?assertThrow({required_field_missing, <<"eee">>},
        emqx_rule_validator:validate_params(
            #{<<"abc">> => 1},
            #{<<"eee">> => #{
                type => string,
                required => true
             }})).

t_validate_params_format(_) ->
    Params = #{<<"eee">> => <<"abc">>},
    Params1 = #{<<"eee">> => <<"http://abc:8080">>},
    Params2 = #{<<"eee">> => <<"http://abc">>},
    Specs = #{<<"eee">> => #{
                type => string,
                format => url,
                required => true
             }},
    ?assertThrow({invalid_data_type, {string, <<"abc">>}},
        emqx_rule_validator:validate_params(Params, Specs)),
    ?assertEqual(Params1,
        emqx_rule_validator:validate_params(Params1, Specs)),
    ?assertEqual(Params2,
        emqx_rule_validator:validate_params(Params2, Specs)).

t_validate_params_fill_default(_) ->
    Params = #{<<"abc">> => 1},
    Specs = #{<<"eee">> => #{
                type => string,
                required => false,
                default => <<"hello">>
             }},
    ?assertMatch(#{<<"abc">> := 1, <<"eee">> := <<"hello">>},
        emqx_rule_validator:validate_params(Params, Specs)).

t_validate_params_binary_file(_) ->
    Params = #{<<"kfile">> => #{<<"file">> => <<"foo">>, <<"filename">> => <<"foo.key">>}},
    Specs = #{<<"kfile">> => #{
                type => binary_file,
                required => true
             }},
    ?assertMatch(#{<<"kfile">> := #{<<"file">> := <<"foo">>, <<"filename">> := <<"foo.key">>}},
        emqx_rule_validator:validate_params(Params, Specs)),
    Params1 = #{<<"kfile">> => #{<<"file">> => <<"foo">>}},
    Specs1 = #{<<"kfile">> => #{
                type => binary_file,
                required => true
             }},
    ?assertThrow({invalid_data_type, {binary_file, #{<<"file">> := <<"foo">>}}},
        emqx_rule_validator:validate_params(Params1, Specs1)).

t_validate_params_the_complex(_) ->
    Params = #{
        <<"string_required">> => <<"hello">>,
        <<"type_number">> => 1,
        <<"type_boolean">> => true,
        <<"type_enum_number">> => 2,
        <<"type_file">> => <<"">>,
        <<"type_object">> => #{
            <<"string_required">> => <<"hello2">>,
            <<"type_number">> => 1.3
        },
        <<"type_cfgselect">> => <<"upload">>,
        <<"kerberos_keytab">> => #{<<"file">> => <<"foo">>, <<"filename">> => <<"foo.key">>},
        <<"type_array">> => [<<"ok">>, <<"no">>]
    },
    ?assertMatch(
        #{  <<"string_required">> := <<"hello">>,
            <<"string_optional_with_default">> := <<"a/b">>,
            <<"type_number">> := 1,
            <<"type_boolean">> := true,
            <<"type_enum_number">> := 2,
            <<"type_file">> := <<"">>,
            <<"type_object">> := #{
                <<"string_required">> := <<"hello2">>,
                <<"type_number">> := 1.3
            },
            <<"kerberos_keytab">> := #{<<"file">> := <<"foo">>, <<"filename">> := <<"foo.key">>},
            <<"type_cfgselect">> := <<"upload">>,
            <<"type_array">> := [<<"ok">>, <<"no">>]
        },
        emqx_rule_validator:validate_params(Params, ?VALID_SPEC)).
