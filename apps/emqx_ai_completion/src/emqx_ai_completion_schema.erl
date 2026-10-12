%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    tags/0
]).

-export([
    completion_profile_sctype_api/1,
    provider_sctype_api/1
]).

-export([check_provider/1]).

%%------------------------------------------------------------------------------
%% `hocon_schema' APIs
%%------------------------------------------------------------------------------

namespace() -> ai.

roots() ->
    [ai].

tags() ->
    [<<"AI Functions">>].

fields(ai) ->
    [
        {providers,
            mk(
                hoconsc:array(provider_sctype()),
                #{
                    default => [],
                    desc => ?DESC(providers)
                }
            )},
        {completion_profiles,
            mk(
                hoconsc:array(completion_profile_sctype()),
                #{
                    default => [],
                    desc => ?DESC(completion_profiles)
                }
            )}
    ];
%%
%% OpenAI /chat/messages provider
%%
fields(openai_provider) ->
    base_provider_fields() ++
        [
            {base_url,
                mk(binary(), #{
                    required => false,
                    desc => ?DESC(base_url),
                    validator => fun validate_url/1,
                    default => <<"https://api.openai.com/v1">>
                })}
        ];
fields(openai_provider_api_get) ->
    fields(openai_provider);
fields(openai_provider_api_put) ->
    without_fields([name], fields(openai_provider));
%%
%% OpenAI /responses provider
%%
fields(openai_response_provider) ->
    fields(openai_provider);
fields(openai_response_provider_api_get) ->
    fields(openai_response_provider);
fields(openai_response_provider_api_put) ->
    without_fields([name], fields(openai_response_provider));
%%
%% OpenAI /decisions provider
%%
fields(openai_decisions_provider) ->
    fields(openai_provider);
fields(openai_decisions_provider_api_get) ->
    fields(openai_decisions_provider);
fields(openai_decisions_provider_api_put) ->
    without_fields([name], fields(openai_decisions_provider));
%%
%% Anthropic provider
%%
fields(anthropic_provider) ->
    base_provider_fields() ++
        [
            {anthropic_version,
                mk(enum(['2023-06-01']), #{
                    default => '2023-06-01', desc => ?DESC(anthropic_version), required => false
                })},
            {base_url,
                mk(binary(), #{
                    required => false,
                    desc => ?DESC(base_url),
                    validator => fun validate_url/1,
                    default => <<"https://api.anthropic.com/v1">>
                })}
        ];
fields(anthropic_provider_api_get) ->
    fields(anthropic_provider);
fields(anthropic_provider_api_put) ->
    without_fields([name], fields(anthropic_provider));
%%
%% OpenAI /chat/messages completion profile
fields(openai_completion_profile) ->
    [
        {name,
            mk(binary(), #{
                required => true,
                desc => ?DESC(completion_profile_name),
                validator => fun validate_name/1
            })},
        {type, mk(openai, #{default => openai, required => true, desc => ?DESC(type)})},
        {provider_name, mk(binary(), #{required => true, desc => ?DESC(provider_name)})},
        {system_prompt, mk(binary(), #{default => <<>>, desc => ?DESC(system_prompt)})},
        {model, mk(binary(), #{default => <<"gpt-4o">>, desc => ?DESC(model)})}
    ];
fields(openai_completion_profile_api_get) ->
    fields(openai_completion_profile);
fields(openai_completion_profile_api_put) ->
    without_fields([name], fields(openai_completion_profile));
%%
%% OpenAI /responses completion profile
%%
fields(openai_response_completion_profile) ->
    without_fields([type], fields(openai_completion_profile)) ++
        [
            {type,
                mk(openai_response, #{
                    default => openai_response, required => true, desc => ?DESC(type)
                })}
        ];
fields(openai_response_completion_profile_api_get) ->
    fields(openai_response_completion_profile);
fields(openai_response_completion_profile_api_put) ->
    without_fields([name], fields(openai_response_completion_profile));
fields(openai_decisions_completion_profile) ->
    without_fields([type, system_prompt, model], fields(openai_completion_profile)) ++
        [
            {type,
                mk(openai_decisions, #{
                    default => openai_decisions, required => true, desc => ?DESC(type)
                })},
            {model, mk(binary(), #{default => <<"gpt-6-luna">>, desc => ?DESC(model)})},
            {questions,
                mk(hoconsc:array(decision_question_sctype()), #{
                    required => true,
                    validator => fun validate_decision_questions/1,
                    desc => ?DESC(decision_questions)
                })}
        ];
fields(openai_decisions_completion_profile_api_get) ->
    fields(openai_decisions_completion_profile);
fields(openai_decisions_completion_profile_api_put) ->
    without_fields([name], fields(openai_decisions_completion_profile));
fields(decision_predicate) ->
    decision_question_fields(predicate);
fields(decision_choice) ->
    decision_question_fields(choice) ++
        [
            {choices,
                mk(hoconsc:array(ref(decision_choice_option)), #{
                    required => true,
                    validator => fun validate_decision_choices/1,
                    desc => ?DESC(decision_choices)
                })}
        ];
fields(decision_score) ->
    decision_question_fields(score) ++
        [
            {levels,
                mk(hoconsc:array(ref(decision_score_level)), #{
                    required => true,
                    validator => fun validate_non_empty/1,
                    desc => ?DESC(decision_levels)
                })}
        ];
fields(decision_choice_option) ->
    [
        {value,
            mk(hoconsc:union([binary(), boolean()]), #{
                required => true, desc => ?DESC(decision_choice_value)
            })},
        {description,
            mk(binary(), #{required => false, desc => ?DESC(decision_option_description)})}
    ];
fields(decision_score_level) ->
    [
        {label,
            mk(binary(), #{
                required => true,
                validator => fun validate_non_empty/1,
                desc => ?DESC(decision_level_label)
            })},
        {description,
            mk(binary(), #{required => false, desc => ?DESC(decision_option_description)})}
    ];
%%
%% Anthropic completion profile
%%
fields(anthropic_completion_profile) ->
    [
        {name,
            mk(binary(), #{
                required => true,
                desc => ?DESC(completion_profile_name),
                validator => fun validate_name/1
            })},
        {type, mk(anthropic, #{default => anthropic, required => true, desc => ?DESC(type)})},
        {provider_name, mk(binary(), #{required => true, desc => ?DESC(provider_name)})},
        {anthropic_version,
            mk(enum(['2023-06-01']), #{
                default => '2023-06-01',
                desc => ?DESC(anthropic_version),
                required => false,
                deprecated => {since, <<"6.0.0">>},
                importance => ?IMPORTANCE_HIDDEN
            })},
        {system_prompt,
            mk(binary(), #{required => false, default => <<>>, desc => ?DESC(system_prompt)})},
        {model,
            mk(binary(), #{
                required => false,
                default => <<"claude-3-5-sonnet-20240620">>,
                desc => ?DESC(model)
            })},
        {max_tokens,
            mk(pos_integer(), #{required => false, default => 100, desc => ?DESC(max_tokens)})}
    ];
fields(anthropic_completion_profile_api_get) ->
    fields(anthropic_completion_profile);
fields(anthropic_completion_profile_api_put) ->
    without_fields([name], fields(anthropic_completion_profile));
fields(transport_options) ->
    [
        {connect_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                required => false,
                default => <<"1s">>,
                desc => ?DESC(connect_timeout),
                importance => ?IMPORTANCE_LOW
            })},
        {recv_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                required => false,
                default => <<"5s">>,
                desc => ?DESC(recv_timeout),
                importance => ?IMPORTANCE_LOW
            })},
        {checkout_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                required => false,
                default => <<"1s">>,
                desc => ?DESC(checkout_timeout),
                importance => ?IMPORTANCE_LOW
            })},
        {max_connections,
            mk(pos_integer(), #{
                required => false,
                default => 50,
                desc => ?DESC(max_connections),
                importance => ?IMPORTANCE_LOW
            })}
    ].

base_provider_fields() ->
    [
        {name,
            mk(binary(), #{
                required => true,
                desc => ?DESC(provider_name),
                validator => fun validate_name/1
            })},
        {type,
            mk(hoconsc:enum([openai, openai_response, openai_decisions, anthropic]), #{
                default => openai, required => true, desc => ?DESC(type)
            })},
        {api_key, emqx_schema_secret:mk(#{required => true, desc => ?DESC(api_key)})},
        {transport_options,
            mk(ref(transport_options), #{
                default => #{},
                desc => ?DESC(transport_options),
                importance => ?IMPORTANCE_LOW
            })}
    ].

desc(ai) ->
    ?DESC(ai);
desc(openai_provider) ->
    ?DESC(openai_provider);
desc(openai_response_provider) ->
    ?DESC(openai_response_provider);
desc(openai_decisions_provider) ->
    ?DESC(openai_decisions_provider);
desc(anthropic_provider) ->
    ?DESC(anthropic_provider);
desc(openai_completion_profile) ->
    ?DESC(openai_completion_profile);
desc(openai_response_completion_profile) ->
    ?DESC(openai_response_completion_profile);
desc(openai_decisions_completion_profile) ->
    ?DESC(openai_decisions_completion_profile);
desc(decision_predicate) ->
    ?DESC(decision_predicate);
desc(decision_choice) ->
    ?DESC(decision_choice);
desc(decision_score) ->
    ?DESC(decision_score);
desc(decision_choice_option) ->
    ?DESC(decision_choice_option);
desc(decision_score_level) ->
    ?DESC(decision_score_level);
desc(anthropic_completion_profile) ->
    ?DESC(anthropic_completion_profile);
desc(transport_options) ->
    ?DESC(transport_options);
desc(_) ->
    undefined.

completion_profile_sctype() ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_completion_profile),
            <<"openai_response">> => ref(openai_response_completion_profile),
            <<"openai_decisions">> => ref(openai_decisions_completion_profile),
            <<"anthropic">> => ref(anthropic_completion_profile)
        },
        <<"openai">>
    ).

completion_profile_sctype_api(get) ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_completion_profile_api_get),
            <<"openai_response">> => ref(openai_response_completion_profile_api_get),
            <<"openai_decisions">> => ref(openai_decisions_completion_profile_api_get),
            <<"anthropic">> => ref(anthropic_completion_profile_api_get)
        },
        <<"openai">>
    );
completion_profile_sctype_api(put) ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_completion_profile_api_put),
            <<"openai_response">> => ref(openai_response_completion_profile_api_put),
            <<"openai_decisions">> => ref(openai_decisions_completion_profile_api_put),
            <<"anthropic">> => ref(anthropic_completion_profile_api_put)
        },
        <<"openai">>
    );
completion_profile_sctype_api(post) ->
    completion_profile_sctype().

provider_sctype() ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_provider),
            <<"openai_response">> => ref(openai_response_provider),
            <<"openai_decisions">> => ref(openai_decisions_provider),
            <<"anthropic">> => ref(anthropic_provider)
        },
        <<"openai">>
    ).

provider_sctype_api(put) ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_provider_api_put),
            <<"openai_response">> => ref(openai_response_provider_api_put),
            <<"openai_decisions">> => ref(openai_decisions_provider_api_put),
            <<"anthropic">> => ref(anthropic_provider_api_put)
        },
        <<"openai">>
    );
provider_sctype_api(get) ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_provider_api_get),
            <<"openai_response">> => ref(openai_response_provider_api_get),
            <<"openai_decisions">> => ref(openai_decisions_provider_api_get),
            <<"anthropic">> => ref(anthropic_provider_api_get)
        },
        <<"openai">>
    );
provider_sctype_api(post) ->
    provider_sctype().

check_provider(ProviderRaw) ->
    Options = #{atom_key => true},
    Schema = #{roots => [{provider, mk(provider_sctype_api(put), #{})}]},
    try
        #{provider := Provider} = hocon_tconf:check_plain(
            Schema, #{<<"provider">> => ProviderRaw}, Options
        ),
        {ok, Provider}
    catch
        throw:Error ->
            {error, {invalid_provider, Error}}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

decision_question_sctype() ->
    emqx_schema:mkunion(type, #{
        <<"predicate">> => ref(decision_predicate),
        <<"choice">> => ref(decision_choice),
        <<"score">> => ref(decision_score)
    }).

decision_question_fields(Type) ->
    [
        {type, mk(Type, #{required => true, desc => ?DESC(decision_question_type)})},
        {name,
            mk(binary(), #{
                required => true,
                validator => fun validate_non_empty/1,
                desc => ?DESC(decision_question_name)
            })},
        {instructions,
            mk(binary(), #{
                required => true,
                validator => fun validate_non_empty/1,
                desc => ?DESC(decision_instructions)
            })}
    ].

validate_decision_questions(Questions) ->
    validate_unique_values(<<"name">>, Questions).

validate_decision_choices(Choices) ->
    validate_unique_values(<<"value">>, Choices).

validate_unique_values(Key, Items) ->
    maybe
        ok ?= validate_non_empty(Items),
        Values = [Value || #{Key := Value} <- emqx_utils_maps:binary_key_map(Items)],
        case length(Values) =:= length(lists:usort(Values)) of
            true -> ok;
            false -> {error, {duplicate_values, Key}}
        end
    end.

validate_non_empty([]) ->
    {error, must_not_be_empty};
validate_non_empty(<<>>) ->
    {error, must_not_be_empty};
validate_non_empty(_) ->
    ok.

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).
enum(Values) -> hoconsc:enum(Values).

without_fields(FieldNames, Fields) ->
    lists:filter(
        fun({Name, _}) ->
            not lists:member(Name, FieldNames)
        end,
        Fields
    ).

validate_name(Name) ->
    emqx_resource:validate_name(Name).

validate_url(URL) ->
    case uri_string:parse(URL) of
        #{scheme := <<"http">>} ->
            ok;
        #{scheme := <<"https">>} ->
            ok;
        _ ->
            throw("bad_base_url")
    end.
