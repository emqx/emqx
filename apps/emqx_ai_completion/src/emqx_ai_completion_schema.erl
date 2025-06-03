%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
fields(provider) ->
    [
        {name,
            mk(binary(), #{
                required => true,
                desc => ?DESC(provider_name),
                validator => fun validate_name/1
            })},
        {type,
            mk(hoconsc:enum([openai, anthropic]), #{
                default => openai, required => true, desc => ?DESC(type)
            })},
        {api_key, emqx_schema_secret:mk(#{required => true, desc => ?DESC(api_key)})},
        {base_url,
            mk(binary(), #{
                required => false,
                desc => ?DESC(base_url),
                validator => fun validate_url/1,
                default => <<"https://api.openai.com/v1">>
            })},
        {transport_options,
            mk(ref(transport_options), #{
                default => #{},
                desc => ?DESC(transport_options),
                importance => ?IMPORTANCE_HIDDEN
            })}
    ];
fields(transport_options) ->
    [
        {connect_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                required => false,
                default => <<"1s">>,
                desc => ?DESC(connect_timeout),
                importance => ?IMPORTANCE_HIDDEN
            })},
        {recv_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                required => false,
                default => <<"5s">>,
                desc => ?DESC(recv_timeout),
                importance => ?IMPORTANCE_HIDDEN
            })},
        {checkout_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                required => false,
                default => <<"1s">>,
                desc => ?DESC(checkout_timeout),
                importance => ?IMPORTANCE_HIDDEN
            })}
    ];
fields(provider_api_get) ->
    fields(provider);
fields(provider_api_put) ->
    without_fields([name], fields(provider));
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
                default => '2023-06-01', desc => ?DESC(anthropic_version), required => false
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
    without_fields([name], fields(anthropic_completion_profile)).

desc(ai) ->
    ?DESC(ai);
desc(provider) ->
    ?DESC(provider);
desc(openai_completion_profile) ->
    ?DESC(openai_completion_profile);
desc(anthropic_completion_profile) ->
    ?DESC(anthropic_completion_profile);
desc(_) ->
    undefined.

completion_profile_sctype() ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_completion_profile),
            <<"anthropic">> => ref(anthropic_completion_profile)
        },
        <<"openai">>
    ).

completion_profile_sctype_api(get) ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_completion_profile_api_get),
            <<"anthropic">> => ref(anthropic_completion_profile_api_get)
        },
        <<"openai">>
    );
completion_profile_sctype_api(put) ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_completion_profile_api_put),
            <<"anthropic">> => ref(anthropic_completion_profile_api_put)
        },
        <<"openai">>
    );
completion_profile_sctype_api(post) ->
    completion_profile_sctype().

provider_sctype() ->
    ref(provider).

provider_sctype_api(put) ->
    ref(provider_api_put);
provider_sctype_api(get) ->
    ref(provider_api_get);
provider_sctype_api(post) ->
    provider_sctype().

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

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
