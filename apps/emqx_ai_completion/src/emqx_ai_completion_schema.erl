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
    completion_profile_sctype/0,
    completion_profile_sctype_api/0,
    credential_sctype/0,
    credential_sctype_api/0
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
        {credentials,
            mk(
                hoconsc:array(credential_sctype()),
                #{
                    default => [],
                    desc => ?DESC(credentials)
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
fields(credential_api) ->
    [
        {type,
            mk(hoconsc:enum([openai, anthropic]), #{
                default => openai, required => true, desc => ?DESC(type)
            })},
        {api_key, emqx_schema_secret:mk(#{required => true, desc => ?DESC(api_key)})}
    ];
fields(credential) ->
    [name_field(credential_name)] ++ fields(credential_api);
fields(openai_completion_profile_api) ->
    [
        {type, mk(openai, #{default => openai, required => true, desc => ?DESC(type)})},
        {credential_name, mk(binary(), #{required => true, desc => ?DESC(credential_name)})},
        {system_prompt, mk(binary(), #{default => <<>>, desc => ?DESC(system_prompt)})},
        {model, mk(enum(['gpt-4o', 'gpt-4o-mini']), #{default => 'gpt-4o', desc => ?DESC(model)})}
    ];
fields(openai_completion_profile) ->
    [name_field(completion_profile_name)] ++ fields(openai_completion_profile_api);
fields(anthropic_completion_profile_api) ->
    [
        {type, mk(anthropic, #{default => anthropic, required => true, desc => ?DESC(type)})},
        {credential_name, mk(binary(), #{required => true, desc => ?DESC(credential_name)})},
        {anthropic_version,
            mk(enum(['2023-06-01']), #{default => '2023-06-01', desc => ?DESC(anthropic_version)})},
        {system_prompt, mk(binary(), #{default => <<>>, desc => ?DESC(system_prompt)})},
        {model,
            mk(enum(['claude-3-5-sonnet-20240620', 'claude-3-5-haiku-20240307']), #{
                default => 'claude-3-5-sonnet-20240620', desc => ?DESC(model)
            })},
        {max_tokens, mk(pos_integer(), #{default => 100, desc => ?DESC(max_tokens)})}
    ];
fields(anthropic_completion_profile) ->
    [name_field(completion_profile_name)] ++ fields(anthropic_completion_profile_api).

desc(ai) ->
    "AI functions settings.";
desc(credential_api) ->
    "AI provider credential used in HTTP API.";
desc(credential) ->
    "AI provider credential.";
desc(openai_completion_profile_api) ->
    "AI completion profile for OpenAI used in HTTP API.";
desc(openai_completion_profile) ->
    "AI completion profile for OpenAI.";
desc(anthropic_completion_profile_api) ->
    "AI completion profile for Anthropic used in HTTP API.";
desc(anthropic_completion_profile) ->
    "AI completion profile for Anthropic.";
desc(_) ->
    undefined.

name_field(Desc) ->
    {name,
        mk(binary(), #{
            default => <<>>,
            required => true,
            desc => ?DESC(Desc),
            validator => fun emqx_schema:non_empty_string/1
        })}.

completion_profile_sctype() ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_completion_profile),
            <<"anthropic">> => ref(anthropic_completion_profile)
        },
        <<"openai">>
    ).

completion_profile_sctype_api() ->
    emqx_schema:mkunion(
        type,
        #{
            <<"openai">> => ref(openai_completion_profile_api),
            <<"anthropic">> => ref(anthropic_completion_profile_api)
        },
        <<"openai">>
    ).

credential_sctype() ->
    ref(credential).

credential_sctype_api() ->
    ref(credential_api).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).
enum(Values) -> hoconsc:enum(Values).
