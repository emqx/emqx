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
                hoconsc:array(ref(credentials)),
                #{
                    default => [],
                    desc => ?DESC(credentials)
                }
            )},
        {completion_profiles,
            mk(
                hoconsc:array(
                    emqx_schema:mkunion(
                        type,
                        #{
                            <<"openai">> => ref(openai_completion_profile),
                            <<"anthropic">> => ref(anthropic_completion_profile)
                        },
                        <<"openai">>
                    )
                ),
                #{
                    default => [],
                    desc => ?DESC(ai_completion_profiles)
                }
            )}
    ];
fields(credentials) ->
    [
        {type,
            mk(hoconsc:enum([openai, anthropic]), #{
                default => openai, required => true, desc => ?DESC(type)
            })},
        {name,
            mk(binary(), #{
                default => <<>>,
                required => true,
                desc => ?DESC(name),
                validator => fun emqx_schema:non_empty_string/1
            })},
        {api_key, emqx_schema_secret:mk(#{required => true, desc => ?DESC(api_key)})}
    ];
fields(openai_completion_profile) ->
    [
        {name,
            mk(binary(), #{
                default => <<>>,
                required => true,
                desc => ?DESC(name),
                validator => fun emqx_schema:non_empty_string/1
            })},
        {type, mk(openai, #{default => openai, required => true, desc => ?DESC(type)})},
        {credential, mk(binary(), #{required => true, desc => ?DESC(credential)})},
        {system_prompt, mk(binary(), #{default => <<>>, desc => ?DESC(system_prompt)})},
        {model, mk(enum(['gpt-4o', 'gpt-4o-mini']), #{default => 'gpt-4o', desc => ?DESC(model)})}
    ];
fields(anthropic_completion_profile) ->
    [
        {name,
            mk(binary(), #{
                default => <<>>,
                required => true,
                desc => ?DESC(name),
                validator => fun emqx_schema:non_empty_string/1
            })},
        {type, mk(anthropic, #{default => anthropic, required => true, desc => ?DESC(type)})},
        {credential, mk(binary(), #{required => true, desc => ?DESC(credential)})},
        {anthropic_version,
            mk(enum(['2023-06-01']), #{default => '2023-06-01', desc => ?DESC(anthropic_version)})},
        {system_prompt, mk(binary(), #{default => <<>>, desc => ?DESC(system_prompt)})},
        {model,
            mk(enum(['claude-3-5-sonnet-20240620', 'claude-3-5-haiku-20240307']), #{
                default => 'claude-3-5-sonnet-20240620', desc => ?DESC(model)
            })},
        {max_tokens, mk(pos_integer(), #{default => 100, desc => ?DESC(max_tokens)})}
    ].

desc(credentials) ->
    ?DESC("ai_credentials");
desc(openai_completion_profile) ->
    ?DESC("ai_completion_profile_openai");
desc(anthropic_completion_profile) ->
    ?DESC("ai_completion_profile_anthropic");
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Name) -> hoconsc:ref(?MODULE, Name).
enum(Values) -> hoconsc:enum(Values).
