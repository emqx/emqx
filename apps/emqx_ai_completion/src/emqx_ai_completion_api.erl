%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ai_completion_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

%% API callbacks
-export([
    '/ai/credentials'/2,
    '/ai/credentials/:name'/2,
    '/ai/completion_profiles'/2,
    '/ai/completion_profiles/:name'/2
]).

-define(TAGS, [<<"AI Completion">>]).

namespace() -> "ai_completion".

%%--------------------------------------------------------------------
%% Minirest
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/ai/credentials",
        "/ai/credentials/:name",
        "/ai/completion_profiles",
        "/ai/completion_profiles/:name"
    ].

schema("/ai/credentials") ->
    #{
        'operationId' => '/ai/credentials',
        get => #{
            tags => ?TAGS,
            summary => <<"List all AI credentials">>,
            description => ?DESC(ai_credentials_list),
            parameters => [],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(emqx_ai_completion_schema:credential_sctype()),
                    [get_credential_example()]
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Create AI credential">>,
            description => ?DESC(ai_credentials_create),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_ai_completion_schema:credential_sctype(),
                post_credential_example()
            ),
            responses => #{
                204 => <<"Operation success">>,
                400 => emqx_dashboard_swagger:error_codes(
                    ['INVALID_CREDENTIAL'], <<"Invalid credential">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        }
    };
schema("/ai/credentials/:name") ->
    #{
        'operationId' => '/ai/credentials/:name',
        put => #{
            tags => ?TAGS,
            summary => <<"Update AI credential">>,
            description => ?DESC(ai_credentials_update),
            parameters => [name_param()],
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_ai_completion_schema:credential_sctype_api(),
                put_credential_example()
            ),
            responses => #{
                204 => <<"Operation success">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], <<"Credential not found">>
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['INVALID_CREDENTIAL'], <<"Invalid credential">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete AI credential">>,
            description => ?DESC(ai_credentials_delete),
            parameters => [name_param()],
            responses => #{
                204 => <<"Operation success">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], <<"Credential not found">>
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['INVALID_CREDENTIAL'], <<"Invalid request">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        }
    };
schema("/ai/completion_profiles") ->
    #{
        'operationId' => '/ai/completion_profiles',
        get => #{
            tags => ?TAGS,
            summary => <<"List all AI completion profiles">>,
            description => ?DESC(ai_completion_profiles_list),
            parameters => [],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    emqx_ai_completion_schema:completion_profile_sctype(),
                    [get_completion_profile_example()]
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Create AI completion profile">>,
            description => ?DESC(ai_completion_profiles_create),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_ai_completion_schema:completion_profile_sctype(),
                post_completion_profile_example()
            ),
            responses => #{
                204 => <<"Operation success">>,
                400 => emqx_dashboard_swagger:error_codes(
                    ['INVALID_COMPLETION_PROFILE'], <<"Invalid completion profile">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        }
    };
schema("/ai/completion_profiles/:name") ->
    #{
        'operationId' => '/ai/completion_profiles/:name',
        put => #{
            tags => ?TAGS,
            summary => <<"Update AI completion profile">>,
            description => ?DESC(ai_completion_profiles_update),
            parameters => [name_param()],
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                emqx_ai_completion_schema:completion_profile_sctype_api(),
                put_completion_profile_example()
            ),
            responses => #{
                204 => <<"Operation success">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], <<"Completion profile not found">>
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['INVALID_COMPLETION_PROFILE'], <<"Invalid completion profile">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete AI completion profile">>,
            description => ?DESC(ai_completion_profiles_delete),
            parameters => [name_param()],
            responses => #{
                204 => <<"Operation success">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'], <<"Completion profile not found">>
                ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['INVALID_COMPLETION_PROFILE'], <<"Invalid request">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Service unavailable">>
                )
            }
        }
    }.

%%--------------------------------------------------------------------
%% Schema
%%--------------------------------------------------------------------

name_param() ->
    {name,
        hoconsc:mk(binary(), #{
            default => <<>>,
            required => true,
            desc => ?DESC(name),
            validator => fun emqx_schema:non_empty_string/1,
            in => path
        })}.

put_credential_example() ->
    #{
        <<"type">> => <<"openai">>,
        <<"api_key">> => <<"sk-1234567890">>
    }.

get_credential_example() ->
    maps:merge(
        #{<<"name">> => <<"my_credential">>},
        put_credential_example()
    ).

post_credential_example() ->
    get_credential_example().

put_completion_profile_example() ->
    #{
        <<"type">> => <<"openai">>,
        <<"credential_name">> => <<"my_credential">>,
        <<"system_prompt">> => <<"You are a helpful assistant.">>,
        <<"model">> => <<"gpt-4o">>
    }.

get_completion_profile_example() ->
    maps:merge(
        #{<<"name">> => <<"my_completion_profile">>},
        put_completion_profile_example()
    ).

post_completion_profile_example() ->
    get_completion_profile_example().

%%--------------------------------------------------------------------
%% Minirest handlers
%%--------------------------------------------------------------------

'/ai/credentials'(get, _Params) ->
    {200, get_credentials()};
'/ai/credentials'(post, #{body := NewCredential}) ->
    update_credentials({add, NewCredential}).

'/ai/credentials/:name'(put, #{body := UpdatedCredential, bindings := #{name := Name}}) ->
    update_credentials({update, UpdatedCredential#{<<"name">> => Name}});
'/ai/credentials/:name'(delete, #{bindings := #{name := Name}}) ->
    update_credentials({delete, Name}).

'/ai/completion_profiles'(get, _Params) ->
    {200, get_completion_profiles()};
'/ai/completion_profiles'(post, #{body := NewCompletionProfile}) ->
    update_completion_profiles({add, NewCompletionProfile}).

'/ai/completion_profiles/:name'(put, #{
    body := UpdatedCompletionProfile, bindings := #{name := Name}
}) ->
    update_completion_profiles({update, UpdatedCompletionProfile#{<<"name">> => Name}});
'/ai/completion_profiles/:name'(delete, #{bindings := #{name := Name}}) ->
    update_completion_profiles({delete, Name}).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

get_credentials() ->
    emqx_schema:fill_defaults_for_type(
        hoconsc:array(emqx_ai_completion_schema:credential_sctype()),
        emqx_ai_completion_config:get_credentials_raw()
    ).

get_completion_profiles() ->
    emqx_schema:fill_defaults_for_type(
        hoconsc:array(emqx_ai_completion_schema:completion_profile_sctype()),
        emqx_ai_completion_config:get_completion_profiles_raw()
    ).

update_credentials(Request) ->
    wrap_update_error(emqx_ai_completion_config:update_credentials_raw(Request)).

update_completion_profiles(Request) ->
    wrap_update_error(emqx_ai_completion_config:update_completion_profiles_raw(Request)).

wrap_update_error(ok) ->
    {204};
wrap_update_error({error, #{reason := Reason}}) ->
    {Code, Message} = error_response(Reason),
    {http_code(Code), #{
        code => Code,
        message => Message
    }};
wrap_update_error({error, Reason}) ->
    {503, #{
        code => 'SERVICE_UNAVAILABLE',
        message => emqx_utils:readable_error_msg(Reason)
    }}.

error_response(duplicate_credential_name) ->
    {'INVALID_CREDENTIAL', <<"Duplicate credential name">>};
error_response(credential_in_use) ->
    {'INVALID_CREDENTIAL', <<"Credential in use">>};
error_response(completion_profile_credential_type_mismatch) ->
    {'INVALID_COMPLETION_PROFILE', <<"Completion profile type does not match credential type">>};
error_response(credential_not_found) ->
    {'NOT_FOUND', <<"Credential not found">>};
error_response(duplicate_completion_profile_name) ->
    {'INVALID_COMPLETION_PROFILE', <<"Duplicate completion profile name">>};
error_response(completion_profile_credential_not_found) ->
    {'INVALID_COMPLETION_PROFILE', <<"Completion profile credential not found">>};
error_response(completion_profile_not_found) ->
    {'NOT_FOUND', <<"Completion profile not found">>};
error_response(UnknownError) ->
    {'SERVICE_UNAVAILABLE', emqx_utils:readable_error_msg(UnknownError)}.

http_code('INVALID_CREDENTIAL') ->
    400;
http_code('INVALID_COMPLETION_PROFILE') ->
    400;
http_code('NOT_FOUND') ->
    404;
http_code('SERVICE_UNAVAILABLE') ->
    503.
