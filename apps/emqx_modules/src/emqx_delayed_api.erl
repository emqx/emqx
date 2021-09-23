%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_delayed_api).

-behavior(minirest_api).

-import(emqx_mgmt_util, [ page_params/0
                        , schema/1
                        , schema/2
                        , object_schema/2
                        , error_schema/2
                        , page_object_schema/1
                        , properties/1
                        ]).

-define(MAX_PAYLOAD_LENGTH, 2048).
-define(PAYLOAD_TOO_LARGE, 'PAYLOAD_TOO_LARGE').

-export([ status/2
        , delayed_messages/2
        , delayed_message/2
        ]).

%% for rpc
-export([update_config_/1]).

-export([api_spec/0]).

-define(ALREADY_ENABLED, 'ALREADY_ENABLED').
-define(ALREADY_DISABLED, 'ALREADY_DISABLED').

-define(BAD_REQUEST, 'BAD_REQUEST').

-define(MESSAGE_ID_NOT_FOUND, 'MESSAGE_ID_NOT_FOUND').
-define(MESSAGE_ID_SCHEMA_ERROR, 'MESSAGE_ID_SCHEMA_ERROR').

api_spec() ->
    {
        [status_api(), delayed_messages_api(), delayed_message_api()],
        []
    }.

conf_schema() ->
    emqx_mgmt_api_configs:gen_schema(emqx:get_raw_config([delayed])).
properties() ->
    PayloadDesc = io_lib:format("Payload, base64 encode. Payload will be ~p if length large than ~p",
            [?PAYLOAD_TOO_LARGE, ?MAX_PAYLOAD_LENGTH]),
    properties([
        {msgid, integer, <<"Message Id">>},
        {publish_at, string, <<"Client publish message time, rfc 3339">>},
        {delayed_interval, integer, <<"Delayed interval, second">>},
        {delayed_remaining, integer, <<"Delayed remaining, second">>},
        {expected_at, string, <<"Expect publish time, rfc 3339">>},
        {topic, string, <<"Topic">>},
        {qos, string, <<"QoS">>},
        {payload, string, iolist_to_binary(PayloadDesc)},
        {from_clientid, string, <<"From ClientId">>},
        {from_username, string, <<"From Username">>}
    ]).

parameters() ->
    [#{
        name => msgid,
        in => path,
        schema => #{type => string},
        required => true
    }].

status_api() ->
    Metadata = #{
        get => #{
            description => <<"Get delayed status">>,
            responses => #{
                <<"200">> => schema(conf_schema())}
            },
        put => #{
            description => <<"Enable or disable delayed, set max delayed messages">>,
            'requestBody' => schema(conf_schema()),
            responses => #{
                <<"200">> =>
                    schema(conf_schema(), <<"Enable or disable delayed successfully">>),
                <<"400">> =>
                    error_schema(<<"Max limit illegality">>, [?BAD_REQUEST])
            }
        }
    },
    {"/mqtt/delayed", Metadata, status}.

delayed_messages_api() ->
    Metadata = #{
        get => #{
            description => "List delayed messages",
            parameters => page_params(),
            responses => #{
                <<"200">> => page_object_schema(properties())
            }
        }
    },
    {"/mqtt/delayed/messages", Metadata, delayed_messages}.

delayed_message_api() ->
    Metadata = #{
        get => #{
            description => <<"Get delayed message">>,
            parameters => parameters(),
            responses => #{
                <<"400">> => error_schema(<<"Message ID Schema error">>, [?MESSAGE_ID_SCHEMA_ERROR]),
                <<"404">> => error_schema(<<"Message ID not found">>, [?MESSAGE_ID_NOT_FOUND]),
                <<"200">> => object_schema(maps:without([payload], properties()), <<"Get delayed message success">>)
            }
        },
        delete => #{
            description => <<"Delete delayed message">>,
            parameters => parameters(),
            responses => #{
                <<"400">> => error_schema(<<"Message ID Schema error">>, [?MESSAGE_ID_SCHEMA_ERROR]),
                <<"404">> => error_schema(<<"Message ID not found">>, [?MESSAGE_ID_NOT_FOUND]),
                <<"200">> => schema(<<"Delete delayed message success">>)
            }
        }
    },
    {"/mqtt/delayed/messages/:msgid", Metadata, delayed_message}.

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------
status(get, _Params) ->
    {200, get_status()};

status(put, #{body := Body}) ->
    update_config(Body).

delayed_messages(get, #{query_string := Qs}) ->
    {200, emqx_delayed:list(Qs)}.

delayed_message(get, #{bindings := #{msgid := Id}}) ->
    case emqx_delayed:get_delayed_message(Id) of
        {ok, Message} ->
            Payload = maps:get(payload, Message),
            case size(Payload) > ?MAX_PAYLOAD_LENGTH of
                true ->
                    {200, Message#{payload => ?PAYLOAD_TOO_LARGE}};
                _ ->
                    {200, Message#{payload => base64:encode(Payload)}}
            end;
        {error, id_schema_error} ->
            {400, generate_http_code_map(id_schema_error, Id)};
        {error, not_found} ->
            {404, generate_http_code_map(not_found, Id)}
    end;
delayed_message(delete, #{bindings := #{msgid := Id}}) ->
    case emqx_delayed:get_delayed_message(Id) of
        {ok, _Message} ->
            _ = emqx_delayed:delete_delayed_message(Id),
            {200};
        {error, id_schema_error} ->
            {400, generate_http_code_map(id_schema_error, Id)};
        {error, not_found} ->
            {404, generate_http_code_map(not_found, Id)}
    end.

%%--------------------------------------------------------------------
%% internal function
%%--------------------------------------------------------------------
get_status() ->
    emqx:get_config([delayed], #{}).

update_config(Config) ->
    case generate_config(Config) of
        {ok, Config} ->
            update_config_(Config),
            {200, get_status()};
        {error, {Code, Message}} ->
            {400, #{code => Code, message => Message}}
    end.
generate_config(Config) ->
    generate_config(Config, [fun generate_max_delayed_messages/1]).

generate_config(Config, []) ->
    {ok, Config};
generate_config(Config, [Fun | Tail]) ->
    case Fun(Config) of
        {ok, Config} ->
            generate_config(Config, Tail);
        {error, CodeMessage} ->
            {error, CodeMessage}
    end.

generate_max_delayed_messages(Config = #{<<"max_delayed_messages">> := Max}) when Max >= 0 ->
    {ok, Config};
generate_max_delayed_messages(#{<<"max_delayed_messages">> := Max}) when Max < 0 ->
    {error, {?BAD_REQUEST, <<"Max delayed must be equal or greater than 0">>}};
generate_max_delayed_messages(Config) ->
    {ok, Config}.

update_config_(Config) ->
    lists:foreach(fun(Node) ->
        update_config_(Node, Config)
    end, ekka_mnesia:running_nodes()).

update_config_(Node, Config) when Node =:= node() ->
    _ = emqx_delayed:update_config(Config),
    case maps:get(<<"enable">>, Config, undefined) of
        undefined ->
            ignore;
        true ->
            emqx_delayed:enable();
        false ->
            emqx_delayed:disable()
    end,
    case maps:get(<<"max_delayed_messages">>, Config, undefined) of
        undefined ->
            ignore;
        Max ->
            ok = emqx_delayed:set_max_delayed_messages(Max)
    end;

update_config_(Node, Config) ->
    rpc_call(Node, ?MODULE, ?FUNCTION_NAME, [Node, Config]).

generate_http_code_map(id_schema_error, Id) ->
    #{code => ?MESSAGE_ID_SCHEMA_ERROR, message => iolist_to_binary(io_lib:format("Message ID ~p schema error", [Id]))};
generate_http_code_map(not_found, Id) ->
    #{code => ?MESSAGE_ID_NOT_FOUND, message => iolist_to_binary(io_lib:format("Message ID ~p not found", [Id]))}.

rpc_call(Node, Module, Fun, Args) ->
    case rpc:call(Node, Module, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Result -> Result
    end.
