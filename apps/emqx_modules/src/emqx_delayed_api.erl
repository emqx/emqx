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

-import(emqx_mgmt_util, [ schema/1
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
        {id, integer, <<"Message Id (MQTT message id hash)">>},
        {publish_time, string, <<"publish time, rfc 3339">>},
        {topic, string, <<"Topic">>},
        {qos, string, <<"QoS">>},
        {payload, string, iolist_to_binary(PayloadDesc)},
        {form_clientid, string, <<"Form ClientId">>},
        {form_username, string, <<"Form Username">>}
    ]).

parameters() ->
    [#{
        name => id,
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
                    error_schema(<<"Already disabled or enabled">>, [?ALREADY_ENABLED, ?ALREADY_DISABLED])
            }
        }
    },
    {"/mqtt/delayed", Metadata, status}.

delayed_messages_api() ->
    Metadata = #{
        get => #{
            description => "List delayed messages",
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
                <<"200">> => object_schema(maps:without([payload], properties()), <<"Get delayed message success">>),
                <<"404">> => error_schema(<<"Message ID not found">>, [?MESSAGE_ID_NOT_FOUND])
            }
        },
        delete => #{
            description => <<"Delete delayed message">>,
            parameters => parameters(),
            responses => #{
                <<"200">> => schema(<<"Delete delayed message success">>)
            }
        }
    },
    {"/mqtt/delayed/messages/:id", Metadata, delayed_message}.

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------
status(get, _Params) ->
    {200, get_status()};

status(put, #{body := Body}) ->
    update_config(Body).

delayed_messages(get, #{query_string := Qs}) ->
    {200, emqx_delayed:list(Qs)}.

delayed_message(get, #{bindings := #{id := Id}}) ->
    case emqx_delayed:get_delayed_message(Id) of
        {ok, Message} ->
            Payload = maps:get(payload, Message),
            case size(Payload) > ?MAX_PAYLOAD_LENGTH of
                true ->
                    {200, Message#{payload => ?PAYLOAD_TOO_LARGE}};
                _ ->
                    {200, Message#{payload => base64:encode(Payload)}}
            end;
        {error, not_found} ->
            Message = iolist_to_binary(io_lib:format("Message ID ~p not found", [Id])),
            {404, #{code => ?MESSAGE_ID_NOT_FOUND, message => Message}}
    end;
delayed_message(delete, #{bindings := #{id := Id}}) ->
    _ = emqx_delayed:delete_delayed_message(Id),
    {200}.

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
    generate_config(Config, [fun generate_enable/1, fun generate_max_delayed_messages/1]).

generate_config(Config, []) ->
    {ok, Config};
generate_config(Config, [Fun | Tail]) ->
    case Fun(Config) of
        {ok, Config} ->
            generate_config(Config, Tail);
        {error, CodeMessage} ->
            {error, CodeMessage}
    end.

generate_enable(Config = #{<<"enable">> := Enable}) ->
    case {Enable =:= maps:get(enable, get_status()), Enable} of
        {true, true} ->
            {error, {?ALREADY_ENABLED, <<"Delayed message status is already enabled">>}};
        {true, false} ->
            {error, {?ALREADY_DISABLED, <<"Delayed message status is already disable">>}};
        _ ->
            {ok, Config}
    end;
generate_enable(Config) ->
    {ok, Config}.

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

rpc_call(Node, Module, Fun, Args) ->
    case rpc:call(Node, Module, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Result -> Result
    end.
