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

-import(emqx_mgmt_util, [ response_schema/1
                        , response_schema/2
                        , response_error_schema/2
                        , response_page_schema/1
                        , request_body_schema/1
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

delayed_schema() ->
    delayed_schema(false).

delayed_schema(WithPayload) ->
    case WithPayload of
        true ->
            #{
                type => object,
                properties => delayed_message_properties()
            };
        _ ->
            #{
                type => object,
                properties => maps:without([payload], delayed_message_properties())
            }
    end.

delayed_message_properties() ->
    PayloadDesc = list_to_binary(
        io_lib:format("Payload, base64 encode. Payload will be ~p if length large than ~p",
            [?PAYLOAD_TOO_LARGE, ?MAX_PAYLOAD_LENGTH])),
    #{
        id => #{
            type => integer,
            description => <<"Message Id (MQTT message id hash)">>},
        publish_time => #{
            type => string,
            description => <<"publish time, rfc 3339">>},
        topic => #{
            type => string,
            description => <<"Topic">>},
        qos => #{
            type => integer,
            enum => [0, 1, 2],
            description => <<"Qos">>},
        payload => #{
            type => string,
            description => PayloadDesc},
        form_clientid => #{
            type => string,
            description => <<"Client ID">>},
        form_username => #{
            type => string,
            description => <<"Username">>}
    }.

status_api() ->
    Schema = #{
        type => object,
        properties => #{
            enable => #{
                type => boolean},
            max_delayed_messages => #{
                type => integer,
                description => <<"Max limit, 0 is no limit">>}}},
    Metadata = #{
        get => #{
            description => "Get delayed status",
            responses => #{
                <<"200">> => response_schema(<<"Bad Request">>, Schema)}},
        put => #{
            description => "Enable or disable delayed, set max delayed messages",
            'requestBody' => request_body_schema(Schema),
            responses => #{
                <<"200">> =>
                    response_schema(<<"Enable or disable delayed successfully">>, Schema),
                <<"400">> =>
                    response_error_schema(<<"Already disabled or enabled">>, [?ALREADY_ENABLED, ?ALREADY_DISABLED])}}},
    {"/mqtt/delayed_messages/status", Metadata, status}.

delayed_messages_api() ->
    Metadata = #{
        get => #{
            description => "List delayed messages",
            responses => #{
                <<"200">> => response_page_schema(delayed_schema())}}},
    {"/mqtt/delayed_messages", Metadata, delayed_messages}.

delayed_message_api() ->
    Metadata = #{
        get => #{
            description => "Get delayed message",
            parameters => [#{
                name => id,
                in => path,
                schema => #{type => string},
                required => true
            }],
            responses => #{
                <<"200">> => response_schema(<<"Get delayed message success">>, delayed_schema(true)),
                <<"404">> => response_error_schema(<<"Message ID not found">>, [?MESSAGE_ID_NOT_FOUND])}},
        delete => #{
            description => "Delete delayed message",
            parameters => [#{
                name => id,
                in => path,
                schema => #{type => string},
                required => true
            }],
            responses => #{
                <<"200">> => response_schema(<<"Delete delayed message success">>)}}},
    {"/mqtt/delayed_messages/:id", Metadata, delayed_message}.

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------
status(get, _Request) ->
    {200, get_status()};

status(put, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Config = emqx_json:decode(Body, [return_maps]),
    update_config(Config).

delayed_messages(get, Request) ->
    Qs = cowboy_req:parse_qs(Request),
    {200, emqx_delayed:list(Qs)}.

delayed_message(get, Request) ->
    Id = cowboy_req:binding(id, Request),
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
            Message = list_to_binary(io_lib:format("Message ID ~p not found", [Id])),
            {404, #{code => ?MESSAGE_ID_NOT_FOUND, message => Message}}
    end;
delayed_message(delete, Request) ->
    Id = cowboy_req:binding(id, Request),
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
