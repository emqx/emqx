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

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/1, ref/2]).

-define(MAX_PAYLOAD_LENGTH, 2048).
-define(PAYLOAD_TOO_LARGE, 'PAYLOAD_TOO_LARGE').

-export([status/2
    , delayed_messages/2
    , delayed_message/2
]).

-export([paths/0, fields/1, schema/1]).

%% for rpc
-export([update_config_/1]).

-export([api_spec/0]).

-define(ALREADY_ENABLED, 'ALREADY_ENABLED').
-define(ALREADY_DISABLED, 'ALREADY_DISABLED').

-define(BAD_REQUEST, 'BAD_REQUEST').

-define(MESSAGE_ID_NOT_FOUND, 'MESSAGE_ID_NOT_FOUND').
-define(MESSAGE_ID_SCHEMA_ERROR, 'MESSAGE_ID_SCHEMA_ERROR').

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE).

paths() -> ["/mqtt/delayed", "/mqtt/delayed/messages", "/mqtt/delayed/messages/:msgid"].

schema("/mqtt/delayed") ->
    #{
        'operationId' => status,
        get => #{
            tags => [<<"mqtt">>],
            description => <<"Get delayed status">>,
            summary => <<"Get delayed status">>,
            responses => #{
                200 => ref(emqx_modules_schema, "delayed")
            }
        },
        put => #{
            tags => [<<"mqtt">>],
            description => <<"Enable or disable delayed, set max delayed messages">>,
            'requestBody' => ref(emqx_modules_schema, "delayed"),
            responses => #{
                200 => mk(ref(emqx_modules_schema, "delayed"),
                    #{desc => <<"Enable or disable delayed successfully">>}),
                400 => emqx_dashboard_swagger:error_codes( [?BAD_REQUEST]
                                                         , <<"Max limit illegality">>)
            }
        }
    };

schema("/mqtt/delayed/messages/:msgid") ->
    #{'operationId' => delayed_message,
        get => #{
            tags => [<<"mqtt">>],
            description => <<"Get delayed message">>,
            parameters => [{msgid, mk(binary(), #{in => path, desc => <<"delay message ID">>})}],
            responses => #{
                200 => ref("message_without_payload"),
                400 => emqx_dashboard_swagger:error_codes( [?MESSAGE_ID_SCHEMA_ERROR]
                                                         , <<"Bad MsgId format">>),
                404 => emqx_dashboard_swagger:error_codes( [?MESSAGE_ID_NOT_FOUND]
                                                         , <<"MsgId not found">>)
            }
        },
        delete => #{
            tags => [<<"mqtt">>],
            description => <<"Delete delayed message">>,
            parameters => [{msgid, mk(binary(), #{in => path, desc => <<"delay message ID">>})}],
            responses => #{
                204 => <<"Delete delayed message success">>,
                400 => emqx_dashboard_swagger:error_codes( [?MESSAGE_ID_SCHEMA_ERROR]
                                                         , <<"Bad MsgId format">>),
                404 => emqx_dashboard_swagger:error_codes( [?MESSAGE_ID_NOT_FOUND]
                                                         , <<"MsgId not found">>)
            }
        }
    };
schema("/mqtt/delayed/messages") ->
    #{
        'operationId' => delayed_messages,
        get => #{
            tags => [<<"mqtt">>],
            description => <<"List delayed messages">>,
            parameters => [ref(emqx_dashboard_swagger, page), ref(emqx_dashboard_swagger, limit)],
            responses => #{
                200 =>
                [
                    {data, mk(hoconsc:array(ref("message")), #{})},
                    {meta, [
                        {page, mk(integer(), #{})},
                        {limit, mk(integer(), #{})},
                        {count, mk(integer(), #{})}
                    ]}
                ]
            }
        }
    }.

fields("message_without_payload") ->
    [
        {msgid, mk(integer(), #{desc => <<"Message Id (MQTT message id hash)">>})},
        {publish_at, mk(binary(), #{desc => <<"Client publish message time, rfc 3339">>})},
        {delayed_interval, mk(integer(), #{desc => <<"Delayed interval, second">>})},
        {delayed_remaining, mk(integer(), #{desc => <<"Delayed remaining, second">>})},
        {expected_at, mk(binary(), #{desc => <<"Expect publish time, rfc 3339">>})},
        {topic, mk(binary(), #{desc => <<"Topic">>, example => <<"/sys/#">>})},
        {qos, mk(binary(), #{desc => <<"QoS">>})},
        {from_clientid, mk(binary(), #{desc => <<"From ClientId">>})},
        {from_username, mk(binary(), #{desc => <<"From Username">>})}
    ];
fields("message") ->
    PayloadDesc = io_lib:format(
                    "Payload, base64 encode. Payload will be ~p if length large than ~p",
        [?PAYLOAD_TOO_LARGE, ?MAX_PAYLOAD_LENGTH]),
    fields("message_without_payload") ++
    [{payload, mk(binary(), #{desc => iolist_to_binary(PayloadDesc)})}].

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
                    {200, Message#{payload => Payload}}
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
            {204};
        {error, id_schema_error} ->
            {400, generate_http_code_map(id_schema_error, Id)};
        {error, not_found} ->
            {404, generate_http_code_map(not_found, Id)}
    end.

%%--------------------------------------------------------------------
%% internal function
%%--------------------------------------------------------------------
get_status() ->
    emqx_conf:get([delayed], #{}).

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
                  end, mria_mnesia:running_nodes()).

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
    #{code => ?MESSAGE_ID_SCHEMA_ERROR, message =>
          iolist_to_binary(io_lib:format("Message ID ~p schema error", [Id]))};
generate_http_code_map(not_found, Id) ->
    #{code => ?MESSAGE_ID_NOT_FOUND, message =>
          iolist_to_binary(io_lib:format("Message ID ~p not found", [Id]))}.

rpc_call(Node, Module, Fun, Args) ->
    case rpc:call(Node, Module, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Result -> Result
    end.
