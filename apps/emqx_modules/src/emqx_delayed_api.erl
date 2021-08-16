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
                        , request_body_schema/1
                        ]).

% -export([cli/1]).

-export([ status/2
        , delayed_messages/2
        , delete_delayed_message/2
        ]).

-export([enable_delayed/2]).

-export([api_spec/0]).

api_spec() ->
    {[status(), delayed_messages(), delete_delayed_message()],
     [delayed_message_schema()]}.


delayed_message_schema() ->
    #{broker_info => #{
        type => object,
        properties => #{
            msgid => #{
                type => string,
                description => <<"Message Id">>
            }
        }
    }}.

status() ->
    Metadata = #{
        get => #{
            description => "Get delayed status",
            responses => #{
                <<"200">> => response_schema(<<"Bad Request">>,
                    #{
                        type => object,
                        properties => #{enable => #{type => boolean}}
                    }
                )
            }
        },
        put => #{
            description => "Enable or disbale delayed",
            'requestBody' => request_body_schema(#{
                type => object,
                properties => #{
                    enable => #{
                        type => boolean
                    }
                }
            }),
            responses => #{
                <<"200">> =>
                    response_schema(<<"Enable or disbale delayed successfully">>),
                <<"400">> =>
                    response_schema(<<"Bad Request">>,
                        #{
                            type => object,
                            properties => #{
                                message => #{type => string},
                                code => #{type => string}
                            }
                        }
                    )
            }
        }
    },
    {"/delayed/status", Metadata, status}.

delayed_messages() ->
    Metadata = #{
        get => #{
            description => "Get delayed message list",
            responses => #{
                <<"200">> => emqx_mgmt_util:response_array_schema(<<>>, delayed_message)
            }
        }
    },
    {"/delayed/messages", Metadata, delayed_messages}.

delete_delayed_message() ->
    Metadata = #{
        delete => #{
            description => "Delete delayed message",
            parameters => [#{
                name => msgid,
                in => path,
                schema => #{type => string},
                required => true
            }],
            responses => #{
                <<"200">> => response_schema(<<"Bad Request">>,
                    #{
                        type => object,
                        properties => #{enable => #{type => boolean}}
                    }
                )
            }
        }
    },
    {"/delayed/messages/:msgid", Metadata, delete_delayed_message}.


%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------
status(get, _Request) ->
    {200, get_status()};

status(put, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    Enable = maps:get(<<"enable">>, Params),
    case Enable =:= get_status() of
        true ->
            Reason = case Enable of
                true -> <<"Telemetry status is already enabled">>;
                false -> <<"Telemetry status is already disable">>
            end,
            {400, #{code => "BAD_REQUEST", message => Reason}};
        false ->
            enable_delayed(Enable),
             {200}
    end.

delayed_messages(get, _Request) ->
    {200, []}.

delete_delayed_message(delete, _Request) ->
    {200}.

%%--------------------------------------------------------------------
%% internal function
%%--------------------------------------------------------------------
enable_delayed(Enable) ->
    lists:foreach(fun(Node) ->
        enable_delayed(Node, Enable)
    end, ekka_mnesia:running_nodes()).

enable_delayed(Node, Enable) when Node =:= node() ->
    case Enable of
        true ->
            emqx_delayed:enable();
        false ->
            emqx_delayed:disable()
    end;

enable_delayed(Node, Enable) ->
    rpc_call(Node, ?MODULE, enable_delayed, [Node, Enable]).

rpc_call(Node, Module, Fun, Args) ->
    case rpc:call(Node, Module, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Result -> Result
    end.

get_status() ->
    emqx_config:get([delayed, enable], true).
