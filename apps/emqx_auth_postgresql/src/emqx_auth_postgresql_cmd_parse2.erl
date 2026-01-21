%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_postgresql_cmd_parse2).

-moduledoc """
epgsql_cmd_parse2 command with additional sync
""".

-include_lib("epgsql/include/protocol.hrl").

-record(state, {
    parse2_state :: term(),
    parse2_result ::
        {finish, _Result :: any(), _Notification :: any()} | {error, _Error :: any()} | undefined
}).

-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).

init({Name, Sql, Types}) ->
    #state{
        parse2_state = epgsql_cmd_parse2:init({Name, Sql, Types}),
        parse2_result = undefined
    }.

execute(Sock0, #state{parse2_state = Parse2State0} = State0) ->
    Sock1 = epgsql_sock:set_attr(sync_required, false, Sock0),
    {send_multi, Commands, Sock, Parse2State} = epgsql_cmd_parse2:execute(Sock1, Parse2State0),
    {send_multi, Commands ++ [epgsql_wire:encode_sync()], Sock, State0#state{
        parse2_state = Parse2State
    }}.

handle_message(?READY_FOR_QUERY, _, Sock, #state{
    parse2_state = undefined, parse2_result = {finish, Result, Notification}
}) ->
    {finish, Result, Notification, Sock};
handle_message(?READY_FOR_QUERY, _, Sock, #state{
    parse2_state = undefined, parse2_result = {error, _} = Error
}) ->
    {finish, Error, done, Sock};
handle_message(?ERROR, Error, Sock, #state{} = State) ->
    %% Delay till we get ?READY_FOR_QUERY from sync
    {noaction, Sock, State#state{parse2_result = {error, Error}, parse2_state = undefined}};
handle_message(Type, Payload, Sock0, #state{parse2_state = Parse2State0} = State) ->
    case epgsql_cmd_parse2:handle_message(Type, Payload, Sock0, Parse2State0) of
        {noaction, Sock} ->
            {noaction, Sock};
        {noaction, Sock, Parse2State} ->
            {noaction, Sock, State#state{parse2_state = Parse2State}};
        {finish, Result, Notification, Sock} ->
            %% Delay till we get ?READY_FOR_QUERY from sync
            {noaction, Sock, State#state{
                parse2_state = undefined, parse2_result = {finish, Result, Notification}
            }};
        unknown ->
            unknown
    end.
