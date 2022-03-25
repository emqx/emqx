%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_license_installer).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-export([
    start_link/1,
    start_link/4
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-define(NAME, emqx).
-define(INTERVAL, 5000).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Callback) ->
    start_link(?NAME, ?MODULE, ?INTERVAL, Callback).

start_link(Name, ServerName, Interval, Callback) ->
    gen_server:start_link({local, ServerName}, ?MODULE, [Name, Interval, Callback], []).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Name, Interval, Callback]) ->
    Pid = whereis(Name),
    State = #{
        interval => Interval,
        name => Name,
        pid => Pid,
        callback => Callback
    },
    {ok, ensure_timer(State)}.

handle_call(_Req, _From, State) ->
    {reply, unknown, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, Timer, check_pid}, #{timer := Timer} = State) ->
    NewState = check_pid(State),
    {noreply, ensure_timer(NewState)};
handle_info(_Msg, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------

ensure_timer(#{interval := Interval} = State) ->
    _ =
        case State of
            #{timer := Timer} -> erlang:cancel_timer(Timer);
            _ -> ok
        end,
    State#{timer => erlang:start_timer(Interval, self(), check_pid)}.

check_pid(#{name := Name, pid := OldPid, callback := Callback} = State) ->
    case whereis(Name) of
        undefined ->
            ?tp(debug, emqx_license_installer_noproc, #{pid => OldPid}),
            State;
        OldPid ->
            ?tp(debug, emqx_license_installer_nochange, #{pid => OldPid}),
            State;
        NewPid ->
            _ = Callback(),
            ?tp(debug, emqx_license_installer_called, #{pid => OldPid}),
            State#{pid => NewPid}
    end.
