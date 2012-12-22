-module(emqtt_auth).

-include("emqtt.hrl").

-export([start_link/0,
		add/2,
		check/2,
		delete/1]).

-behavior(gen_server).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3]).

-record(state, {authmod, authopts}).

start_link() ->
	gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

check(Username, Password) ->
	gen_server2:call(?MODULE, {check, Username, Password}).

add(Username, Password) when is_binary(Username) ->
	gen_server2:call(?MODULE, {add, Username, Password}).

delete(Username) when is_binary(Username) ->
	gen_server2:cast(?MODULE, {delete, Username}).

init([]) ->
	{ok, {Name, Opts}} = application:get_env(auth),
	AuthMod = authmod(Name),
	ok = AuthMod:init(Opts),
	?INFO("authmod is ~p", [AuthMod]),
	?INFO("~p is started", [?MODULE]),
	{ok, #state{authmod=AuthMod, authopts=Opts}}.

authmod(Name) when is_atom(Name) ->
	list_to_atom(lists:concat(["emqtt_auth_", Name])).

handle_call({check, Username, Password}, _From, #state{authmod=AuthMod} = State) ->
	{reply, AuthMod:check(Username, Password), State};

handle_call({add, Username, Password}, _From, #state{authmod=AuthMod} = State) ->
	{reply, AuthMod:add(Username, Password), State};

handle_call(Req, _From, State) ->
	{stop, {badreq, Req}, State}.

handle_cast({delete, Username}, #state{authmod=AuthMod} = State) ->
	AuthMod:delete(Username),
	{noreply, State};

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
