%%-----------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------

-module(emqtt_app).

-author('feng@emqtt.io').

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(PRINT_MSG(Msg), io:format(Msg)).

-define(PRINT(Format, Args), io:format(Format, Args)).

%% ===================================================================
%% Application callbacks
%% ===================================================================

%%
%% @spec start(atom(), list()) -> {ok, pid()}
%%
start(_StartType, _StartArgs) ->
	print_banner(),
    {ok, Sup} = emqtt_sup:start_link(),
	start_servers(Sup),
	{ok, Listeners} = application:get_env(listen),
    emqtt:open(Listeners),
	register(emqtt, self()),
    print_vsn(),
	{ok, Sup}.

print_banner() ->
	?PRINT("starting emqtt on node '~s'~n", [node()]).

print_vsn() ->
	{ok, Vsn} = application:get_key(vsn),
	{ok, Desc} = application:get_key(description),
	?PRINT("~s ~s is running now~n", [Desc, Vsn]).

start_servers(Sup) ->
    {ok, SessOpts} = application:get_env(session),
    {ok, RetainOpts} = application:get_env(retain),
	lists:foreach(
        fun({Name, F}) when is_function(F) ->
			?PRINT("~s is starting...", [Name]),
            F(),
			?PRINT_MSG("[done]~n");
		   ({Name, Server}) ->
			?PRINT("~s is starting...", [Name]),
			start_child(Sup, Server),
			?PRINT_MSG("[done]~n");
           ({Name, Server, Opts}) ->
			?PRINT("~s is starting...", [ Name]),
			start_child(Sup, Server, Opts),
			?PRINT_MSG("[done]~n")
		end,
	 	[{"emqtt config", emqtt_config},
		 {"emqtt server", emqtt_server, RetainOpts},
         {"emqtt client manager", emqtt_cm},
         {"emqtt session manager", emqtt_sm},
         {"emqtt session supervisor", {supervisor, emqtt_session_sup}, SessOpts},
         {"emqtt auth", emqtt_auth},
		 {"emqtt pubsub", emqtt_pubsub},
		 {"emqtt router", emqtt_router},
		 {"emqtt monitor", emqtt_monitor}
		]).

start_child(Sup, {supervisor, Name}) ->
    supervisor:start_child(Sup, supervisor_spec(Name));
start_child(Sup, Name) when is_atom(Name) ->
    {ok, _ChiId} = supervisor:start_child(Sup, worker_spec(Name)).

start_child(Sup, {supervisor, Name}, Opts) ->
    supervisor:start_child(Sup, supervisor_spec(Name, Opts));
start_child(Sup, Name, Opts) when is_atom(Name) ->
    {ok, _ChiId} = supervisor:start_child(Sup, worker_spec(Name, Opts)).

%%TODO: refactor...
supervisor_spec(Name) ->
    {Name, 
        {Name, start_link, []},
            permanent, infinity, supervisor, [Name]}.

supervisor_spec(Name, Opts) ->
    {Name, 
        {Name, start_link, [Opts]},
            permanent, infinity, supervisor, [Name]}.

worker_spec(Name) ->
    {Name, 
        {Name, start_link, []}, 
            permanent, 5000, worker, [Name]}.
worker_spec(Name, Opts) -> 
    {Name, 
        {Name, start_link, [Opts]}, 
            permanent, 5000, worker, [Name]}.

%%
%% @spec stop(atom) -> 'ok'
%%
stop(_State) ->
    ok.

