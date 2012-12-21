-module(emqtt_client_sup).

-export([start_link/0, start_client/1]).

-behaviour(supervisor2).

-export([init/1]).

start_link() ->
	supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one_terminate, 0, 1},
          [{client, {emqtt_client, start_link, []}, 
				temporary, 5000, worker, [emqtt_client]}]}}.

start_client(Sock) ->
    {ok, Client} = supervisor:start_child(?MODULE, []),
	ok = gen_tcp:controlling_process(Sock, Client),
	emqtt_client:go(Client, Sock),

    %% see comment in rabbit_networking:start_client/2
    gen_event:which_handlers(error_logger),

	Client.

