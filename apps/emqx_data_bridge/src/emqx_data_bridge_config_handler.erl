-module(emqx_data_bridge_config_handler).

-behaviour(gen_server).

%% API functions
-export([ start_link/0
        , notify_updated/0
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

notify_updated() ->
    gen_server:cast(?MODULE, updated).

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(updated, State) ->
    Configs = [format_conf(Data) || Data <- emqx_data_bridge:list_bridges()],
    emqx_config_handler ! {emqx_data_bridge, Configs},
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%============================================================================

format_conf(#{resource_type := Type, id := Id, config := Conf}) ->
    #{type => Type, name => emqx_data_bridge:resource_id_to_name(Id),
      config => Conf}.
