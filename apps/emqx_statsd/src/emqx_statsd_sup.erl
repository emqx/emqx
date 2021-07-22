%%%-------------------------------------------------------------------
%% @doc emqx_statsd top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(emqx_statsd_sup).

-behaviour(supervisor).

-include("emqx_statsd.hrl").

-export([start_link/0]).

-export([init/1]).


 start_link() ->
     supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Opts = emqx_config:get([?APP], #{}),
    {ok, {{one_for_one, 10, 3600},
          [#{id       => emqx_statsd,
             start    => {emqx_statsd, start_link, [Opts]},
             restart  => permanent,
             shutdown => 5000,
             type     => worker,
             modules  => [emqx_statsd]}]}}.



