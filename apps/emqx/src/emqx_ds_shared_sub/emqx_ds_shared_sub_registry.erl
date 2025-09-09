%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_registry).

-include("emqx_ds_shared_sub_proto.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% API
-export([
    get_leader_sync/2,
    leader_wanted/2,
    start_local/2
]).

%% Internal exports:
-export([
    start_link/0
]).

-ifdef(TEST).
-export([
    purge/0
]).
-endif.

-behaviour(supervisor).
-export([init/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec get_leader_sync(emqx_types:share(), emqx_ds_shared_sub:options()) ->
    {ok, pid()} | emqx_ds:error(_).
get_leader_sync(ShareTopic, Options) ->
    emqx_ds_shared_sub_leader:wait_leader(ensure_local(ShareTopic, Options)).

-spec get_leader_nowait(emqx_types:share()) -> {ok, pid()} | undefined.
get_leader_nowait(Share) ->
    emqx_ds_shared_sub_leader:whereis_leader(Share).

-spec leader_wanted(
    emqx_ds_shared_sub_proto:borrower_id(),
    emqx_types:share()
) -> ok | emqx_ds:error(_).
leader_wanted(BorrowerId, ShareTopic) ->
    maybe
        %% FIXME: do it async. Tests should expect that though
        {ok, Pid} ?= get_leader_nowait(ShareTopic),
        emqx_ds_shared_sub_proto:send_to_leader(Pid, ?borrower_connect(BorrowerId, ShareTopic))
    end,
    ok.

-spec ensure_local(emqx_types:share(), emqx_ds_shared_sub:options()) ->
    pid().
ensure_local(ShareTopic, Options) ->
    case start_local(ShareTopic, Options) of
        {ok, Pid} ->
            Pid;
        {error, {already_started, Pid}} when is_pid(Pid) ->
            Pid
    end.

-spec start_local(emqx_types:share(), emqx_ds_shared_sub:options()) ->
    supervisor:startchild_ret().
start_local(ShareTopic, Options) ->
    supervisor:start_child(?MODULE, [ShareTopic, Options]).

%%------------------------------------------------------------------------------

-ifdef(TEST).
-doc """
Permanently destroy **ALL** shared groups.
""".
-spec purge() -> ok.
purge() ->
    Go = fun
        Go('$end_of_table') ->
            ok;
        Go(It0) ->
            {Items, It} = emqx_ds_shared_sub:list(It0, 100),
            lists:foreach(
                fun(#{id := Id}) ->
                    emqx_ds_shared_sub:destroy(Id)
                end,
                Items
            ),
            Go(It)
    end,
    Go(undefined).
-endif.

%%------------------------------------------------------------------------------
%% supervisor behaviour callbacks
%%------------------------------------------------------------------------------

init([]) ->
    Children = [
        #{
            id => worker,
            start => {emqx_ds_shared_sub_leader, start_link, []},
            shutdown => 5_000,
            type => worker,
            restart => transient
        }
    ],
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 100,
        period => 1
    },
    {ok, {SupFlags, Children}}.
