%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_registry).

-include("emqx_ds_shared_sub_proto.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% API
-export([
    get_leader_sync/2,
    leader_wanted/2,
    start_local/2,
    stop_local/1,
    list_local/0
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

-doc """
If the leader is already present, send it a borrower connect request,
otherwise trigger leader election. In the latter case the message is
NOT sent, and borrower should retry.

Note: delivery is async, so the borrower should not treat return value
`ok` as a delivery guarantee.
""".
-spec leader_wanted(
    emqx_ds_shared_sub_proto:borrower_id(),
    emqx_types:share()
) -> ok | retry.
leader_wanted(BorrowerId, ShareTopic) ->
    %% Ensure at least one local candidate is running, which should
    %% eventually create the leader:
    _ = ensure_local(ShareTopic, #{}),
    maybe
        {ok, Leader} ?= emqx_ds_shared_sub_leader:whereis_leader(ShareTopic),
        %% If the leader is already running send it the connect request:
        emqx_ds_shared_sub_proto:send_to_leader(Leader, ?borrower_connect(BorrowerId, ShareTopic)),
        ok
    else
        _ ->
            retry
    end.

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
    Spec = #{
        id => ShareTopic,
        start => {emqx_ds_shared_sub_leader, start_link, [ShareTopic, Options]},
        type => worker,
        restart => temporary,
        shutdown => 5_000
    },
    supervisor:start_child(?MODULE, Spec).

-doc """
Stop local worker for the shared topic filter, regardless of its leadership state.
""".
-spec stop_local(emqx_types:share()) -> boolean().
stop_local(Share) ->
    case supervisor:terminate_child(?MODULE, Share) of
        ok ->
            true;
        {error, not_found} ->
            false
    end.

-spec list_local() -> [{emqx_types:share(), pid() | restarting}].
list_local() ->
    lists:map(
        fun({Share, Child, _, _}) ->
            {Share, Child}
        end,
        supervisor:which_children(?MODULE)
    ).

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
    Children = [],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 1
    },
    {ok, {SupFlags, Children}}.
