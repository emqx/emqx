-module(emqx_acme_challenge).

-moduledoc """
Cluster-wide ACME HTTP-01 challenge responder.

A registered gen_server runs on every cluster node so that an
HTTP-01 challenge request routed by an NLB to any node can be
served. The leader (lowest-sorted core node) drives issuance and
populates the local challenge table; non-leader nodes' cowboy
handlers fall back to a remote gen_server:call against the leader
to resolve tokens they don't have.

Lifecycle:
- emqx_acme_sup starts one of these per node at boot.
- The leader calls cluster_start_listener/1 before issuance and
  cluster_stop_listener/0 after, both via gen_server:multi_call so
  every node's cowboy listener stays in lockstep.
- Tokens written via set_challenges/1 only land on the leader's
  local ETS; remote nodes look them up through the leader.
""".

-behaviour(gen_server).

-include("emqx_acme.hrl").

%% Lifecycle
-export([start_link/0]).
%% Cluster-wide fanout — used by emqx_acme_issuer during issuance.
-export([
    cluster_start_listener/1,
    cluster_stop_listener/0
]).
%% Leader-only state mutation (called inside acme-client's challenge_fn).
-export([
    set_challenges/1,
    clear_challenges/0
]).
%% Cowboy handler entry point.
-export([init/2]).
%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-ifdef(TEST).
%% Local (single-node) variants used by CT to avoid pulling in mria.
-export([start_listener/1, stop_listener/0]).
-endif.

-define(SERVER, ?MODULE).
-define(LISTENER_NAME, emqx_acme_challenge).
-define(CHALLENGE_TAB, emqx_acme_challenge_tab).
-define(MULTI_CALL_TIMEOUT, 15_000).

%%--------------------------------------------------------------------
%% Lifecycle
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Cluster-wide listener fanout
%%--------------------------------------------------------------------

%% Start the HTTP-01 listener on every running cluster node (cores +
%% replicants), in lockstep. Idempotent per-node: if a node already has
%% the listener up, its responder no-ops. Returns {error, _} if any
%% node fails so the caller can decide to bail out before issuance.
-spec cluster_start_listener(pos_integer()) -> ok | {error, term()}.
cluster_start_listener(Port) ->
    multi_call({start_listener, Port}).

-spec cluster_stop_listener() -> ok | {error, term()}.
cluster_stop_listener() ->
    multi_call(stop_listener).

multi_call(Msg) ->
    Nodes = cluster_nodes(),
    {Replies, BadNodes} = gen_server:multi_call(
        Nodes, ?SERVER, Msg, ?MULTI_CALL_TIMEOUT
    ),
    NonOk = [{N, R} || {N, R} <- Replies, R =/= ok],
    case {NonOk, BadNodes} of
        {[], []} -> ok;
        _ -> {error, #{failed => NonOk, bad_nodes => BadNodes}}
    end.

%%--------------------------------------------------------------------
%% Local state mutation (leader's challenge_fn)
%%--------------------------------------------------------------------

-spec set_challenges([map()]) -> ok.
set_challenges(Challenges) ->
    gen_server:call(?SERVER, {set_challenges, Challenges}, infinity).

-spec clear_challenges() -> ok.
clear_challenges() ->
    gen_server:call(?SERVER, clear_challenges, infinity).

-ifdef(TEST).
start_listener(Port) ->
    gen_server:call(?SERVER, {start_listener, Port}, infinity).
stop_listener() ->
    gen_server:call(?SERVER, stop_listener, infinity).
-endif.

%%--------------------------------------------------------------------
%% Cowboy handler
%%--------------------------------------------------------------------

init(Req0, State) ->
    Token = cowboy_req:binding(token, Req0),
    case lookup_token(Token) of
        {ok, KeyAuth} ->
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/octet-stream">>},
                KeyAuth,
                Req0
            ),
            {ok, Req, State};
        not_found ->
            Req = cowboy_req:reply(404, #{}, <<"not found">>, Req0),
            {ok, Req, State}
    end.

%% Check our local ETS first; on miss, ask the leader. Replicants and
%% non-leader cores never write to the table, so a miss is the common
%% case for them — every CA validation hop costs one extra round-trip
%% to the leader, which is fine for ACME's short challenge phase.
lookup_token(Token) ->
    case ets:lookup(?CHALLENGE_TAB, Token) of
        [{Token, KeyAuth}] ->
            {ok, KeyAuth};
        [] ->
            remote_lookup(Token)
    end.

remote_lookup(Token) ->
    case emqx_acme_issuer:leader_node() of
        {ok, Leader} when Leader =:= node() ->
            %% We are the leader; the local ETS already said no.
            not_found;
        {ok, Leader} ->
            try
                gen_server:call(
                    {?SERVER, Leader}, {lookup_token, Token}, ?MULTI_CALL_TIMEOUT
                )
            catch
                exit:_ -> not_found
            end;
        {error, _} ->
            not_found
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    %% Owning the table from the gen_server keeps its lifetime tied to
    %% our supervised process — no transient-owner / heir gymnastics.
    _ = ets:new(?CHALLENGE_TAB, [
        named_table, public, set, {read_concurrency, true}
    ]),
    {ok, #{listener => stopped}}.

handle_call({start_listener, Port}, _From, #{listener := running} = State) ->
    %% Idempotent: a re-issue (or a cross-node start that races with
    %% local boot) shouldn't fail just because we already have cowboy up.
    _ = Port,
    {reply, ok, State};
handle_call({start_listener, Port}, _From, #{listener := stopped} = State) ->
    case do_start_listener(Port) of
        ok ->
            {reply, ok, State#{listener => running}};
        {error, _} = Error ->
            {reply, Error, State}
    end;
handle_call(stop_listener, _From, #{listener := stopped} = State) ->
    {reply, ok, State};
handle_call(stop_listener, _From, #{listener := running} = State) ->
    ok = do_stop_listener(),
    %% Tokens are only valid for the ACME order that's just ended; drop
    %% them so a stray late HTTP-01 hit (or the next issuance's first
    %% lookup) doesn't see stale data.
    true = ets:delete_all_objects(?CHALLENGE_TAB),
    {reply, ok, State#{listener => stopped}};
handle_call({set_challenges, Challenges}, _From, State) ->
    lists:foreach(
        fun(#{token := Token, key := Key}) ->
            true = ets:insert(?CHALLENGE_TAB, {Token, Key})
        end,
        Challenges
    ),
    {reply, ok, State};
handle_call(clear_challenges, _From, State) ->
    true = ets:delete_all_objects(?CHALLENGE_TAB),
    {reply, ok, State};
handle_call({lookup_token, Token}, _From, State) ->
    %% Remote-side handler called by a non-leader's cowboy init/2.
    Reply =
        case ets:lookup(?CHALLENGE_TAB, Token) of
            [{Token, KeyAuth}] -> {ok, KeyAuth};
            [] -> not_found
        end,
    {reply, Reply, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{listener := running}) ->
    _ = do_stop_listener(),
    ok;
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal: cowboy lifecycle
%%--------------------------------------------------------------------

do_start_listener(Port) ->
    Dispatch = cowboy_router:compile([
        {'_', [{"/.well-known/acme-challenge/:token", ?MODULE, []}]}
    ]),
    TransOpts = #{socket_opts => [{port, Port}], num_acceptors => 4},
    ProtoOpts = #{env => #{dispatch => Dispatch}},
    case cowboy:start_clear(?LISTENER_NAME, TransOpts, ProtoOpts) of
        {ok, _Pid} ->
            ?LOG(info, #{msg => "acme_challenge_listener_started", port => Port}),
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} = Error ->
            ?LOG(error, #{
                msg => "acme_challenge_listener_start_failed", reason => Reason
            }),
            Error
    end.

do_stop_listener() ->
    case cowboy:stop_listener(?LISTENER_NAME) of
        ok ->
            ?LOG(info, #{msg => "acme_challenge_listener_stopped"}),
            ok;
        {error, not_found} ->
            ok
    end.

%%--------------------------------------------------------------------
%% Cluster membership
%%--------------------------------------------------------------------

%% Includes cores and replicants — an NLB at L4 will steer connections
%% to either, so every node needs to be able to answer HTTP-01. Falls
%% back to a single-node list when mria isn't running (test VMs / very
%% early boot) so the same code path drives CT and production.
cluster_nodes() ->
    try mria:running_nodes() of
        [] -> [node()];
        Nodes -> Nodes
    catch
        _:_ -> [node()]
    end.
