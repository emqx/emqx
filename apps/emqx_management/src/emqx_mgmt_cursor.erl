%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_cursor).
-moduledoc """
Keeps the state of REST API pagination cursors on the node that issued them.

The REST API client gets an opaque URL-safe ID instead of the state. The ID
holds random bytes and the name of the issuing node, so any node in the
cluster can look the state up.

A cursor can be looked up any number of times until its TTL ends. Each domain
has a limit of open cursors per node. When a domain is full, storing a new
cursor drops the oldest one of that domain.
""".

-behaviour(gen_server).

-export([start_link/0, store/2, lookup/2]).

%% RPC target
-export([lookup_local/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(BPAPI_NAME, emqx_mgmt_cursor).
-define(RAND_BYTES, 16).
-define(DEFAULT_MAX_OPEN, 1000).
-define(DEFAULT_TTL, timer:minutes(10)).
%% Shorter than the RPC timeout, so that the issuing node answers a remote lookup itself.
-define(DEFAULT_CALL_TIMEOUT, timer:seconds(3)).
-define(SWEEP_INTERVAL, timer:seconds(30)).

-type domain() :: atom().
-type id() :: binary().

-export_type([domain/0, id/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Store a cursor state and return its ID.".
-spec store(domain(), term()) -> id().
store(Domain, State) when is_atom(Domain) ->
    gen_server:call(?MODULE, {store, Domain, State}, call_timeout()).

-doc "Look up the cursor state stored under the ID, on whichever node issued it.".
-spec lookup(domain(), binary()) -> {ok, term()} | {error, bad_cursor}.
lookup(Domain, Id) when is_atom(Domain), is_binary(Id) ->
    case id_node(Id) of
        {ok, Node} when Node =:= node() ->
            lookup_local(Domain, Id);
        {ok, Node} ->
            lookup_remote(Node, Domain, Id);
        error ->
            {error, bad_cursor}
    end;
lookup(_Domain, _Id) ->
    {error, bad_cursor}.

-spec lookup_local(domain(), id()) -> {ok, term()} | {error, bad_cursor}.
lookup_local(Domain, Id) ->
    try
        gen_server:call(?MODULE, {lookup, Domain, Id}, call_timeout())
    catch
        %% The cursors are lost together with the process.
        exit:{noproc, _} ->
            {error, bad_cursor};
        exit:{timeout, _} ->
            {error, bad_cursor}
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    schedule_sweep(),
    {ok, #{cursors => #{}, queues => #{}}}.

handle_call({store, Domain, State}, _From, S0) ->
    Id = new_id(),
    S = insert(Domain, Id, State, make_room(Domain, expire(Domain, now_ms(), S0))),
    {reply, Id, S};
handle_call({lookup, Domain, Id}, _From, #{cursors := Cursors} = S) ->
    Now = now_ms(),
    Reply =
        case Cursors of
            #{Id := {Domain, Deadline, State}} when Now < Deadline ->
                {ok, State};
            #{} ->
                {error, bad_cursor}
        end,
    {reply, Reply, S};
handle_call(_Req, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(sweep, #{queues := Queues} = S) ->
    schedule_sweep(),
    Now = now_ms(),
    {noreply, lists:foldl(fun(D, Acc) -> expire(D, Now, Acc) end, S, maps:keys(Queues))};
handle_info(_Info, S) ->
    {noreply, S}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

lookup_remote(Node, Domain, Id) ->
    IsMember = lists:member(Node, emqx:running_nodes()),
    case IsMember andalso emqx_bpapi:supported_version(Node, ?BPAPI_NAME) of
        Vsn when is_integer(Vsn), Vsn >= 1 ->
            try
                emqx_mgmt_cursor_proto_v1:lookup(Node, Domain, Id)
            catch
                error:{erpc, _} ->
                    {error, bad_cursor}
            end;
        _ ->
            {error, bad_cursor}
    end.

%% Each domain queue holds IDs in the order they were stored, which is also
%% the order they expire in.
insert(Domain, Id, State, #{cursors := Cursors, queues := Queues} = S) ->
    {N, Q} = maps:get(Domain, Queues, {0, queue:new()}),
    S#{
        cursors := Cursors#{Id => {Domain, now_ms() + ttl(), State}},
        queues := Queues#{Domain => {N + 1, queue:in(Id, Q)}}
    }.

make_room(Domain, S) ->
    make_room(Domain, max_open(), S).

make_room(Domain, Max, #{cursors := Cursors, queues := Queues} = S) ->
    case Queues of
        #{Domain := {N, Q0}} when N >= 1, N >= Max ->
            {{value, Id}, Q} = queue:out(Q0),
            make_room(Domain, Max, S#{
                cursors := maps:remove(Id, Cursors),
                queues := Queues#{Domain := {N - 1, Q}}
            });
        #{} ->
            S
    end.

expire(Domain, Now, #{cursors := Cursors, queues := Queues} = S) ->
    case Queues of
        #{Domain := {N, Q0}} when N > 0 ->
            {value, Id} = queue:peek(Q0),
            case Cursors of
                #{Id := {_, Deadline, _}} when Now < Deadline ->
                    S;
                #{} ->
                    expire(Domain, Now, S#{
                        cursors := maps:remove(Id, Cursors),
                        queues := Queues#{Domain := {N - 1, queue:drop(Q0)}}
                    })
            end;
        #{} ->
            S
    end.

schedule_sweep() ->
    erlang:send_after(?SWEEP_INTERVAL, self(), sweep).

new_id() ->
    b64_encode(<<(crypto:strong_rand_bytes(?RAND_BYTES))/binary, (atom_to_binary(node()))/binary>>).

id_node(Id) ->
    try
        <<_:?RAND_BYTES/binary, NodeBin/binary>> = b64_decode(Id),
        true = NodeBin =/= <<>>,
        {ok, binary_to_existing_atom(NodeBin)}
    catch
        error:_ ->
            error
    end.

b64_encode(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).

b64_decode(Bin) ->
    base64:decode(Bin, #{mode => urlsafe, padding => false}).

max_open() ->
    application:get_env(emqx_management, cursor_max_open, ?DEFAULT_MAX_OPEN).

ttl() ->
    application:get_env(emqx_management, cursor_ttl, ?DEFAULT_TTL).

call_timeout() ->
    application:get_env(emqx_management, cursor_call_timeout, ?DEFAULT_CALL_TIMEOUT).

now_ms() ->
    erlang:monotonic_time(millisecond).
