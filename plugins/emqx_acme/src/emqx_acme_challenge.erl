-module(emqx_acme_challenge).

-include("emqx_acme.hrl").

%% API
-export([
    create_tab/0,
    delete_tab/0,
    start/1,
    stop/0,
    set_challenges/1,
    clear_challenges/0
]).

%% Cowboy handler callbacks
-export([init/2]).

-define(LISTENER_NAME, emqx_acme_challenge).
-define(CHALLENGE_TAB, emqx_acme_challenge_tab).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start(pos_integer()) -> ok | {error, term()}.
start(Port) ->
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/.well-known/acme-challenge/:token", ?MODULE, []}
        ]}
    ]),
    TransOpts = #{
        socket_opts => [{port, Port}],
        num_acceptors => 4
    },
    ProtoOpts = #{env => #{dispatch => Dispatch}},
    case cowboy:start_clear(?LISTENER_NAME, TransOpts, ProtoOpts) of
        {ok, _Pid} ->
            ?LOG(info, #{msg => "acme_challenge_listener_started", port => Port}),
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} = Error ->
            ?LOG(error, #{msg => "acme_challenge_listener_start_failed", reason => Reason}),
            Error
    end.

-spec stop() -> ok.
stop() ->
    case cowboy:stop_listener(?LISTENER_NAME) of
        ok ->
            ?LOG(info, #{msg => "acme_challenge_listener_stopped"}),
            ok;
        {error, not_found} ->
            ok
    end,
    clear_challenges().

-spec set_challenges([map()]) -> ok.
set_challenges(Challenges) ->
    lists:foreach(
        fun(#{token := Token, key := Key}) ->
            ets:insert(?CHALLENGE_TAB, {Token, Key})
        end,
        Challenges
    ),
    ok.

-spec clear_challenges() -> ok.
clear_challenges() ->
    ets:delete_all_objects(?CHALLENGE_TAB),
    ok.

%%--------------------------------------------------------------------
%% Cowboy handler
%%--------------------------------------------------------------------

init(Req0, State) ->
    Token = cowboy_req:binding(token, Req0),
    case ets:lookup(?CHALLENGE_TAB, Token) of
        [{Token, KeyAuth}] ->
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/octet-stream">>},
                KeyAuth,
                Req0
            ),
            {ok, Req, State};
        [] ->
            Req = cowboy_req:reply(404, #{}, <<"not found">>, Req0),
            {ok, Req, State}
    end.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

%% Called from emqx_acme_sup:init/1 so the table is owned by the
%% supervisor and outlives every challenge-listener restart.
-spec create_tab() -> ok.
create_tab() ->
    emqx_utils_ets:new(?CHALLENGE_TAB, [public, set, {read_concurrency, true}]).

%% Symmetric teardown for tests that create the table outside the sup
%% (production code never deletes it — the sup process owns it until
%% the plugin's beam tree shuts down).
-spec delete_tab() -> ok.
delete_tab() ->
    emqx_utils_ets:delete(?CHALLENGE_TAB).
