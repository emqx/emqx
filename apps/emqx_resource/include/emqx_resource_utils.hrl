-define(CLUSTER_CALL(Func, Args), ?CLUSTER_CALL(Func, Args, ok)).

-define(CLUSTER_CALL(Func, Args, ResParttern),
%% ekka_mnesia:running_nodes()
    fun() ->
        case LocalResult = erlang:apply(?MODULE, Func, Args) of
            ResParttern ->
                case rpc:multicall(nodes(), ?MODULE, Func, Args, 5000) of
                {ResL, []} ->
                    Filter = fun
                        (ResParttern) -> false;
                        ({badrpc, {'EXIT', {undef, [{?MODULE, Func0, _, []}]}}})
                            when Func0 =:= Func -> false;
                        (_) -> true
                    end,
                    case lists:filter(Filter, ResL) of
                        [] -> LocalResult;
                        ErrL -> {error, ErrL}
                    end;
                {ResL, BadNodes} ->
                    {error, {failed_on_nodes, BadNodes, ResL}}
                end;
            ErrorResult ->
                {error, ErrorResult}
        end
    end()).

-define(SAFE_CALL(_EXP_),
        ?SAFE_CALL(_EXP_, _ = do_nothing)).

-define(SAFE_CALL(_EXP_, _EXP_ON_FAIL_),
        fun() ->
            try (_EXP_)
            catch _EXCLASS_:_EXCPTION_:_ST_ ->
                _EXP_ON_FAIL_,
                {error, {_EXCLASS_, _EXCPTION_, _ST_}}
            end
        end()).