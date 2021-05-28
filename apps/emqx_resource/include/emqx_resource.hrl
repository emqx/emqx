-type resource_type() :: module().
-type instance_id() :: binary().
-type resource_config() :: jsx:json_term().
-type resource_spec() :: map().
-type resource_state() :: term().
-type resource_data() :: #{
    id => instance_id(),
    mod => module(),
    config => resource_config(),
    state => resource_state(),
    status => started | stopped
}.

-type after_query() :: {OnSuccess :: after_query_fun(), OnFailed :: after_query_fun()} |
    undefined.

%% the `after_query_fun()` is mainly for callbacks that increment counters or do some fallback
%% actions upon query failure
-type after_query_fun() :: {fun((...) -> ok), Args :: [term()]}.
