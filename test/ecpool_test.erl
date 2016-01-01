
-module(ecpool_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(POOL_OPTS, [
        %% schedulers number
        {pool_size, 10},
        %% round-robbin | random | hash
        {pool_type, random},
        %% false | pos_integer()
        {auto_reconnect, false},
        
        {host, "localhost"},
        {port, 5432},
        {username, "feng"},
        {password, ""},
        {database, "mqtt"},
        {encoding,  utf8}]).

pool_test() ->
    application:start(gproc),
    application:start(ecpool),
    ecpool:start_pool(test_pool, test_client, ?POOL_OPTS).
    %%ecpool:stop_pool(test_pool),
    %%application:stop(ecpool),
    %%application:stop(gproc).


-endif.
