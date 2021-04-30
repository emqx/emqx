%% @author:
%% @description:
-module(test_utils).
%% ====================================================================
%% API functions
%% ====================================================================
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_rule_engine/include/rule_engine.hrl").

-compile([export_all, nowarn_export_all]).

%% ====================================================================
%% Internal functions
%% ====================================================================
resource_is_alive(Id) ->
    {ok, #resource_params{status = #{is_alive := Alive}} = Params} = emqx_rule_registry:find_resource_params(Id),
    ct:pal("Id: ~p, Alive: ~p, Resource ===> :~p~n", [Id, Alive, Params]),
    ?assertEqual(true, Alive),
    Alive.
