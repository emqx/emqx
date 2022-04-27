-module(prop_rule_maps).

-include_lib("proper/include/proper.hrl").

prop_get_put_single_key() ->
    ?FORALL(
        {Key, Val},
        {term(), term()},
        begin
            Val =:=
                emqx_rule_maps:nested_get(
                    {var, Key},
                    emqx_rule_maps:nested_put({var, Key}, Val, #{})
                )
        end
    ).
