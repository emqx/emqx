# emqx_resource

The `emqx_resource` is a behavior that manages configuration specs and runtime states
for resources like mysql or redis backends.

It is intended to be used by the emqx_data_bridges and all other resources that need CRUD operations
to their configs, and need to initialize the states when creating.

There can be foreign references between resource instances via resource-id.
So they may find each other via this Id.

The main idea of the emqx resource is to put all the `general` code in a common lib, including
the config operations (like config validation, config dump back to files), and the state management.
And we put all the `specific` codes to the callback modules.

## Try it out

    $ ./demo.sh
    Eshell V11.1.8  (abort with ^G)
    1> == the demo log tracer <<"log_tracer_clientid_shawn">> started.
    config: #{<<"config">> =>
                #{<<"bulk">> => <<"10KB">>,<<"cache_log_dir">> => <<"/tmp">>,
                    <<"condition">> => #{<<"clientid">> => <<"abc">>},
                    <<"level">> => <<"debug">>},
            <<"id">> => <<"log_tracer_clientid_shawn">>,
            <<"resource_type">> => <<"log_tracer">>}
    1> emqx_resource_instance:health_check(<<"log_tracer_clientid_shawn">>).
    == the demo log tracer <<"log_tracer_clientid_shawn">> is working well
    state: #{health_checked => 1,logger_handler_id => abc}
    ok

    2> emqx_resource_instance:health_check(<<"log_tracer_clientid_shawn">>).
    == the demo log tracer <<"log_tracer_clientid_shawn">> is working well
    state: #{health_checked => 2,logger_handler_id => abc}
    ok

    3> emqx_resource_instance:query(<<"log_tracer_clientid_shawn">>, get_log).
    == the demo log tracer <<"log_tracer_clientid_shawn">> received request: get_log
    state: #{health_checked => 2,logger_handler_id => abc}
    "this is a demo log messages..."

    4> emqx_resource_instance:remove(<<"log_tracer_clientid_shawn">>).
    == the demo log tracer <<"log_tracer_clientid_shawn">> stopped.
    state: #{health_checked => 0,logger_handler_id => abc}
    ok

    5> emqx_resource_instance:query(<<"log_tracer_clientid_shawn">>, get_log).
    ** exception error: {get_instance,{<<"log_tracer_clientid_shawn">>,not_found}}
