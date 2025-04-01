Plugin subsystem now respects the result of the plugin's `on_config_changed` callback.

The callback is called before updating the config. If the callback does not return `ok`, the config is not updated and a error is returned to the dasboard.
