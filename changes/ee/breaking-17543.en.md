The default EMQX Enterprise docker image (`emqx/emqx-enterprise:<vsn>`)
no longer bundles the Snowflake ODBC driver, which trims roughly
240 MB from the image.

A separately tagged image that bundles the driver is still published
as `emqx/emqx-enterprise:<vsn>-sf` for users who connect to
Snowflake via the Snowflake bridge from inside the container.
Switch to the `-sf` tag, or layer
`scripts/install-snowflake-driver.sh` on top of the base image with
a custom Dockerfile.
