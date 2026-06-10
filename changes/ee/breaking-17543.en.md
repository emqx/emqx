EMQX Enterprise docker images (`emqx/emqx-enterprise:<vsn>`) no longer
bundle the Snowflake ODBC driver. The image size drops by roughly
240 MB.

Users who connect to Snowflake via the Snowflake bridge from inside
the docker container must now install the driver themselves on top of
the base image; see `scripts/install-snowflake-driver.sh` in the EMQX
source tree for a reference install procedure that can be layered in
with a custom Dockerfile.
