#!/bin/bash

# Echo to stderr on errors
echoerr() { echo "ERROR: $*" 1>&2; }

die() {
    echoerr "ERROR: $1"
    errno=${2:-1}
    exit "$errno"
}

assert_node_alive() {
    if ! relx_nodetool "ping" > /dev/null; then
        die "node_is_not_running!" 1
    fi
}

check_erlang_start() {
  "$BINDIR/$PROGNAME" \
    -noshell \
    -boot_var RELEASE_LIB "$ERTS_LIB_DIR/lib" \
    -boot "$REL_DIR/start_clean" \
    -s crypto start \
    -s erlang halt
}

# Simple way to check the correct user and fail early
check_user() {
    # Validate that the user running the script is the owner of the
    # RUN_DIR.
    if [ "$RUNNER_USER" ] && [ "x$WHOAMI" != "x$RUNNER_USER" ]; then
        if [ "x$WHOAMI" != "xroot" ]; then
            echo "You need to be root or use sudo to run this command"
            exit 1
        fi
        CMD="DEBUG=$DEBUG \"$RUNNER_SCRIPT\" "
        for ARG in "$@"; do
            CMD="${CMD} \"$ARG\""
        done
        # This will drop priviledges into the runner user
        # It exec's in a new shell and the current shell will exit
        exec su - "$RUNNER_USER" -c "$CMD"
    fi
}

# Get node pid
relx_get_pid() {
    if output="$(relx_nodetool rpcterms os getpid)"
    then
        # shellcheck disable=SC2001 # Escaped quote taken as closing quote in editor
        echo "$output" | sed -e 's/"//g'
        return 0
    else
        echo "$output"
        return 1
    fi
}

# Connect to a remote node
relx_rem_sh() {
    # Generate a unique id used to allow multiple remsh to the same node
    # transparently
    id="remsh$(relx_gen_id)-${NAME}"
    # Get the node's ticktime so that we use the same thing.
    TICKTIME="$(relx_nodetool rpcterms net_kernel get_net_ticktime)"

    # shellcheck disable=SC2086 # $EPMD_ARG is supposed to be split by whitespace
    # shellcheck disable=SC2153 # $NAME_TYPE is defined by `common_defs2.sh`, which runs before this is called
    # Setup remote shell command to control node
    if [ "$IS_ELIXIR" = "yes" ]
    then
      exec "$REL_DIR/iex" \
           --remsh "$NAME" \
           --boot-var RELEASE_LIB "$ERTS_LIB_DIR" \
           --cookie "$COOKIE" \
           --hidden \
           --erl "-kernel net_ticktime $TICKTIME" \
           --erl "$EPMD_ARG" \
           --erl "$NAME_TYPE $id" \
           --boot "$REL_DIR/start_clean"
    else
      exec "$BINDIR/erl" "$NAME_TYPE" "$id" \
           -remsh "$NAME" -boot "$REL_DIR/start_clean" \
           -boot_var ERTS_LIB_DIR "$ERTS_LIB_DIR" \
           -setcookie "$COOKIE" -hidden -kernel net_ticktime "$TICKTIME" \
           $EPMD_ARG
    fi
}

# Generate a random id
relx_gen_id() {
    od -t x -N 4 /dev/urandom | head -n1 | awk '{print $2}'
}

# Control a node
relx_nodetool() {
    command="$1"; shift
    ERL_FLAGS="${ERL_FLAGS:-} $EPMD_ARG" \
    "$ERTS_DIR/bin/escript" "$ROOTDIR/bin/nodetool" "$NAME_TYPE" "$NAME" \
                                -setcookie "$COOKIE" "$command" "$@"
}

call_hocon() {
    "$ERTS_DIR/bin/escript" "$ROOTDIR/bin/nodetool" hocon "$@" \
        || die "call_hocon_failed: $*" $?
}

# Run an escript in the node's environment
relx_escript() {
    shift; scriptpath="$1"; shift
    "$ERTS_DIR/bin/escript" "$ROOTDIR/$scriptpath" "$@"
}

# Output a start command for the last argument of run_erl.
# The calling script defines `$START_OPTION`, when the command is to
# start EMQX in the background.
relx_start_command() {
    printf "exec \"%s\" \"%s\"" "$RUNNER_SCRIPT" \
           "$START_OPTION"
}

# Function to generate app.config and vm.args
generate_config() {
    local name_type="$1"
    local node_name="$2"
    ## Delete the *.siz files first or it cann't start after
    ## changing the config 'log.rotation.size'
    rm -rf "${RUNNER_LOG_DIR}"/*.siz

    EMQX_LICENSE_CONF_OPTION=""
    if [ "${EMQX_LICENSE_CONF:-}" != "" ]; then
        EMQX_LICENSE_CONF_OPTION="-c ${EMQX_LICENSE_CONF}"
    fi

    ## timestamp for each generation
    local NOW_TIME
    NOW_TIME="$(call_hocon now_time)"

    ## ths command populates two files: app.<time>.config and vm.<time>.args
    ## NOTE: the generate command merges environment variables to the base config (emqx.conf),
    ## but does not include the cluster-override.conf and local-override.conf
    ## meaning, certain overrides will not be mapped to app.<time>.config file
    ## disable SC2086 to allow EMQX_LICENSE_CONF_OPTION to split
    # shellcheck disable=SC2086
    call_hocon -v -t "$NOW_TIME" -I "$CONFIGS_DIR/" -s $SCHEMA_MOD -c "$RUNNER_ETC_DIR"/emqx.conf $EMQX_LICENSE_CONF_OPTION -d "$RUNNER_DATA_DIR"/configs generate

    ## filenames are per-hocon convention
    local CONF_FILE="$CONFIGS_DIR/app.$NOW_TIME.config"
    local HOCON_GEN_ARG_FILE="$CONFIGS_DIR/vm.$NOW_TIME.args"

    # This is needed by the Elixir scripts.
    # Do NOT append `.config`.
    RELEASE_SYS_CONFIG="$CONFIGS_DIR/app.$NOW_TIME"
    export RELEASE_SYS_CONFIG

    CONFIG_ARGS="-config $CONF_FILE -args_file $HOCON_GEN_ARG_FILE"

    ## Merge hocon generated *.args into the vm.args
    TMP_ARG_FILE="$CONFIGS_DIR/vm.args.tmp"
    cp "$RUNNER_ETC_DIR/vm.args" "$TMP_ARG_FILE"
    echo "" >> "$TMP_ARG_FILE"
    echo "-pa ${REL_DIR}/consolidated" >> "$TMP_ARG_FILE"
    ## read lines from generated vm.<time>.args file
    ## drop comment lines, and empty lines using sed
    ## pipe the lines to a while loop
    sed '/^#/d' "$HOCON_GEN_ARG_FILE" | sed '/^$/d' | while IFS='' read -r ARG_LINE || [ -n "$ARG_LINE" ]; do
        ## in the loop, split the 'key[:space:]value' pair
        ARG_KEY=$(echo "$ARG_LINE" | awk '{$NF="";print}')
        ARG_VALUE=$(echo "$ARG_LINE" | awk '{print $NF}')
        ## use the key to look up in vm.args file for the value
        TMP_ARG_VALUE=$(grep "^$ARG_KEY" "$TMP_ARG_FILE" || true | awk '{print $NF}')
        ## compare generated (to override) value to original (to be overriden) value
        if [ "$ARG_VALUE" != "$TMP_ARG_VALUE" ] ; then
            ## if they are different
            if [ -n "$TMP_ARG_VALUE" ]; then
                ## if the old value is present, replace it with generated value
                sh -c "$SED_REPLACE 's|^$ARG_KEY.*$|$ARG_LINE|' $TMP_ARG_FILE"
            else
                ## otherwise append generated value to the end
                echo "$ARG_LINE" >> "$TMP_ARG_FILE"
            fi
        fi
    done
    echo "$name_type $node_name" >> "$TMP_ARG_FILE"
    ## rename the generated vm.<time>.args file
    mv -f "$TMP_ARG_FILE" "$HOCON_GEN_ARG_FILE"

    # shellcheck disable=SC2086
    if ! relx_nodetool chkconfig $CONFIG_ARGS; then
        die "failed_to_check_config $CONFIG_ARGS"
    fi
}

# check if a PID is down
is_down() {
    PID="$1"
    if ps -p "$PID" >/dev/null; then
        # still around
        # shellcheck disable=SC2009 # this grep pattern is not a part of the progra names
        if ps -p "$PID" | grep -q 'defunct'; then
            # zombie state, print parent pid
            parent="$(ps -o ppid= -p "$PID" | tr -d ' ')"
            echo "WARN: $PID is marked <defunct>, parent:"
            ps -p "$parent"
            return 0
        fi
        return 1
    fi
    # it's gone
    return 0
}

wait_for() {
    local WAIT_TIME
    local CMD
    WAIT_TIME="$1"
    shift
    CMD="$*"
    while true; do
        if $CMD >/dev/null 2>&1; then
            return 0
        fi
        if [ "$WAIT_TIME" -le 0 ]; then
            return 1
        fi
        WAIT_TIME=$((WAIT_TIME - 1))
        sleep 1
    done
}

latest_vm_args() {
    local hint_var_name="$1"
    local vm_args_file
    vm_args_file="$(find "$CONFIGS_DIR" -type f -name "vm.*.args" | sort | tail -1)"
    if [ -f "$vm_args_file" ]; then
        echo "$vm_args_file"
    else
        echoerr "node not initialized?"
        # shellcheck disable=SC2153 # $COMMAND is defined by the calling script
        echoerr "Generated config file vm.*.args is not found for command '$COMMAND'"
        echoerr "in config dir: $CONFIGS_DIR"
        echoerr "In case the file has been deleted while the node is running,"
        echoerr "set environment variable '$hint_var_name' to continue"
        exit 1
    fi
}
