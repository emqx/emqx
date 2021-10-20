defmodule EmqxReleaseHelper.Overlay do
  use EmqxReleaseHelper.DSL.Overlay

  overlay %{release_version: release_version} do
    mkdir "log"
    mkdir "etc"
    mkdir "data"
    mkdir "data/mnesia"
    mkdir "data/configs"
    mkdir "data/patches"
    mkdir "data/scripts"

    copy "bin/install_upgrade.escript", "bin/install_upgrade.escript"

    copy "bin/node_dump", "bin/node_dump"
    copy "bin/emqx_ctl", "bin/emqx_ctl"

    template "data/emqx_vars", "releases/emqx_vars"
    template "data/BUILT_ON", "releases/#{release_version}/BUILT_ON"
  end
end
