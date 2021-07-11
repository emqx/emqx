defmodule EmqxReleaseHelper.Overlay do
  use EmqxReleaseHelper.DSL.Overlay

  overlay %{release_version: release_version} do
    mkdir "log"
    mkdir "etc"
    mkdir "etc/plugins"
    mkdir "data"
    mkdir "data/mnesia"
    mkdir "data/configs"
    mkdir "data/patches"
    mkdir "data/scripts"

    copy "bin/install_upgrade.escript", "bin/install_upgrade.escript"

    copy "bin/node_dump", "bin/node_dump"
    copy "bin/nodetool", "bin/nodetool"
    copy "bin/nodetool", "bin/nodetool-#{release_version}"

    # copy "bin/emqx", "bin/emqx"
    # copy "bin/emqx_ctl", "bin/emqx_ctl"

    # for relup
    copy "bin/emqx", "bin/emqx-#{release_version}"
    copy "bin/emqx_ctl", "bin/emqx_ctl-#{release_version}"

    copy "bin/install_upgrade.escript", "bin/install_upgrade.escript-#{release_version}"

    template "data/loaded_plugins.tmpl", "data/loaded_plugins"

    template "data/loaded_modules.tmpl", "data/loaded_modules"

    template "data/emqx_vars", "releases/emqx_vars"
    template "data/BUILT_ON", "releases/#{release_version}/BUILT_ON"
    # template "bin/emqx.cmd", "bin/emqx.cmd"
    # template "bin/emqx_ctl.cmd", "bin/emqx_ctl.cmd"
  end

  def run(release, config) do
    Enum.each(__overlays__(), fn overlay -> overlay.(config) end)
    release
  end
end
