defmodule EmqxReleaseHelper.Script do
  def run(release, config) do
    script_path = Path.join(config.project_path, "scripts")

    {_, 0} =
      script_path
      |> Path.join("merge-config.escript")
      |> System.cmd([])

    {_, 0} =
      script_path
      |> Path.join("get-dashboard.sh")
      |> System.cmd([], env: [{"EMQX_DASHBOARD_VERSION", "v5.0.0-beta.13"}])

    release
  end
end
