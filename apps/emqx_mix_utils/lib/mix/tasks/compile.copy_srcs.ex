defmodule Mix.Tasks.Compile.CopySrcs do
  use Mix.Task.Compiler

  @recursive true

  @impl true
  def run(_args) do
    Mix.Project.get!()
    config = Mix.Project.config()
    extra_dirs = config[:extra_dirs]

    unless extra_dirs && is_list(extra_dirs) do
      Mix.raise(
        "application option :extra_dirs in #{Mix.Project.project_file()} must be a list of directories under the application"
      )
    end

    app_root = File.cwd!()
    app_build_path = Mix.Project.app_path(config)

    for extra_dir <- extra_dirs do
      src = Path.join([app_root, extra_dir])
      dest = Path.join([app_build_path, extra_dir])
      File.rm(dest)

      case File.ln_s(src, dest) do
        :ok ->
          :ok

        {:error, :eexist} ->
          Mix.shell().info(
            IO.ANSI.format([:yellow, "#{dest} still exists after attempted removal"])
          )

          :ok

        {:error, error} ->
          Mix.raise("error trying to link #{src} to #{dest}: #{error}")
      end
    end

    {:noop, []}
  end
end
