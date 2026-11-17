defmodule Mix.Tasks.Compile.Cargo do
  @moduledoc """
  Builds the Rust NIF crates listed in the `:cargo_crates` project option and copies each
  shared library into the `priv` directory of the application build path.

  Each entry is a map:

    * `:path` - crate directory, relative to the application root;
    * `:lib` - library name from the crate `[lib]` section (`lib<name>.so` is copied).

  Cargo writes its target directory under the build path, so the source tree stays clean.
  Cargo decides itself whether the crate is stale. `Cargo.lock` must be committed, because
  the build runs with `--locked`.
  """
  use Mix.Task.Compiler

  @recursive true

  @impl true
  def run(_args) do
    Mix.Project.get!()
    config = Mix.Project.config()
    app_root = File.cwd!()
    app_path = Mix.Project.app_path(config)
    target_root = Path.join([Mix.Project.build_path(config), "cargo", to_string(config[:app])])

    results =
      Enum.map(config[:cargo_crates] || [], fn crate ->
        build(crate, app_root, app_path, target_root)
      end)

    if Enum.any?(results, &(&1 == :ok)), do: {:ok, []}, else: {:noop, []}
  end

  defp build(%{path: path, lib: lib}, app_root, app_path, target_root) do
    manifest = Path.join([app_root, path, "Cargo.toml"])

    args = [
      "build",
      "--release",
      "--locked",
      "--manifest-path",
      manifest,
      "--target-dir",
      target_root
    ]

    Mix.shell().info("cargo #{Enum.join(args, " ")}")

    case System.cmd("cargo", args, stderr_to_stdout: true) do
      {_out, 0} ->
        copy_lib(lib, target_root, app_path)

      {out, code} ->
        Mix.raise("cargo build failed for #{path} with exit code #{code}:\n#{out}")
    end
  end

  defp copy_lib(lib, target_root, app_path) do
    src = Path.join([target_root, "release", "lib#{lib}.so"])
    dest = Path.join([app_path, "priv", "lib#{lib}.so"])
    File.mkdir_p!(Path.dirname(dest))

    if File.exists?(dest) and File.read!(dest) == File.read!(src) do
      :noop
    else
      File.cp!(src, dest)
      :ok
    end
  end
end
