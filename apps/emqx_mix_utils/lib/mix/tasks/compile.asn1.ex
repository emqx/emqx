defmodule Mix.Tasks.Compile.Asn1 do
  use Mix.Task.Compiler

  @recursive true
  @manifest_vsn 1
  @manifest "compile.asn1"
  # TODO: use manifest to track generated files?

  @stale? {__MODULE__, :stale?}

  @impl true
  def manifests(), do: [manifest()]
  defp manifest(), do: Path.join(Mix.Project.manifest_path(), @manifest)

  @impl true
  def run(_args) do
    add_to_path_and_cache(:asn1)

    Mix.Project.get!()
    config = Mix.Project.config()
    app_root = File.cwd!()

    asn1_srcs = config[:asn1_srcs] || []
    manifest_data = read_manifest(manifest())
    manifest_modified_time = Mix.Utils.last_modified(manifest())
    Enum.each(asn1_srcs, &compile(&1, app_root, manifest_modified_time))

    if Process.get(@stale?, false) do
      write_manifest(manifest(), manifest_data)
    end

    {:noop, []}
  after
    Process.delete(@stale?)
  end

  defp compile(src, app_root, manifest_modified_time) do
    %{
      src: src_path,
      compile_opts: compile_opts
    } = src

    src_path =
      app_root
      |> Path.join(src_path)
      |> Path.expand()

    if stale?(src_path, manifest_modified_time) do
      Process.put(@stale?, true)
      debug("compiling asn1 file: #{src_path}")
      :ok = :asn1ct.compile(to_charlist(src_path), compile_opts)
    else
      debug("file is up to date, not compiling: #{src_path}")
    end
  end

  defp stale?(file, manifest_modified_time) do
    with true <- File.exists?(file),
         false <- Mix.Utils.stale?([file], [manifest_modified_time]) do
      false
    else
      _ -> true
    end
  end

  defp read_manifest(file) do
    try do
      file |> File.read!() |> :erlang.binary_to_term()
    rescue
      _ -> %{}
    else
      {@manifest_vsn, data} when is_map(data) -> data
      _ -> %{}
    end
  end

  defp write_manifest(file, data) do
    debug("writing manifest #{file}")
    File.mkdir_p!(Path.dirname(file))
    File.write!(file, :erlang.term_to_binary({@manifest_vsn, data}))
  end

  def add_to_path_and_cache(lib_name) do
    :code.lib_dir()
    |> Path.join("#{lib_name}-*")
    |> Path.wildcard()
    |> hd()
    |> Path.join("ebin")
    |> to_charlist()
    |> :code.add_path(:cache)
  end

  defp debug(iodata) do
    if Mix.debug?() do
      Mix.shell().info(IO.ANSI.format([:cyan, iodata]))
    end
  end
end
