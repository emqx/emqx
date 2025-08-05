defmodule Mix.Tasks.Emqx.StaticChecks do
  use Mix.Task

  alias Mix.Tasks.Emqx.Ct, as: ECt

  @requirements ["compile", "loadpaths"]

  @shortdoc "Run static checks (xref, dialyzer, bpapi checks)"

  @moduledoc """
  This is a very lightweight and specific version of CT invocation, because this suite is
  run in CI using the release build (we don't want to run static checks on test code), but
  the test script itself is written assuming to be run by CT.

  ## Examples

      $ mix emqx.static_checks
  """

  @common_helpers_src ~c"apps/emqx/test/emqx_common_test_helpers.erl"
  @bpapi_test_src ~c"apps/emqx_bpapi/test/emqx_bpapi_static_checks.erl"
  @test_script_src ~c"apps/emqx/test/emqx_static_checks.erl"

  @impl true
  def run(_args) do
    ensure_compiled_and_loaded!(@common_helpers_src)
    ensure_compiled_and_loaded!(@bpapi_test_src)
    ECt.ensure_whole_emqx_project_is_loaded()
    mod = ensure_compiled_and_loaded!(@test_script_src)
    tests = mod.all()
    cfg = [unused: true]
    :logger.set_primary_config(:level, :warning)

    error? =
      Enum.reduce(tests, false, fn test, error_acc? ->
        try do
          apply(mod, test, [cfg])
          error_acc?
        catch
          k, e ->
            ECt.warn([
              "Error running #{mod}.#{test}:\n",
              inspect({k, e, __STACKTRACE__}, pretty: true)
            ])

            _error = true
        end
      end)

    if error? do
      :logger.set_primary_config(:level, :notice)
      mod.end_per_suite(cfg)
      System.halt(1)
    else
      ECt.info([:green, "Ok!"])
    end
  end

  def ensure_compiled_and_loaded!(src) do
    case :compile.file(src, [:binary, :report]) do
      {:ok, mod, bin} ->
        :code.purge(mod)
        {:module, mod} = :code.load_binary(mod, src, bin)
        mod

      err ->
        raise "error compiling #{src}: #{inspect(err, pretty: true)}"
    end
  end
end
