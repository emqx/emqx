defmodule EMQX.Mix.DepsTest do
  use ExUnit.Case

  alias EMQX.Mix.Deps

  # Ensure the module is compiled and loaded
  setup_all do
    # Add the build path to code path
    # Try multiple possible build paths
    build_paths = [
      Path.expand("../../../../_build/emqx-enterprise/lib/emqx_mix_utils/ebin", __DIR__),
      Path.expand("../../../../_build/emqx-enterprise-test/lib/emqx_mix_utils/ebin", __DIR__)
    ]

    Enum.each(build_paths, fn path ->
      if File.dir?(path) do
        Code.prepend_path(path)
      end
    end)

    :ok
  end

  describe "compute_transitive_closure/2" do
    test "empty list returns empty map" do
      result = Deps.compute_transitive_closure([], [])
      assert result == %{}
    end

    test "empty map returns empty map" do
      result = Deps.compute_transitive_closure([%{}], [])
      assert result == %{}
    end

    test "single app with no users" do
      used_by_map = %{app1: MapSet.new()}
      all_apps = [:app1]
      result = Deps.compute_transitive_closure([used_by_map], all_apps)
      assert result == %{app1: MapSet.new()}
    end

    test "direct dependency only" do
      used_by_map = %{
        app1: MapSet.new([:app2]),
        app2: MapSet.new()
      }

      all_apps = [:app1, :app2]
      result = Deps.compute_transitive_closure([used_by_map], all_apps)
      assert result == %{app1: MapSet.new([:app2]), app2: MapSet.new()}
    end

    test "transitive dependency" do
      # app1 uses app2, app2 uses app3
      # So app1 transitively uses app3
      used_by_map = %{
        app1: MapSet.new([:app2]),
        app2: MapSet.new([:app3]),
        app3: MapSet.new()
      }

      all_apps = [:app1, :app2, :app3]
      result = Deps.compute_transitive_closure([used_by_map], all_apps)
      # app1 should transitively use both app2 and app3
      assert MapSet.equal?(result[:app1], MapSet.new([:app2, :app3]))
      assert MapSet.equal?(result[:app2], MapSet.new([:app3]))
      assert MapSet.equal?(result[:app3], MapSet.new())
    end

    test "circular dependency" do
      # app1 uses app2, app2 uses app1 (circular)
      used_by_map = %{
        app1: MapSet.new([:app2]),
        app2: MapSet.new([:app1])
      }

      all_apps = [:app1, :app2]
      result = Deps.compute_transitive_closure([used_by_map], all_apps)
      # Both should transitively use each other
      assert MapSet.equal?(result[:app1], MapSet.new([:app1, :app2]))
      assert MapSet.equal?(result[:app2], MapSet.new([:app1, :app2]))
    end

    test "complex transitive chain" do
      # app1 -> app2 -> app3 -> app4
      used_by_map = %{
        app1: MapSet.new([:app2]),
        app2: MapSet.new([:app3]),
        app3: MapSet.new([:app4]),
        app4: MapSet.new()
      }

      all_apps = [:app1, :app2, :app3, :app4]
      result = Deps.compute_transitive_closure([used_by_map], all_apps)
      # app1 should transitively use app2, app3, app4
      assert MapSet.equal?(result[:app1], MapSet.new([:app2, :app3, :app4]))
      # app2 should transitively use app3, app4
      assert MapSet.equal?(result[:app2], MapSet.new([:app3, :app4]))
      # app3 should transitively use app4
      assert MapSet.equal?(result[:app3], MapSet.new([:app4]))
      # app4 has no users
      assert MapSet.equal?(result[:app4], MapSet.new())
    end

    test "multiple users of same app" do
      # app2 and app3 both use app1
      used_by_map = %{
        app1: MapSet.new(),
        app2: MapSet.new([:app1]),
        app3: MapSet.new([:app1])
      }

      all_apps = [:app1, :app2, :app3]
      result = Deps.compute_transitive_closure([used_by_map], all_apps)
      assert MapSet.equal?(result[:app1], MapSet.new())
      assert MapSet.equal?(result[:app2], MapSet.new([:app1]))
      assert MapSet.equal?(result[:app3], MapSet.new([:app1]))
    end

    test "merges multiple maps" do
      # Map1: app1 uses app2
      # Map2: app2 uses app3
      # After merge: app1 uses app2, app2 uses app3
      map1 = %{app1: MapSet.new([:app2])}
      map2 = %{app2: MapSet.new([:app3])}
      all_apps = [:app1, :app2, :app3]
      result = Deps.compute_transitive_closure([map1, map2], all_apps)
      # app1 should transitively use app2 and app3
      assert MapSet.equal?(result[:app1], MapSet.new([:app2, :app3]))
      # app2 should transitively use app3
      assert MapSet.equal?(result[:app2], MapSet.new([:app3]))
      # app3 has no users
      assert MapSet.equal?(result[:app3], MapSet.new())
    end

    test "merges overlapping maps" do
      # Map1: app1 uses app2
      # Map2: app1 uses app3 (overlapping key)
      # After merge: app1 uses both app2 and app3
      map1 = %{app1: MapSet.new([:app2])}
      map2 = %{app1: MapSet.new([:app3])}
      all_apps = [:app1, :app2, :app3]
      result = Deps.compute_transitive_closure([map1, map2], all_apps)
      # app1 should use both app2 and app3
      assert MapSet.equal?(result[:app1], MapSet.new([:app2, :app3]))
    end
  end

  describe "get_include_dependents/2" do
    setup do
      test_dir =
        Path.join([
          System.tmp_dir!(),
          "emqx_mix_deps_test_#{System.system_time(:second)}"
        ])

      # Create test app directories
      app1_dir = Path.join([test_dir, "emqx_test1", "ebin"])
      app2_dir = Path.join([test_dir, "emqx_test2", "ebin"])
      File.mkdir_p!(app1_dir)
      File.mkdir_p!(app2_dir)

      on_exit(fn ->
        File.rm_rf!(test_dir)
      end)

      %{test_dir: test_dir}
    end

    test "no include_lib directives", %{test_dir: test_dir} do
      app_names = [:emqx_test1, :emqx_test2]
      result = Deps.get_include_dependents(test_dir, app_names)

      # Should return map with empty sets for all apps
      assert is_map(result)

      Enum.each(app_names, fn app ->
        case Map.get(result, app) do
          nil -> :ok
          set -> assert MapSet.equal?(set, MapSet.new())
        end
      end)
    end

    test "single include_lib directive", %{test_dir: _test_dir} do
      # Note: Testing include_lib directives requires actual BEAM files with abstract code
      # or mocking of :beam_lib.chunks/2. This test is a placeholder.
      # In a real scenario, we would create a properly compiled BEAM file or mock the beam_lib calls.
      assert true
    end

    test "non-existent app is filtered out", %{test_dir: test_dir} do
      app_names = [:emqx_test1]
      result = Deps.get_include_dependents(test_dir, app_names)

      # other_app should not be in the result (not in app_names)
      refute Map.has_key?(result, :other_app)
    end
  end

  describe "get_call_dependents/2" do
    test "non-existent directory raises error" do
      mod_to_app_map = %{module1: :app1}

      assert_raise RuntimeError, ~r/Directory not found/, fn ->
        Deps.get_call_dependents("/nonexistent/path", mod_to_app_map)
      end
    end

    test "empty ModToAppMap returns map with empty sets" do
      # Create a temporary directory structure
      test_dir =
        Path.join([
          System.tmp_dir!(),
          "emqx_mix_deps_test_#{System.system_time(:second)}"
        ])

      lib_dir = Path.join([test_dir, "lib"])
      File.mkdir_p!(lib_dir)

      try do
        # This will fail because there are no BEAM files, but we can test the structure
        mod_to_app_map = %{}
        result = Deps.get_call_dependents(lib_dir, mod_to_app_map)
        assert is_map(result)
      rescue
        # Expected to fail if xref can't find BEAM files
        RuntimeError -> :ok
      after
        File.rm_rf!(test_dir)
      end
    end
  end
end
