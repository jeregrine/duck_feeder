integration_enabled? = System.get_env("RUN_INTEGRATION_TESTS") in ["1", "true"]

exclude_tags = [provider_integration: true, ecto_integration: true]

exclude_tags =
  if integration_enabled?, do: exclude_tags, else: Keyword.put(exclude_tags, :integration, true)

ExUnit.start(exclude: exclude_tags)
