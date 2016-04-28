Embulk::JavaPlugin.register_filter(
  "split_column", "org.embulk.filter.split_column.SplitColumnFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
