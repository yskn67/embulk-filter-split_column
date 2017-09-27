package org.embulk.filter.split_column;

import org.apache.commons.lang3.StringUtils;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Timestamps;

import org.embulk.spi.DataException;
import org.embulk.spi.time.TimestampParseException;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public class SplitColumnFilterPlugin
        implements FilterPlugin
{
    private static final Logger log = Exec.getLogger(SplitColumnFilterPlugin.class);

    public interface PluginTask
            extends Task, TimestampParser.Task
    {
        @Config("delimiter")
        @ConfigDefault("\",\"")
        public String getDelimiter();

        @Config("is_skip")
        @ConfigDefault("true")
        public Optional<Boolean> getIsSkip();

        @Config("target_key")
        public String getTargetKey();

        @Config("output_columns")
        public SchemaConfig getOutputColumns();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        String targetColumnName = task.getTargetKey();
        int i = 0;
        for (Column inputColumn: inputSchema.getColumns()) {
            String columnName = inputColumn.getName();
            if (columnName.equals(targetColumnName)) {
                // Separate target_key column
                for (ColumnConfig outputColumnConfig : task.getOutputColumns().getColumns()) {
                    Column outputColumn = outputColumnConfig.toColumn(i++);
                    builder.add(outputColumn);
                }
                continue;
            }
		    Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
            builder.add(outputColumn);
        }
        Schema outputSchema = new Schema(builder.build());
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final Column targetColumn = inputSchema.lookupColumn(task.getTargetKey());
        final TimestampParser[] timestampParsers = Timestamps.newTimestampColumnParsers(task, task.getOutputColumns());

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish() {
                builder.finish();
            }

            @Override
            public void close() {
                builder.close();
            }

            @Override
            public void add(Page page) {
                reader.setPage(page);
                int rowNum = 0;
                while (reader.nextRecord()) {
                    rowNum++;
                    String targetColumnValue = reader.getString(targetColumn);
                    String[] words = StringUtils.split(targetColumnValue, task.getDelimiter());
                    SchemaConfig outputSchemaConfig = task.getOutputColumns();
                    // check split values
                    if (outputSchemaConfig.size() != words.length) {
                        Boolean isSkip = task.getIsSkip().get();
                        if (isSkip.booleanValue()) {
                            String message = String.format("Skipped line %d: output_column has %d columns but value was separated in %d: \"%s\"",
                                rowNum,
                                outputSchemaConfig.size(),
                                words.length,
                                targetColumnValue
                            );
                            log.warn(message);
                            continue;
                        } else {
                            String message = String.format("output_column has %d columns but value was separated in %d: \"%s\"",
                                outputSchemaConfig.size(),
                                words.length,
                                targetColumnValue
                            );
                            throw new SplitColumnValidateException(message);
                        }
                    }
                    int colNum = 0;
                    for (Column column: inputSchema.getColumns()) {
                        if (column.getName().equals(targetColumn.getName())) {
                            // TODO: support default value
                            // TODO: throw exception
                            int i = 0;
                            for (ColumnConfig outputColumnConfig: outputSchemaConfig.getColumns()) {
                                Column outputColumn = outputSchema.lookupColumn(outputColumnConfig.getName());
                                Type outputColumnType = outputColumn.getType();
                                if (Types.STRING.equals(outputColumnType)) {
                                    builder.setString(colNum++, words[i++]);
                                } else if (Types.BOOLEAN.equals(outputColumnType)) {
                                    builder.setBoolean(colNum++, Boolean.parseBoolean(words[i++]));
                                } else if (Types.DOUBLE.equals(outputColumnType)) {
                                    builder.setDouble(colNum++, Double.parseDouble(words[i++]));
                                } else if (Types.LONG.equals(outputColumnType)) {
                                    builder.setLong(colNum++, Long.parseLong(words[i++]));
                                } else if (Types.TIMESTAMP.equals(outputColumnType)) {
                                    builder.setTimestamp(colNum++, timestampParsers[i].parse(words[i]));
                                    i++;
                                }
                            }
                            continue;
                        }
                        if (reader.isNull(column)) {
                            builder.setNull(colNum++);
                            continue;
                        }
                        add_builder(colNum++, column);
                    }
                    builder.addRecord();
                }
            }
            // TODO: use embulk-core system
            private void add_builder(int colNum, Column column) {
                if (Types.STRING.equals(column.getType())) {
                    builder.setString(colNum, reader.getString(column));
                } else if (Types.BOOLEAN.equals(column.getType())) {
                    builder.setBoolean(colNum, reader.getBoolean(column));
                } else if (Types.DOUBLE.equals(column.getType())) {
                    builder.setDouble(colNum, reader.getDouble(column));
                } else if (Types.LONG.equals(column.getType())) {
                    builder.setLong(colNum, reader.getLong(column));
                } else if (Types.TIMESTAMP.equals(column.getType())) {
                    builder.setTimestamp(colNum, reader.getTimestamp(column));
                }
            }
        };
    }

	static class SplitColumnValidateException
            extends DataException
    {
        SplitColumnValidateException(String message)
        {
            super(message);
        }
    }
}
