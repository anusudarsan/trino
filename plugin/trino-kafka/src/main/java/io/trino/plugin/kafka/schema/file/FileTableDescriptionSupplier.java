/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kafka.schema.file;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.plugin.kafka.KafkaConfig;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.plugin.kafka.schema.MapBasedTableDescriptionSupplier;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.connector.SchemaTableName;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class FileTableDescriptionSupplier
        implements Provider<TableDescriptionSupplier>
{
    public static final String NAME = "file";

    private static final Logger log = Logger.get(FileTableDescriptionSupplier.class);

    private final JsonCodec<KafkaTopicDescription> topicDescriptionCodec;
    private final File tableDescriptionDir;
    private final String defaultSchema;
    private final Set<String> tableNames;

    @Inject
    FileTableDescriptionSupplier(FileTableDescriptionSupplierConfig config, KafkaConfig kafkaConfig, JsonCodec<KafkaTopicDescription> topicDescriptionCodec)
    {
        this.topicDescriptionCodec = requireNonNull(topicDescriptionCodec, "topicDescriptionCodec is null");
        this.tableDescriptionDir = config.getTableDescriptionDir();
        this.defaultSchema = kafkaConfig.getDefaultSchema();
        this.tableNames = ImmutableSet.copyOf(config.getTableNames());
    }

    @Override
    public TableDescriptionSupplier get()
    {
        Map<SchemaTableName, KafkaTopicDescription> tables = populateTables();
        return new MapBasedTableDescriptionSupplier(tables);
    }

    private Map<SchemaTableName, KafkaTopicDescription> populateTables()
    {
        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> builder = ImmutableMap.builder();

        log.debug("Loading kafka table definitions from %s", tableDescriptionDir.getAbsolutePath());

        for (File file : listFiles(tableDescriptionDir)) {
            if (file.isFile() && file.getName().endsWith(".json")) {
                KafkaTopicDescription table;
                try (InputStream stream = new FileInputStream(file)) {
                    table = topicDescriptionCodec.fromJson(stream);
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Failed to get table description file for Kafka: " + file, e);
                }
                String schemaName = table.schemaName().orElse(defaultSchema);
                log.debug("Kafka table %s.%s: %s", schemaName, table.tableName(), table);
                builder.put(new SchemaTableName(schemaName, table.tableName()), table);
            }
        }

        Map<SchemaTableName, KafkaTopicDescription> tableDefinitions = builder.buildOrThrow();

        log.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

        builder = ImmutableMap.builder();
        for (String definedTable : tableNames) {
            SchemaTableName tableName;
            try {
                tableName = parseTableName(definedTable);
            }
            catch (IllegalArgumentException iae) {
                tableName = new SchemaTableName(defaultSchema, definedTable);
            }

            if (tableDefinitions.containsKey(tableName)) {
                KafkaTopicDescription kafkaTable = tableDefinitions.get(tableName);
                log.debug("Found Table definition for %s: %s", tableName, kafkaTable);
                builder.put(tableName, kafkaTable);
            }
            else {
                // A dummy table definition only supports the internal columns.
                log.debug("Created dummy Table definition for %s", tableName);
                builder.put(tableName, new KafkaTopicDescription(
                        tableName.getTableName(),
                        Optional.ofNullable(tableName.getSchemaName()),
                        definedTable,
                        Optional.of(new KafkaTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), Optional.empty(), ImmutableList.of())),
                        Optional.of(new KafkaTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), Optional.empty(), ImmutableList.of()))));
            }
        }

        return builder.buildOrThrow();
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static SchemaTableName parseTableName(String schemaTableName)
    {
        checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
        List<String> parts = Splitter.on('.').splitToList(schemaTableName);
        checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
        return new SchemaTableName(parts.get(0), parts.get(1));
    }
}
