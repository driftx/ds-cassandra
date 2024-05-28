/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.config;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.service.reads.range.EndpointGroupingRangeCommandIterator;

/** A class that extracts system properties for the cassandra node it runs within. */
public enum CassandraRelevantProperties
{
    //base JVM properties
    JAVA_HOME("java.home"),
    CASSANDRA_PID_FILE ("cassandra-pidfile"),

    /**
     * Indicates the temporary directory used by the Java Virtual Machine (JVM)
     * to create and store temporary files.
     */
    JAVA_IO_TMPDIR ("java.io.tmpdir"),

    /**
     * Path from which to load native libraries.
     * Default is absolute path to lib directory.
     */
    JAVA_LIBRARY_PATH ("java.library.path"),

    JAVA_SECURITY_EGD ("java.security.egd"),

    /** Java Runtime Environment version */
    JAVA_VERSION ("java.version"),

    /** Java Virtual Machine implementation name */
    JAVA_VM_NAME ("java.vm.name"),

    /** Line separator ("\n" on UNIX). */
    LINE_SEPARATOR ("line.separator"),

    /** Java class path. */
    JAVA_CLASS_PATH ("java.class.path"),

    /** Operating system architecture. */
    OS_ARCH ("os.arch"),

    /** Operating system name. */
    OS_NAME ("os.name"),

    /** User's home directory. */
    USER_HOME ("user.home"),

    /** Platform word size sun.arch.data.model. Examples: "32", "64", "unknown"*/
    SUN_ARCH_DATA_MODEL ("sun.arch.data.model"),

    //JMX properties
    /**
     * The value of this property represents the host name string
     * that should be associated with remote stubs for locally created remote objects,
     * in order to allow clients to invoke methods on the remote object.
     */
    JAVA_RMI_SERVER_HOSTNAME ("java.rmi.server.hostname"),

    /**
     * If this value is true, object identifiers for remote objects exported by this VM will be generated by using
     * a cryptographically secure random number generator. The default value is false.
     */
    JAVA_RMI_SERVER_RANDOM_ID ("java.rmi.server.randomIDs"),

    /**
     * This property indicates whether password authentication for remote monitoring is
     * enabled. By default it is disabled - com.sun.management.jmxremote.authenticate
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE ("com.sun.management.jmxremote.authenticate"),


    /**
     * Controls the JMX server threadpool keap-alive time.
     * Should only be set by in-jvm dtests.
     */
    SUN_RMI_TRANSPORT_TCP_THREADKEEPALIVETIME("sun.rmi.transport.tcp.threadKeepAliveTime"),

    /**
     * Controls the distributed garbage collector lease time for JMX objects.
     * Should only be set by in-jvm dtests.
     */
    JAVA_RMI_DGC_LEASE_VALUE_IN_JVM_DTEST("java.rmi.dgc.leaseValue"),

    /**
     * The port number to which the RMI connector will be bound - com.sun.management.jmxremote.rmi.port.
     * An Integer object that represents the value of the second argument is returned
     * if there is no port specified, if the port does not have the correct numeric format,
     * or if the specified name is empty or null.
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT ("com.sun.management.jmxremote.rmi.port", "0"),

    /** Cassandra jmx remote port */
    CASSANDRA_JMX_REMOTE_PORT("cassandra.jmx.remote.port"),

    /** Cassandra jmx local port */
    CASSANDRA_JMX_LOCAL_PORT("cassandra.jmx.local.port"),

    /** This property  indicates whether SSL is enabled for monitoring remotely. Default is set to false. */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL ("com.sun.management.jmxremote.ssl"),

    /**
     * This property indicates whether SSL client authentication is enabled - com.sun.management.jmxremote.ssl.need.client.auth.
     * Default is set to false.
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH ("com.sun.management.jmxremote.ssl.need.client.auth"),

    /**
     * This property indicates the location for the access file. If com.sun.management.jmxremote.authenticate is false,
     * then this property and the password and access files, are ignored. Otherwise, the access file must exist and
     * be in the valid format. If the access file is empty or nonexistent, then no access is allowed.
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_ACCESS_FILE ("com.sun.management.jmxremote.access.file"),

    /** This property indicates the path to the password file - com.sun.management.jmxremote.password.file */
    COM_SUN_MANAGEMENT_JMXREMOTE_PASSWORD_FILE ("com.sun.management.jmxremote.password.file"),

    /** Port number to enable JMX RMI connections - com.sun.management.jmxremote.port */
    COM_SUN_MANAGEMENT_JMXREMOTE_PORT ("com.sun.management.jmxremote.port"),

    /**
     * A comma-delimited list of SSL/TLS protocol versions to enable.
     * Used in conjunction with com.sun.management.jmxremote.ssl - com.sun.management.jmxremote.ssl.enabled.protocols
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS ("com.sun.management.jmxremote.ssl.enabled.protocols"),

    /**
     * A comma-delimited list of SSL/TLS cipher suites to enable.
     * Used in conjunction with com.sun.management.jmxremote.ssl - com.sun.management.jmxremote.ssl.enabled.cipher.suites
     */
    COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES ("com.sun.management.jmxremote.ssl.enabled.cipher.suites"),

    /** mx4jaddress */
    MX4JADDRESS ("mx4jaddress"),

    /** mx4jport */
    MX4JPORT ("mx4jport"),

    RING_DELAY("cassandra.ring_delay_ms", "30000"),

    /**
     * When bootstraping we wait for all schema versions found in gossip to be seen, and if not seen in time we fail
     * the bootstrap; this property will avoid failing and allow bootstrap to continue if set to true.
     */
    BOOTSTRAP_SKIP_SCHEMA_CHECK("cassandra.skip_schema_check"),

    /**
     * When bootstraping how long to wait for schema versions to be seen.
     */
    BOOTSTRAP_SCHEMA_DELAY_MS("cassandra.schema_delay_ms"),

    /**
     * When draining, how long to wait for mutating executors to shutdown.
     */
    DRAIN_EXECUTOR_TIMEOUT_MS("cassandra.drain_executor_timeout_ms", String.valueOf(TimeUnit.MINUTES.toMillis(5))),

    /**
     * Gossip quarantine delay is used while evaluating membership changes and should only be changed with extreme care.
     */
    GOSSIPER_QUARANTINE_DELAY("cassandra.gossip_quarantine_delay_ms"),

    IGNORED_SCHEMA_CHECK_VERSIONS("cassandra.skip_schema_check_for_versions"),

    IGNORED_SCHEMA_CHECK_ENDPOINTS("cassandra.skip_schema_check_for_endpoints"),

    /**
     * Factory to create instances used during log transaction processing
     */
    LOG_TRANSACTIONS_FACTORY("cassandra.log_transactions_factory"),

    /**
     * When doing a host replacement its possible that the gossip state is "empty" meaning that the endpoint is known
     * but the current state isn't known.  If the host replacement is needed to repair this state, this property must
     * be true.
     */
    REPLACEMENT_ALLOW_EMPTY("cassandra.allow_empty_replace_address", "true"),

    /**
     * Whether {@link org.apache.cassandra.db.ConsistencyLevel#NODE_LOCAL} should be allowed.
     */
    ENABLE_NODELOCAL_QUERIES("cassandra.enable_nodelocal_queries", "false"),

    CONSISTENT_DIRECTORY_LISTINGS("cassandra.consistent_directory_listings", "false"),

    /**
     * To provide custom implementation to prioritize compaction tasks in UCS
     */
    UCS_COMPACTION_AGGREGATE_PRIORITIZER("unified_compaction.custom_compaction_aggregate_prioritizer"),

    /**
     * The handler of the storage of sstables, and possibly other files such as txn logs.
     */
    REMOTE_STORAGE_HANDLER("cassandra.remote_storage_handler"),

    /**
     * To provide a provider to a different implementation of the truncate statement.
     */
    TRUNCATE_STATEMENT_PROVIDER("cassandra.truncate_statement_provider"),

    /**
     * custom native library for os access
     */
    CUSTOM_NATIVE_LIBRARY("cassandra.custom_native_library"),

    /**
     * Repair progress reporter, default using system distributed keyspace
     */
    REPAIR_PROGRESS_REPORTER("cassandra.custom_repair_progress_reporter_class"),

    /**
     * Listen to repair parent session lifecycle
     */
    REPAIR_PARENT_SESSION_LISTENER("cassandra.custom_parent_repair_session_listener_class"),

    //cassandra properties (without the "cassandra." prefix)

    /**
     * The cassandra-foreground option will tell CassandraDaemon whether
     * to close stdout/stderr, but it's up to us not to background.
     * yes/null
     */
    CASSANDRA_FOREGROUND ("cassandra-foreground"),

    DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES ("default.provide.overlapping.tombstones"),
    ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION ("org.apache.cassandra.disable_mbean_registration"),
    //only for testing
    ORG_APACHE_CASSANDRA_CONF_CASSANDRA_RELEVANT_PROPERTIES_TEST("org.apache.cassandra.conf.CassandraRelevantPropertiesTest"),
    ORG_APACHE_CASSANDRA_DB_VIRTUAL_SYSTEM_PROPERTIES_TABLE_TEST("org.apache.cassandra.db.virtual.SystemPropertiesTableTest"),

    /** This property indicates whether disable_mbean_registration is true */
    IS_DISABLED_MBEAN_REGISTRATION("org.apache.cassandra.disable_mbean_registration"),

    /** what class to use for mbean registeration */
    MBEAN_REGISTRATION_CLASS("org.apache.cassandra.mbean_registration_class"),

    /** This property indicates if the code is running under the in-jvm dtest framework */
    DTEST_IS_IN_JVM_DTEST("org.apache.cassandra.dtest.is_in_jvm_dtest"),

    /** Represents the maximum size (in bytes) of a serialized mutation that can be cached **/
    CACHEABLE_MUTATION_SIZE_LIMIT("cassandra.cacheable_mutation_size_limit_bytes", Long.toString(1_000_000)),
    
    MIGRATION_DELAY("cassandra.migration_delay_ms", "60000"),
    /** Defines how often schema definitions are pulled from the other nodes */
    SCHEMA_PULL_INTERVAL_MS("cassandra.schema_pull_interval_ms", "60000"),
    /**
     * Minimum delay after a failed pull request before it is reattempted. It prevents reattempting failed requests
     * immediately as it is high chance they will fail anyway. It is better to wait a bit instead of flooding logs
     * and wasting resources.
     */
    SCHEMA_PULL_BACKOFF_DELAY_MS("cassandra.schema_pull_backoff_delay_ms", "3000"),

    /** When enabled, recursive directory deletion will be executed using a unix command `rm -rf` instead of traversing
     * and removing individual files. This is now used only tests, but eventually we will make it true by default.*/
    USE_NIX_RECURSIVE_DELETE("cassandra.use_nix_recursive_delete"),

    /** If set, {@link org.apache.cassandra.net.MessagingService} is shutdown abrtuptly without waiting for anything.
     * This is an optimization used in unit tests becuase we never restart a node there. The only node is stopoped
     * when the JVM terminates. Therefore, we can use such optimization and not wait unnecessarily. */
    NON_GRACEFUL_SHUTDOWN("cassandra.test.messagingService.nonGracefulShutdown"),

    /** Flush changes of {@link org.apache.cassandra.schema.SchemaKeyspace} after each schema modification. In production,
     * we always do that. However, tests which do not restart nodes may disable this functionality in order to run
     * faster. Note that this is disabled for unit tests but if an individual test requires schema to be flushed, it
     * can be also done manually for that particular case: {@code flush(SchemaConstants.SCHEMA_KEYSPACE_NAME);}. */
    FLUSH_LOCAL_SCHEMA_CHANGES("cassandra.test.flush_local_schema_changes", "true"),

    /**
     * Delay before checking if gossip is settled.
     */
    GOSSIP_SETTLE_MIN_WAIT_MS("cassandra.gossip_settle_min_wait_ms", "5000"),

    /**
     * Interval delay between checking gossip is settled.
     */
    GOSSIP_SETTLE_POLL_INTERVAL_MS("cassandra.gossip_settle_interval_ms", "1000"),

    /**
     * Number of polls without gossip state change to consider gossip as settled.
     */
    GOSSIP_SETTLE_POLL_SUCCESSES_REQUIRED("cassandra.gossip_settle_poll_success_required", "3"),
    
    /** Which class to use for token metadata provider */
    CUSTOM_TMD_PROVIDER_PROPERTY("cassandra.custom_token_metadata_provider_class"),

    /** Which class to use for failure detection */
    CUSTOM_FAILURE_DETECTOR_PROPERTY("cassandra.custom_failure_detector_class"),

    /** Set this property to true in order to switch to micrometer metrics */
    USE_MICROMETER("cassandra.use_micrometer_metrics", "false"),

    /** Set this property to true in order to use DSE-like histogram bucket boundaries and behaviour */
    USE_DSE_COMPATIBLE_HISTOGRAM_BOUNDARIES("cassandra.use_dse_compatible_histogram_boundaries", "false"),

    /** Which class to use for coordinator client request metrics */
    CUSTOM_CLIENT_REQUEST_METRICS_PROVIDER_PROPERTY("cassandra.custom_client_request_metrics_provider_class"),

    /**
     * Which class to use for messaging metrics for {@link org.apache.cassandra.net.MessagingService}.
     * The provided class name must point to an implementation of {@link org.apache.cassandra.metrics.MessagingMetrics}.
     */
    CUSTOM_MESSAGING_METRICS_PROVIDER_PROPERTY("cassandra.custom_messaging_metrics_provider_class"),

    /**
     * Which class to use for creating guardrails
     */
    CUSTOM_GUARDRAILS_FACTORY_PROPERTY("cassandra.custom_guardrails_factory_class"),

    /**
     * Used to support directory creation for different file system and remote/local conversion
     */
    CUSTOM_STORAGE_PROVIDER("cassandra.custom_storage_provider"),

    /** Watcher used when opening sstables to discover extra components, eg. archive component */
    CUSTOM_SSTABLE_WATCHER("cassandra.custom_sstable_watcher"),

    /** Controls the maximum top-k limit for vector search */
    SAI_VECTOR_SEARCH_MAX_TOP_K("cassandra.sai.vector_search.max_top_k", "1000"),

    /**
     * Controls the maximum number of PrimaryKeys that will be read into memory at one time when ordering/limiting
     * the results of an ANN query constrained by non-ANN predicates.
     */
    SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE("cassandra.sai.vector_search.order_chunk_size", "100000"),

    /** Controls the hnsw vector cache size, in bytes, per index segment. 0 to disable */
    SAI_HNSW_VECTOR_CACHE_BYTES("cassandra.sai.vector_search.vector_cache_bytes", String.valueOf(4 * 1024 * 1024)),

    /** Whether to allow the user to specify custom options to the hnsw index */
    SAI_HNSW_ALLOW_CUSTOM_PARAMETERS("cassandra.sai.hnsw.allow_custom_parameters", "false"),

    /** Whether to validate terms that will be SAI indexed at the coordinator */
    SAI_VALIDATE_TERMS_AT_COORDINATOR("cassandra.sai.validate_terms_at_coordinator", "true"),

    /** Whether vector type only allows float vectors. True by default. **/
    VECTOR_FLOAT_ONLY("cassandra.float_only_vectors", "true"),
    /** Enables use of vector type. True by default. **/
    VECTOR_TYPE_ALLOWED("cassandra.vector_type_allowed", "true"),

    /**
     * Whether to disable auto-compaction
     */
    DISABLED_AUTO_COMPACTION_PROPERTY("cassandra.disabled_auto_compaction"),

    /** Which class to use for dynamic snitch severity values */
    DYNAMIC_SNITCH_SEVERITY_PROVIDER("cassandra.dynamic_snitch_severity_provider"),

    NEVER_PURGE_TOMBSTONES_PROPERTY("cassandra.never_purge_tombstones"),

    SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY("cassandra.system_distributed_replication_per_dc"),
    SYSTEM_DISTRIBUTED_NTS_DC_OVERRIDE_PROPERTY("cassandra.system_distributed_replication_dc_names"),

    // in OSS, when UUID based SSTable generation identifiers are enabled, they use TimeUUID
    // though, for CNDB we want to use ULID - this property allows for that
    // valid values for this property are: uuid, ulid
    SSTABLE_UUID_IMPL("cassandra.sstable.id.uuid_impl", "uuid"),

    /**
     * Name of a custom implementation of {@link org.apache.cassandra.service.Mutator}.
     */
    CUSTOM_MUTATOR_CLASS("cassandra.custom_mutator_class"),

    /** Whether to skip rewriting hints when original host id left the cluster */
    SKIP_REWRITING_HINTS_ON_HOST_LEFT("cassandra.hinted_handoff.skip_rewriting_hints_on_host_left"),

    CUSTOM_HINTS_HANDLER("cassandra.custom_hints_handler"),
    CUSTOM_HINTS_ENDPOINT_PROVIDER("cassandra.custom_hints_endpoint_provider"),

    USE_RANDOM_ALLOCATION_IF_NOT_SUPPORTED("cassandra.token_allocation.use_random_if_not_supported"),

    COMPACTION_HISTORY_ENABLED("cassandra.compaction_history_enabled", "true"),

    // Allows one to turn off cursors in compaction.
    CURSORS_ENABLED("cassandra.allow_cursor_compaction", "true"),

    SYNC_LAG_FACTOR("cassandra.commitlog_sync_block_lag_factor", "1.5"),

    CDC_STREAMING_ENABLED("cassandra.cdc.enable_streaming", "true"),
    // Default metric aggegration strategy for tables without aggregation explicitly set.
    TABLE_METRICS_DEFAULT_HISTOGRAMS_AGGREGATION("cassandra.table_metrics_default_histograms_aggregation", TableMetrics.MetricsAggregation.INDIVIDUAL.name()),
    // Determines if table metrics should be also exported to shared global metric
    TABLE_METRICS_EXPORT_GLOBALS("cassandra.table_metrics_export_globals", "true"),
    FILE_CACHE_SIZE_IN_MB("cassandra.file_cache_size_in_mb", "2048"),
    CUSTOM_HINTS_RATE_LIMITER_FACTORY("cassandra.custom_hints_rate_limiter_factory"),

    CUSTOM_INDEX_BUILD_DECIDER("cassandra.custom_index_build_decider"),

    // Allows admin to include only some system views (see two below)
    SYSTEM_VIEWS_INCLUDE_ALL("cassandra.system_view.include_all", "true"),
    //This only applies if include all is false
    SYSTEM_VIEWS_INCLUDE_LOCAL_AND_PEERS("cassandra.system_view.include_local_and_peers"),
    //This only applies if include all is false
    SYSTEM_VIEWS_INCLUDE_INDEXES("cassandra.system_view.include_indexes"),

    // Allow disabling deletions of corrupt index components for troubleshooting
    DELETE_CORRUPT_SAI_COMPONENTS("cassandra.sai.delete_corrupt_components", "true"),

    // Enables parallel index read.
    USE_PARALLEL_INDEX_READ("cassandra.index_read.parallel", "true"),
    PARALLEL_INDEX_READ_NUM_THREADS("cassandra.index_read.parallel_thread_num"),

    // Allows skipping advising the OS to free cached pages associated commitlog flushing
    COMMITLOG_SKIP_FILE_ADVICE("cassandra.commitlog.skip_file_advice"),

    // Changes the semantic of the "THREE" consistency level to mean "all but one"
    // i.e. that all replicas except for at most one in the cluster (across all DCs) must accept the write for it to be successful.
    THREE_MEANS_ALL_BUT_ONE("dse.consistency_level.three_means_all_but_one", "false"),
    /**
     * Allows to set a custom response messages handler for verbs {@link org.apache.cassandra.net.Verb#REQUEST_RSP} and
     * {@link org.apache.cassandra.net.Verb#FAILURE_RSP}.
     */
    CUSTOM_RESPONSE_VERB_HANDLER_PROVIDER("cassandra.custom_response_verb_handler_provider_class"),
    VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR("cassandra.sai.validate_max_term_size_at_coordinator"),
    CUSTOM_KEYSPACES_FILTER_PROVIDER("cassandra.custom_keyspaces_filter_provider_class"),

    LWT_LOCKS_PER_THREAD("cassandra.lwt_locks_per_thread", "1024"),

    CUSTOM_READ_OBSERVER_FACTORY("cassandra.custom_read_observer_factory_class"),
    /**
     * Whether to enable the use of {@link EndpointGroupingRangeCommandIterator}
     */
    RANGE_READ_ENDPOINT_GROUPING_ENABLED("cassandra.range_read_endpoint_grouping_enabled", "true"),
    /**
     * Allows to set custom current trie index format. This node will produce sstables in this format.
     */
    TRIE_INDEX_FORMAT_VERSION("cassandra.trie_index_format_version", "cc");

    CassandraRelevantProperties(String key, String defaultVal)
    {
        this.key = key;
        this.defaultVal = defaultVal;
    }

    CassandraRelevantProperties(String key)
    {
        this.key = key;
        this.defaultVal = null;
    }

    private final String key;
    private final String defaultVal;

    public String getKey()
    {
        return key;
    }

    /**
     * Gets the value of the indicated system property.
     * @return system property value if it exists, defaultValue otherwise.
     */
    public String getString()
    {
        String value = System.getProperty(key);

        return value == null ? defaultVal : STRING_CONVERTER.convert(value);
    }

    /**
     * Sets the property to its default value if a default value was specified. Remove the property otherwise.
     */
    public void reset()
    {
        if (defaultVal != null)
            System.setProperty(key, defaultVal);
        else
            System.getProperties().remove(key);
    }

    /**
     * Gets the value of a system property as a String.
     * @return system property String value if it exists, overrideDefaultValue otherwise.
     */
    public String getString(String overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return STRING_CONVERTER.convert(value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setString(String value)
    {
        System.setProperty(key, value);
    }

    /**
     * Gets the value of a system property as a boolean.
     * @return system property boolean value if it exists, false otherwise().
     */
    public boolean getBoolean()
    {
        String value = System.getProperty(key);

        return BOOLEAN_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a boolean.
     * @return system property boolean value if it exists, overrideDefaultValue otherwise.
     */
    public boolean getBoolean(boolean overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return BOOLEAN_CONVERTER.convert(value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setBoolean(boolean value)
    {
        System.setProperty(key, Boolean.toString(value));
    }

    /**
     * Gets the value of a system property as a int.
     * @return system property int value if it exists, defaultValue otherwise.
     */
    public int getInt()
    {
        String value = System.getProperty(key);

        return INTEGER_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a int.
     * @return system property int value if it exists, overrideDefaultValue otherwise.
     */
    public int getInt(int overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return INTEGER_CONVERTER.convert(value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setInt(int value)
    {
        System.setProperty(key, Integer.toString(value));
    }

    /**
     * Gets the value of a system property as a long.
     * @return system property long value if it exists, defaultValue otherwise.
     */
    public long getLong()
    {
        String value = System.getProperty(key);

        return LONG_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a long.
     * @return system property long value if it exists, overrideDefaultValue otherwise.
     */
    public long getLong(int overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return LONG_CONVERTER.convert(value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setLong(long value)
    {
        System.setProperty(key, Long.toString(value));
    }

    /**
     * Gets the value of a system property as a double.
     * @return system property double value if it exists, defaultValue otherwise.
     */
    public double getDouble()
    {
        String value = System.getProperty(key);

        return DOUBLE_CONVERTER.convert(value == null ? defaultVal : value);
    }

    /**
     * Gets the value of a system property as a double.
     * @return system property double value if it exists, overrideDefaultValue otherwise.
     */
    public double getDouble(double overrideDefaultValue)
    {
        String value = System.getProperty(key);
        if (value == null)
            return overrideDefaultValue;

        return DOUBLE_CONVERTER.convert(value);
    }

    /**
     * Sets the value into system properties.
     * @param value to set
     */
    public void setDouble(double value)
    {
        System.setProperty(key, Double.toString(value));
    }

    private interface PropertyConverter<T>
    {
        T convert(String value);
    }

    private static final PropertyConverter<String> STRING_CONVERTER = value -> value;

    private static final PropertyConverter<Boolean> BOOLEAN_CONVERTER = Boolean::parseBoolean;

    private static final PropertyConverter<Integer> INTEGER_CONVERTER = value ->
    {
        try
        {
            return Integer.decode(value);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("Invalid value for system property: " +
                                                           "expected integer value but got '%s'", value));
        }
    };

    private static final PropertyConverter<Long> LONG_CONVERTER = value ->
    {
        try
        {
            return Long.decode(value);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("Invalid value for system property: " +
                                                           "expected integer value but got '%s'", value));
        }
    };

    private static final PropertyConverter<Double> DOUBLE_CONVERTER = value ->
    {
        try
        {
            return Double.parseDouble(value);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("Invalid value for system property: " +
                                                           "expected double value but got '%s'", value));
        }
    };

    /**
     * @return whether a system property is present or not.
     */
    public boolean isPresent()
    {
        return System.getProperties().containsKey(key);
    }
}
