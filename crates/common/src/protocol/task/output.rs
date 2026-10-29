use super::*;

/// Key-value option pair for storage and scan configurations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyValue {
    /// Option key name.
    pub key: String,
    /// Option value.
    pub value: String,
}

impl KeyValue {
    /// Creates a new key-value pair.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Target for persistent storage output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageTarget {
    /// The path or table URI.
    pub destination_uri: String,
    /// Storage format and execution options.
    pub options: Vec<KeyValue>,
    /// Authoritative schema of columns to write.
    pub schema: VeinSchemaRef,
    /// Arrow IPC stream containing the complete authoritative output schema.
    /// This preserves nested types and allows sinks to initialize empty writes.
    pub schema_ipc: Vec<u8>,
}

impl StorageTarget {
    /// Creates a new StorageTarget.
    pub fn new(
        destination_uri: impl Into<String>,
        options: Vec<KeyValue>,
        schema: VeinSchemaRef,
    ) -> Self {
        Self {
            destination_uri: destination_uri.into(),
            options,
            schema,
            schema_ipc: Vec::new(),
        }
    }

    /// Sets the canonical Arrow IPC schema used by storage sinks.
    pub fn with_schema_ipc(mut self, schema_ipc: Vec<u8>) -> Self {
        self.schema_ipc = schema_ipc;
        self
    }

    /// Helper to find an option value by key (case-insensitive).
    pub fn get_option(&self, key: &str) -> Option<&str> {
        self.options
            .iter()
            .find(|kv| kv.key.eq_ignore_ascii_case(key))
            .map(|kv| kv.value.as_str())
    }

    /// Sets an option, replacing an existing key case-insensitively.
    pub fn set_option(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        if let Some(option) = self
            .options
            .iter_mut()
            .find(|option| option.key.eq_ignore_ascii_case(&key))
        {
            option.value = value.into();
        } else {
            self.options.push(KeyValue::new(key, value));
        }
    }
}

/// Sink for data produced by a stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataSink {
    /// Stream data directly back to the Query Coordinator/Client.
    Client {
        /// Schema of the columns expected at the client.
        schema: VeinSchemaRef,
    },
    /// Write data out to storage (e.g., Iceberg commit).
    Storage(StorageTarget),
    /// Discard the data (useful for some benchmarks or tests).
    Discard,
}
