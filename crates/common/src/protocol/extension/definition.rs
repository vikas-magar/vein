use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::{Result, VeinError};
use arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};

/// Stable registry key for one operator implementation.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperatorTypeId(Arc<str>);

impl OperatorTypeId {
    /// Creates a non-empty, namespaced operator type identifier.
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(VeinError::invalid_input("operator type id must not be empty"));
        }
        Ok(Self(value))
    }

    /// Returns the identifier text without copying it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OperatorTypeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Query-local identity for a node in an operator definition graph.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct OperatorNodeId(u64);

impl OperatorNodeId {
    /// Creates a node identity from the canonical plan's stable node number.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric node identity.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Streaming role declared by an operator factory.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperatorRole {
    /// Produces batches without consuming an upstream child.
    Source,
    /// Consumes child batches and may emit output batches.
    Transform,
    /// Consumes child batches and terminates the pipeline.
    Sink,
}

/// A child edge and the schema produced by that child.
#[derive(Clone)]
pub struct OperatorChild {
    /// Query-local identity of the child node.
    pub node: OperatorNodeId,
    /// Immutable child output schema.
    pub schema: SchemaRef,
}

/// Immutable operator instance metadata shared by all typed configurations.
#[derive(Clone)]
pub struct OperatorMetadata {
    /// Query-local identity of this operator node.
    pub node: OperatorNodeId,
    /// Streaming role expected from the registered factory.
    pub role: OperatorRole,
    /// Child nodes in the order their input ports consume them.
    pub children: Arc<[OperatorChild]>,
    /// Output schema; terminal sinks do not expose an output stream schema.
    pub output_schema: Option<SchemaRef>,
}

impl OperatorMetadata {
    /// Creates immutable child and schema metadata for one plan node.
    pub fn new(
        node: OperatorNodeId,
        role: OperatorRole,
        children: Vec<OperatorChild>,
        output_schema: Option<SchemaRef>,
    ) -> Result<Self> {
        let valid = match role {
            OperatorRole::Source => children.is_empty() && output_schema.is_some(),
            OperatorRole::Transform => !children.is_empty() && output_schema.is_some(),
            OperatorRole::Sink => !children.is_empty() && output_schema.is_none(),
        };
        if !valid {
            return Err(VeinError::invalid_input(format!(
                "operator metadata does not match its {role:?} role"
            )));
        }
        Ok(Self { node, role, children: children.into(), output_schema })
    }
}

/// Typed immutable operator configuration plus graph and schema metadata.
pub struct OperatorDefinition<D> {
    factory_id: OperatorTypeId,
    metadata: Arc<OperatorMetadata>,
    configuration: Arc<D>,
}

impl<D> Clone for OperatorDefinition<D> {
    fn clone(&self) -> Self {
        Self {
            factory_id: self.factory_id.clone(),
            metadata: Arc::clone(&self.metadata),
            configuration: Arc::clone(&self.configuration),
        }
    }
}

impl<D> OperatorDefinition<D> {
    /// Creates a typed operator definition without cloning its configuration.
    pub fn new(factory_id: OperatorTypeId, metadata: OperatorMetadata, configuration: D) -> Self {
        Self {
            factory_id,
            metadata: Arc::new(metadata),
            configuration: Arc::new(configuration),
        }
    }

    /// Returns the implementation key used to resolve this definition.
    pub fn factory_id(&self) -> &OperatorTypeId {
        &self.factory_id
    }

    /// Returns immutable child and schema metadata.
    pub fn metadata(&self) -> &OperatorMetadata {
        &self.metadata
    }

    /// Borrows the typed immutable configuration.
    pub fn configuration(&self) -> &D {
        &self.configuration
    }

    /// Clones only the configuration's shared owner.
    pub fn configuration_arc(&self) -> Arc<D> {
        Arc::clone(&self.configuration)
    }
}

/// Object-safe view used only at the factory registry boundary.
pub trait AnyOperatorDefinition: Any + Send + Sync {
    /// Returns the implementation key for registry lookup.
    fn factory_id(&self) -> &OperatorTypeId;
    /// Returns the immutable query-local graph and schema metadata.
    fn metadata(&self) -> &OperatorMetadata;
    /// Exposes typed configuration for one checked factory downcast.
    fn as_any(&self) -> &dyn Any;
}

impl<D: Any + Send + Sync> AnyOperatorDefinition for OperatorDefinition<D> {
    fn factory_id(&self) -> &OperatorTypeId { &self.factory_id }
    fn metadata(&self) -> &OperatorMetadata { &self.metadata }
    fn as_any(&self) -> &dyn Any { self }
}

impl TryFrom<String> for OperatorTypeId {
    type Error = VeinError;
    fn try_from(value: String) -> Result<Self> { Self::new(value) }
}

impl From<OperatorTypeId> for String {
    fn from(value: OperatorTypeId) -> Self { value.as_str().to_owned() }
}

