use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! id_newtype {
    ($name:ident, $inner:ty, $vis:vis) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            Serialize,
            Deserialize,
            rkyv::Archive,
            rkyv::Serialize,
            rkyv::Deserialize,
        )]
        #[rkyv(derive(Debug, PartialEq, Eq))]
        #[doc = concat!("A newtype identifier for `", stringify!($name), "`.")]
        $vis struct $name($vis $inner);

        impl $name {
            /// Create a new random ID.
            pub fn new() -> Self {
                let mut rng = rand::thread_rng();
                Self(rng.r#gen())
            }

            /// Create from a raw inner value.
            pub fn from_raw(raw: $inner) -> Self {
                Self(raw)
            }

            /// Consume and return the inner raw value.
            pub fn into_raw(self) -> $inner {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{:x}", self.0)
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

id_newtype!(QueryId, u128, pub);
id_newtype!(TaskId, u128, pub);
id_newtype!(StageId, u64, pub);
id_newtype!(PartitionId, u16, pub);
id_newtype!(NodeId, u128, pub);

impl fmt::Display for ArchivedQueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0.to_native())
    }
}

impl fmt::Display for ArchivedTaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0.to_native())
    }
}

impl fmt::Display for ArchivedStageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0.to_native())
    }
}

impl fmt::Display for ArchivedPartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0.to_native())
    }
}

impl fmt::Display for ArchivedNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x}", self.0.to_native())
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        let qid = QueryId::from_raw(0xdeadbeef);
        assert_eq!(qid.to_string(), "deadbeef");
    }

    #[test]
    fn test_hash_map_key() {
        use std::collections::HashMap;
        let mut map: HashMap<QueryId, &str> = HashMap::new();
        let id = QueryId::new();
        map.insert(id, "query1");
        assert_eq!(*map.get(&id).unwrap(), "query1");
    }

    #[test]
    fn test_random_ids_are_unique() {
        let a = QueryId::new();
        let b = QueryId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn test_serde_roundtrip() {
        let id = QueryId::from_raw(42);
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: QueryId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_from_raw_into_raw() {
        let id = QueryId::from_raw(12345u128);
        assert_eq!(id.into_raw(), 12345);
    }

    #[test]
    fn test_partition_id_range() {
        let p = PartitionId::from_raw(65535);
        assert_eq!(p.into_raw(), 65535);
    }

    #[test]
    fn test_stage_id_default() {
        let default = StageId::default();
        assert_ne!(default.into_raw(), 0);
    }

    #[test]
    fn test_archived_display_and_debug() {
        let id = QueryId::from_raw(0xdeadbeef);
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&id).unwrap();
        let archived = rkyv::access::<ArchivedQueryId, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(archived.to_string(), "deadbeef");
        assert_eq!(format!("{archived:?}"), "ArchivedQueryId(3735928559)");
    }
}
