/// A SQL data type, optionally parameterized (e.g. `DECIMAL(10,2)`).
///
/// Strings embedded in parameterized types borrow from the parsed source.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DataType<'a> {
    /// SQL `BOOLEAN`.
    Boolean,
    /// SQL `INT` (4 bytes).
    Int,
    /// SQL `TINYINT` (1 byte).
    TinyInt,
    /// SQL `SMALLINT` (2 bytes).
    SmallInt,
    /// SQL `BIGINT` (8 bytes).
    BigInt,
    /// SQL `FLOAT` (4-byte single precision).
    Float,
    /// SQL `DOUBLE` (8-byte double precision).
    Double,
    /// SQL `DECIMAL` with the given total `precision` and `scale`.
    Decimal {
        /// Total number of digits.
        precision: u8,
        /// Number of digits after the decimal point.
        scale: u8,
    },
    /// SQL `STRING` / `TEXT` / `VARCHAR` without a length bound.
    String,
    /// SQL `CHAR(n)` with a fixed length.
    Char(u32),
    /// SQL `VARCHAR(n)` with a maximum length.
    Varchar(u32),
    /// SQL `BINARY` fixed-length byte strings.
    Binary,
    /// SQL `VARBINARY(n)` variable-length byte strings.
    Varbinary(u32),
    /// SQL `DATE`.
    Date,
    /// SQL `TIME`.
    Time,
    /// SQL `TIMESTAMP`, optionally with a time zone.
    Timestamp {
        /// Whether the timestamp carries a time zone.
        with_tz: bool,
    },
    /// SQL `INTERVAL` restricted to the given `fields`.
    Interval {
        /// The interval field precision.
        fields: IntervalField,
    },
    #[serde(borrow)]
    /// SQL `ARRAY` of the element type.
    Array(Box<DataType<'a>>),
    /// SQL `VECTOR(n)` with `n` embedding dimensions.
    Vector(u32),
    #[serde(borrow)]
    /// SQL `STRUCT` with named, typed fields.
    Struct(Vec<(&'a str, DataType<'a>)>),
    /// SQL `MAP` with the given key and value types.
    Map {
        /// The key type.
        #[serde(borrow)]
        key: Box<DataType<'a>>,
        /// The value type.
        #[serde(borrow)]
        value: Box<DataType<'a>>,
    },
}

/// The field precision of an `INTERVAL` type.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum IntervalField {
    /// `INTERVAL YEAR`.
    Year,
    /// `INTERVAL MONTH`.
    Month,
    /// `INTERVAL DAY`.
    Day,
    /// `INTERVAL HOUR`.
    Hour,
    /// `INTERVAL MINUTE`.
    Minute,
    /// `INTERVAL SECOND`.
    Second,
    /// `INTERVAL YEAR TO MONTH`.
    YearToMonth,
    /// `INTERVAL DAY TO SECOND`.
    DayToSecond,
}

impl std::str::FromStr for DataType<'_> {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "boolean" | "bool" => Ok(Self::Boolean),
            "int" | "integer" => Ok(Self::Int),
            "tinyint" => Ok(Self::TinyInt),
            "smallint" => Ok(Self::SmallInt),
            "bigint" => Ok(Self::BigInt),
            "float" => Ok(Self::Float),
            "double" | "double precision" => Ok(Self::Double),
            "string" | "text" | "varchar" => Ok(Self::String),
            "date" => Ok(Self::Date),
            "time" => Ok(Self::Time),
            "timestamp" => Ok(Self::Timestamp { with_tz: false }),
            "timestamptz" | "timestamp with time zone" => Ok(Self::Timestamp { with_tz: true }),
            "binary" => Ok(Self::Binary),
            "interval" => Ok(Self::Interval {
                fields: IntervalField::DayToSecond,
            }),
            "vector" => Ok(Self::Vector(0)), // Default 0 dimensions if parsed blindly from a raw string
            _ => Err(format!("unknown data type: {s}")),
        }
    }
}
