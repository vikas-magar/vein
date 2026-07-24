#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DataType {
    Boolean,
    Int,
    TinyInt,
    SmallInt,
    BigInt,
    Float,
    Double,
    Decimal { precision: u8, scale: u8 },
    String,
    Char(u32),
    Varchar(u32),
    Binary,
    Varbinary(u32),
    Date,
    Time,
    Timestamp { with_tz: bool },
    Interval { fields: IntervalField },
    Array(Box<DataType>),
    Struct(Vec<(String, DataType)>),
    Map { key: Box<DataType>, value: Box<DataType> },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    YearToMonth,
    DayToSecond,
}

impl std::str::FromStr for DataType {
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
            "interval" => Ok(Self::Interval { fields: IntervalField::DayToSecond }),
            _ => Err(format!("unknown data type: {}", s)),
        }
    }
}
