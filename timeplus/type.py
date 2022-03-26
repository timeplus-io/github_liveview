from enum import Enum


class Type(Enum):
    Integer = "integer"
    Decimal = "decimal"
    Float = "float"
    Bool = "bool"
    String = "string"
    Date = "date"
    Datetime = "datetime"
    Datetime64 = "datetime64"
    Array = "array"
    Map = "map"
    Tuple = "tuple"
