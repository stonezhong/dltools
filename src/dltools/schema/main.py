from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod
from jsonschema import validate, Draft202012Validator

class JSONType(ABC):
    @property
    def validator(self):
        return Draft202012Validator(self.schema())

    @abstractmethod
    def schema(self):
        pass

    # you can use jt.validator.validate(data) to validate data
    
    @property
    def orNull(self):
        return AnyOf(self, NULL)

class BooleanType(JSONType):
    # for boolean
    def schema(self):
        return {"type": "boolean"}

class NumberType(JSONType):
    # for integer or float number
    def schema(self):
        return {"type": "number"}

class IntType(JSONType):
    # for integer number only
    def schema(self):
        return {"type": "integer"}

class StringType(JSONType):
    # for string
    def schema(self):
        return {"type": "string"}

class NullType(JSONType):
    # for null
    def schema(self):
        return {"type": "null"}

class ArrayType(JSONType):
    def __init__(self, item_type:Optional[JSONType]):
        self.item_type = item_type

    def schema(self):
        return {
            "type": "array",
            "items": self.item_type.schema()
        }

class Field:
    def __init__(self, name:str, type:JSONType):
        self.name = name
        self.type = type
    
class StructType(JSONType):
    def __init__(self, additionalProperties=False):
        self.field_dict:Dict[str, JSONType] = {}
        self.additionalProperties = additionalProperties

    def schema(self):
        return {
            "type": "object",
            "properties": {
                field_name: field_type.schema() for field_name, field_type in self.field_dict.items()
            },
            "additionalProperties": self.additionalProperties
        }
    
    # def field(self, name:str, type: JSONType):
    def field(self, *args):
        if len(args)==1:
            field_name = args[0].name
            field_type = args[0].type
        else:
            field_name = args[0]
            field_type = args[1]
        self.field_dict[field_name] = field_type
        return self



class AllOf(JSONType):
    def __init__(self, *args):
        self.children = args

    def schema(self):
        return {
            "allOf": [child.schema() for child in self.children]
        }

class AnyOf(JSONType):
    def __init__(self, *args):
        self.children = args

    def schema(self):
        return {
            "anyOf": [child.schema() for child in self.children]
        }

class OneOf(JSONType):
    def __init__(self, *args):
        self.children = args

    def schema(self):
        return {
            "oneOf": [child.schema() for child in self.children]
        }

class Not(JSONType):
    def __init__(self, child):
        self.child = child

    def schema(self):
        return {
            "not": self.child.schema()
        }

NUMBER  = NumberType()
INT     = IntType()
STRING  = StringType()
NULL    = NullType()
BOOLEAN = BooleanType()

def _find_instance_type(instance, *types):
    for type in types:
        if isinstance(instance, type):
            return type
    return None

def _split_anyof_in_strict_mode(schema):
    # in strict mode, we only uses AnyOf for NullType
    null_type = None
    non_null_type = None
    for type in schema.children:
        if isinstance(type, NullType):
            assert null_type is None
            null_type = type
        else:
            assert non_null_type is None
            non_null_type = type
    
    assert null_type is not None
    assert non_null_type is not None
    return null_type, non_null_type

class IncompatibleScehma(Exception):
    # Cannot infer schema since the data is incompatible against the existing schema
    pass

def _infer_schema(data:Any, schema:Optional[JSONType]=None, strict:bool=False) -> Optional[JSONType]:
    # We ignore empty array

    # infer schema from data
    if type(data) == int:
        # if original schema is int or number, no need to merge
        if schema is None:
            return INT
        if _find_instance_type(schema, IntType, NumberType):
            return schema
        if isinstance(schema, NullType):
            return AnyOf(schema, INT)
        if _find_instance_type(schema, BooleanType, StringType, ArrayType, StructType):
            if strict:
                raise IncompatibleScehma()
            return AnyOf(schema, INT)
        if isinstance(schema, AnyOf):
            if strict:
                null_type, non_null_type = _split_anyof_in_strict_mode(schema)
                if _find_instance_type(non_null_type, IntType, NumberType):
                    return schema # already included
                raise IncompatibleScehma()
            for child in schema.children:
                if isinstance(child, IntType) or isinstance(child, NumberType):
                    return schema
            return AnyOf(*schema.children, INT)
        assert False
    
    if type(data) == float:
        if schema is None or isinstance(schema, IntType):
            return NUMBER # upgrade
        if isinstance(schema, NumberType):
            return schema  # no need to upgrade
        if isinstance(schema, NullType):
            return AnyOf(schema, NUMBER)
        if _find_instance_type(schema, BooleanType, StringType, ArrayType, StructType):
            if strict:
                raise IncompatibleScehma()
            return AnyOf(schema, NUMBER)
        if isinstance(schema, AnyOf):
            if strict:
                null_type, non_null_type = _split_anyof_in_strict_mode(schema)
                if isinstance(non_null_type, IntType):
                    return AnyOf(null_type, NUMBER)
                if isinstance(non_null_type, NumberType):
                    return schema # already included
                raise IncompatibleScehma()
            return AnyOf(
                *[t for t in schema.children if not isinstance(t, IntType) and not isinstance(t, NumberType)],
                NUMBER
            )
        assert False
    
    if type(schema) == bool:
        if schema is None:
            return BOOLEAN
        if isinstance(schema, BooleanType):
            return schema  # no need to upgrade
        if isinstance(schema, NullType):
            return AnyOf(schema, BOOLEAN)
        if _find_instance_type(schema, NumberType, IntType, StringType, ArrayType, StructType):
            if strict:
                raise IncompatibleScehma()
            return AnyOf(schema, BOOLEAN)
        if isinstance(schema, AnyOf):
            if strict:
                null_type, non_null_type = _split_anyof_in_strict_mode(schema)
                if isinstance(non_null_type, BooleanType):
                    return schema # already included
                raise IncompatibleScehma()
            return AnyOf(
                *[t for t in schema.children if not isinstance(t, BooleanType)],
                BOOLEAN
            )
        assert False
        
    if type(data) == str:
        if schema is None:
            return STRING
        if isinstance(schema, StringType):
            return schema  # no need to upgrade
        if isinstance(schema, NullType):
            return AnyOf(schema, STRING)
        if _find_instance_type(schema, BooleanType, NumberType, IntType, ArrayType, StructType):
            if strict:
                raise IncompatibleScehma()
            return AnyOf(schema, STRING)
        if isinstance(schema, AnyOf):
            if strict:
                null_type, non_null_type = _split_anyof_in_strict_mode(schema)
                if isinstance(non_null_type, StringType):
                    return schema # already included
                raise IncompatibleScehma()
            return AnyOf(
                *[t for t in schema.children if not isinstance(t, StringType)],
                STRING
            )
        assert False
    
    if data is None:
        if schema is None:
            return NullType
        if isinstance(schema, NullType):
            return schema  # no need to upgrade
        if _find_instance_type(schema, BooleanType, NumberType, IntType, StringType, ArrayType, StructType):
            return AnyOf(schema, NULL)
        if isinstance(schema, AnyOf):
            if strict:
                # this schema should include NullType as child
                return schema
            return AnyOf(
                *[t for t in schema.children if not isinstance(t, NullType)],
                NULL
            )
        assert False
    
    if type(data) == list:
        # if data is empty list, we ignroe it since we cannot infer element type
        new_schema = None
        array_schema = None

        if schema is None:
            array_schema = ArrayType(None)
            new_schema = array_schema
        elif isinstance(schema, ArrayType):
            array_schema = schema
            new_schema = schema
        elif isinstance(schema, AnyOf):
            if strict:
                null_type, non_null_type = _split_anyof_in_strict_mode(schema)
                if not isinstance(non_null_type, ArrayType):
                    raise IncompatibleScehma()
                array_schema = non_null_type
                new_schema = schema
            else:
                children = []
                for t in schema.children:
                    if isinstance(t, ArrayType):
                        assert array_schema is None
                        array_schema = t
                    else:
                        children.append(t)
                if array_schema is None:
                    array_schema = ArrayType(None)
                    children.append(array_schema)
                new_schema = AnyOf(*children)
        elif isinstance(schema, NullType):
            array_schema = ArrayType(None)
            new_schema = AnyOf(schema, array_schema)
        elif _find_instance_type(schema, BooleanType, NumberType, IntType, StringType, StructType):
            if strict:
                raise IncompatibleScehma()
            array_schema = ArrayType(None)
            new_schema = AnyOf(schema, array_schema)
        
        assert new_schema is not None
        assert array_schema is not None
        
        item_schema = array_schema.item_type
        for v in data:
            item_schema = _infer_schema(v, schema=item_schema, strict=strict)
        array_schema.item_type = item_schema

        if array_schema.item_type is None:
            if isinstance(new_schema, ArrayType):
                return None
            if isinstance(new_schema, AnyOf):
                children = [child for child in new_schema.children if not isinstance(child, ArrayType)]
                if len(children) == 0:
                    return None
                if len(children) == 1:
                    return children[0]
                return AnyOf(*children)
            assert False
        
        return new_schema
    
    if type(data) == dict:
        new_schema = None
        struct_schema = None

        if schema is None:
            struct_schema = StructType()
            new_schema = struct_schema
        elif isinstance(schema, StructType):
            struct_schema = schema
            new_schema = struct_schema
        elif isinstance(schema, AnyOf):
            if strict:
                null_type, non_null_type = _split_anyof_in_strict_mode(schema)
                if not isinstance(non_null_type, StructType):
                    raise IncompatibleScehma()
                struct_schema = non_null_type
                new_schema = schema
            else:
                children = []
                for t in schema.children:
                    if isinstance(t, StructType):
                        assert struct_schema is None
                        struct_schema = t
                    else:
                        children.append(t)
                if struct_schema is None:
                    struct_schema = StructType()
                    children.append(struct_schema)
                new_schema = AnyOf(*children)
        elif isinstance(schema, NullType):
            struct_schema = StructType()
            new_schema = AnyOf(schema, struct_schema)
        elif _find_instance_type(schema, BooleanType, NumberType, IntType, StringType, ArrayType):
            if strict:
                raise IncompatibleScehma()
            struct_schema = StructType()
            new_schema = AnyOf(schema, struct_schema)

        assert new_schema is not None
        assert struct_schema is not None

        for key, value in data.items():
            field_schema = struct_schema.field_dict.get(key)
            field_schema = _infer_schema(value, schema=field_schema, strict=strict)
            if field_schema is not None:
                struct_schema.field_dict[key] = field_schema

        return new_schema


def infer_schema(*data:Any, schema:Optional[JSONType]=None, strict:bool=False) -> Optional[JSONType]:
    s = schema
    for d in data:
        s = _infer_schema(d, schema=s, strict=strict)
    return s
