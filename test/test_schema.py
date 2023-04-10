#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

# from typing import Dict, Any, Optional, List
# from abc import ABC, abstractmethod
# from jsonschema import validate, Draft202012Validator, ValidationError, validators, FormatChecker
# from datetime import datetime

from dltools.schema import infer_schema, StructType, INT, NUMBER

def main():
    schema = StructType(additionalProperties=True).\
        field("x", INT).\
        field("y", NUMBER)
    print(schema.schema())

    schema = infer_schema({"x": 1})
    print(schema.schema())

if __name__ == '__main__':
    main()
