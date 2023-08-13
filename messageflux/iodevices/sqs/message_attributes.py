import json

from typing import Any, TypedDict


try:
    from mypy_boto3_sqs.type_defs import MessageAttributeValueTypeDef
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[sqs]') from ex


def get_aws_data_type(value: Any) -> str:
    if isinstance(value, (list, set, frozenset, tuple)):
        return "String.Array"
    elif isinstance(value, bool):
        return "String"
    elif isinstance(value, (int, float)):
        return "Number"
    elif isinstance(value, bytes):
        return "Binary"
    else:
        return "String"


def geterate_message_attributes(
    attributes: dict[str, Any]
) -> dict[str, MessageAttributeValueTypeDef]:
    return {
        key: {
            "DataType": get_aws_data_type(value),
            "StringValue": json.dumps(value)
            if not isinstance(value, str)
            else value, # to avoid double encoding
        }
        for key, value in attributes.items()
    }