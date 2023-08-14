import json

from typing import Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import MessageAttributeValueQueueTypeDef


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


def generate_message_attributes(attributes: Dict[str, Any]) -> Dict[str, 'MessageAttributeValueQueueTypeDef']:
    return {
        key: {
            "DataType": get_aws_data_type(value),
            "StringValue": value if isinstance(value, str) else json.dumps(value)  # to avoid double encoding
        }
        for key, value in attributes.items()
    }
