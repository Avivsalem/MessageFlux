import json
import numbers
from typing import Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import MessageAttributeValueQueueTypeDef, MessageAttributeValueTypeDef


def get_aws_data_type(value: Any) -> str:
    if isinstance(value, str):
        return "String"
    elif isinstance(value, numbers.Number):
        return "Number"
    elif isinstance(value, bytes):
        return "Binary"
    elif isinstance(value, bool):
        return "String.bool"
    elif isinstance(value, (list, set, frozenset, tuple)):
        return "String.list"
    else:
        return "String.other"


def generate_message_attributes(headers: Dict[str, Any]) -> Dict[str, 'MessageAttributeValueQueueTypeDef']:
    results: Dict[str, 'MessageAttributeValueQueueTypeDef'] = {}
    for key, value in headers.items():
        data_type = get_aws_data_type(value)
        results[key] = {"DataType": data_type}

        if data_type == "String":
            results[key]["StringValue"] = value

        elif data_type == "Binary":
            results[key]["BinaryValue"] = value

        else:
            results[key]["StringValue"] = json.dumps(value)

    return results


def decode_message_attributes(message_attributes: Dict[str, 'MessageAttributeValueTypeDef']) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in message_attributes.items():
        decoded_val: Any
        if value["DataType"] == "String":
            decoded_val = value["StringValue"]

        elif value["DataType"] == "Binary":
            decoded_val = value["BinaryValue"]
        else:
            decoded_val = json.loads(value["StringValue"])

        result[key] = decoded_val

    return result
