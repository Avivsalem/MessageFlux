from baseservice.iodevices.base.input_devices import InputDevice, ReadMessageResult, InputDeviceManager
from baseservice.iodevices.base.input_transaction import InputTransaction


class _NullTransactionInputDevice(InputDevice):
    def _read_message(self, timeout: float = 0, with_transaction: bool = True) -> 'ReadMessageResult':
        return None, None, None


class _NullTransactionInputDeviceManager(InputDeviceManager):
    def get_input_device(self, name: str) -> '_NullTransactionInputDevice':
        return _NullTransactionInputDevice(self, name)


class _NULLTransaction(InputTransaction[_NullTransactionInputDevice]):
    """
    a transaction object that does nothing. used as placeholder for some places
    """
    __NULL_TRANSACTION_INPUT_DEVICE_MANAGER = _NullTransactionInputDeviceManager()
    __NULL_TRANSACTION_INPUT_DEVICE = __NULL_TRANSACTION_INPUT_DEVICE_MANAGER.get_input_device(
        'NULL_TRANSACTION_INPUT_DEVICE'
    )

    def __init__(self) -> None:
        super().__init__(self.__NULL_TRANSACTION_INPUT_DEVICE)

    def _commit(self) -> None:
        pass

    def _rollback(self) -> None:
        pass


NULL_TRANSACTION = _NULLTransaction()
