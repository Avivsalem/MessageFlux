from abc import ABCMeta, abstractmethod
from threading import Event
from typing import Optional, Tuple, BinaryIO, Dict, Any, List

from baseservice.utils import KwargsException


class InputTransaction(metaclass=ABCMeta):
    def __init__(self, device: 'InputDevice'):
        self._device: 'InputDevice' = device
        self._finished: Event = Event()

    @property
    def device(self) -> 'InputDevice':
        return self._device

    @property
    def finished(self) -> bool:
        return self._finished.is_set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.rollback()  # TODO: should a transaction be auto-committed if no exception was raised?

    def wait_for_finish(self, timeout: Optional[float] = None):
        self._finished.wait(timeout=timeout)

    def commit(self):
        if self.finished:
            return
        self._commit()
        self._finished.set()

    def rollback(self):
        if self.finished:
            return
        self._rollback()
        self._finished.set()

    @abstractmethod
    def _commit(self):
        pass

    @abstractmethod
    def _rollback(self):
        pass


class InputDeviceException(KwargsException):
    pass


class InputDevice(metaclass=ABCMeta):
    def __init__(self, manager: 'InputDeviceManager', name: str):
        self._manager = manager
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def manager(self) -> 'InputDeviceManager':
        return self._manager

    @abstractmethod
    def read_stream(self,
                    timeout: Optional[float] = 0,
                    with_transaction: bool = True) -> Tuple[Optional[BinaryIO],
                                                            Optional[Dict[str, Any]],
                                                            Optional[InputTransaction]]:
        pass

    def _validate_transactions(self, transactions: List[InputTransaction]):
        for transaction in transactions:
            if transaction.device is not self:
                raise InputDeviceException(f'Cannot manage transactions from another device: {transaction.device}',
                                           this_device=self,
                                           other_device=transaction.device)

    def commit_all(self, transactions: List[InputTransaction]):
        self._validate_transactions(transactions)
        for transaction in transactions:
            transaction.commit()

    def rollback_all(self, transactions: List[InputTransaction]):
        self._validate_transactions(transactions)
        for transaction in transactions:
            transaction.rollback()


class InputDeviceManager(metaclass=ABCMeta):
    @abstractmethod
    def get_input_device(self, name: str) -> InputDevice:
        pass


class InputTransactionScope(InputTransaction):
    def __init__(self, device: InputDevice, with_transaction: bool = True):
        super().__init__(device)
        self._with_transaction = with_transaction
        self._transactions: List[InputTransaction] = []

    @abstractmethod
    def read_stream(self,
                    timeout: Optional[float] = 0) -> Tuple[Optional[BinaryIO],
                                                           Optional[Dict[str, Any]]]:
        stream, headers, transaction = self.device.read_stream(timeout=timeout,
                                                               with_transaction=self._with_transaction)
        if transaction is not None:
            self._transactions.append(transaction)

        return stream, headers

    def _commit(self):
        self.device.commit_all(self._transactions)
        self._transactions.clear()

    def _rollback(self):
        self.device.rollback_all(self._transactions)
        self._transactions.clear()


class WrapperTransaction(InputTransaction):
    def __init__(self, device: InputDevice, inner_transaction: InputTransaction):
        super().__init__(device)
        self._inner_transaction = inner_transaction

    def _commit(self):
        self._inner_transaction.commit()

    def _rollback(self):
        self._inner_transaction.rollback()


class NULLTransaction(InputTransaction):
    def _commit(self):
        pass

    def _rollback(self):
        pass
