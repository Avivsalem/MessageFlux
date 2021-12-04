from abc import ABCMeta, abstractmethod
from threading import Event
from typing import Optional, Tuple, List

from baseservice.iodevices.common import Message, DeviceHeaders
from baseservice.utils import KwargsException


class InputTransaction(metaclass=ABCMeta):
    """
    this is the base class for input transaction objects.

    after reading a message from the device (using 'with_transaction=True') the reader should commit/rollback
    the transaction, to signal the device if the message is done processing or not
    """

    def __init__(self, device: 'InputDevice'):
        """

        :param device: the input device that returned that transaction
        """
        self._device: 'InputDevice' = device
        self._finished: Event = Event()

    @property
    def device(self) -> 'InputDevice':
        """
        :return: the input device that returned that transaction
        """
        return self._device

    @property
    def finished(self) -> bool:
        """
        :return: 'True' if the transaction committed/rolledback, 'False' otherwise
        """
        return self._finished.is_set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.rollback()  # TODO: should a transaction be auto-committed if no exception was raised?

    def wait_for_finish(self, timeout: Optional[float] = None) -> bool:
        """
        waits for the transaction to finish (commit/rollback).
        it timeout is not None, after timeout seconds, it will return self.finished

        :param timeout: the timeout (in seconds) to wait for the transaction to finish

        :return: the value of self.finished
        """
        return self._finished.wait(timeout=timeout)

    def commit(self):
        """
        commits the transaction
        """
        if self.finished:
            return
        self._commit()
        self._finished.set()

    def rollback(self):
        """
        rolls back the transaction
        """
        if self.finished:
            return
        self._rollback()
        self._finished.set()

    @abstractmethod
    def _commit(self):
        """
        this method should be implemented by child classes to actually perform the commit
        """
        pass

    @abstractmethod
    def _rollback(self):
        """
        this method should be implemented by child classes to actually perform the rollback
        """
        pass


class InputDeviceException(KwargsException):
    """
    a base exception class for all input device related exceptions
    """
    pass


class InputDevice(metaclass=ABCMeta):
    """
    this is the base class for input devices
    """

    def __init__(self, manager: 'InputDeviceManager', name: str):
        """

        :param manager: the input device manager that created this device
        :param name: the name of this device
        """
        self._manager = manager
        self._name = name

    @property
    def name(self) -> str:
        """
        :return: the name of this device
        """
        return self._name

    @property
    def manager(self) -> 'InputDeviceManager':
        """
        :return: the input device manager that created this device
        """
        return self._manager

    @abstractmethod
    def read_stream(self,
                    timeout: Optional[float] = 0,
                    with_transaction: bool = True) -> Tuple[Optional[Message],
                                                            Optional[DeviceHeaders],
                                                            Optional[InputTransaction]]:
        """
        this method returns a message from the device (should be implemented by child classes)

        :param timeout: an optional timeout (in seconds) to wait for the device to return a message.
        after 'timeout' seconds, if the device doesn't have a message to return, it will return (None, None)
        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: a tuple of (Message, DeviceHeaders, Transaction) or (None, None, None) if no message was available.
        the device headers, can contain extra information about the device that returned the message
        """
        pass

    def _validate_transactions(self, transactions: List[InputTransaction]):
        """
        validates that all transaction objects are from this device

        :param transactions: a list of transaction to validate
        """
        for transaction in transactions:
            if transaction.device is not self:
                raise InputDeviceException(f'Cannot manage transactions from another device: {transaction.device}',
                                           this_device=self,
                                           other_device=transaction.device)

    def commit_all(self, transactions: List[InputTransaction]):
        """
        this method tries commit more than one transaction at once.
        it may be overridden in child classes to perform optimized batch commit

        :param transactions: the list of transactions to commit
        """
        self._validate_transactions(transactions)
        for transaction in transactions:
            transaction.commit()

    def rollback_all(self, transactions: List[InputTransaction]):
        """
        this method tries rollback more than one transaction at once.
        it may be overridden in child classes to perform optimized batch rollback

        :param transactions: the list of transactions to rollback
        """
        self._validate_transactions(transactions)
        for transaction in transactions:
            transaction.rollback()


class InputDeviceManager(metaclass=ABCMeta):
    """
    this is the base class for input device managers. this class is used to create input devices.
    """

    @abstractmethod
    def get_input_device(self, name: str) -> InputDevice:
        """
        creates an input device. should be implemented in child classes

        :param name: the name of the input device to create
        :return: the created input device
        """
        pass


class InputTransactionScope(InputTransaction):
    """
    a helper class for reading several messages inside a transaction scope.
    """

    def __init__(self, device: InputDevice, with_transaction: bool = True):
        """

        :param device: the input device to read the messages from
        :param with_transaction: 'True' if we should actually use transactions, 'False' otherwise
        """
        super().__init__(device)
        self._with_transaction = with_transaction
        self._transactions: List[InputTransaction] = []

    @abstractmethod
    def read_stream(self, timeout: Optional[float] = 0) -> Tuple[Optional[Message],
                                                                 Optional[DeviceHeaders]]:
        message, device_headers, transaction = self.device.read_stream(timeout=timeout,
                                                                       with_transaction=self._with_transaction)

        if transaction is not None:
            self._transactions.append(transaction)

        return message, device_headers

    def _commit(self):
        """
        commits all the transactions in scope
        """
        self.device.commit_all(self._transactions)
        self._transactions.clear()

    def _rollback(self):
        """
        rolls back all the transaction in scope
        """
        self.device.rollback_all(self._transactions)
        self._transactions.clear()


class WrapperTransaction(InputTransaction):
    """
    wraps a transaction for an underlying device. used for wrapper devices.
    """

    def __init__(self, device: InputDevice, inner_transaction: InputTransaction):
        """
        :param device: the wrapper device for this transaction
        :param inner_transaction: the transaction to wrap
        """
        super().__init__(device)
        self._inner_transaction = inner_transaction

    def _commit(self):
        """
        commits the inner transaction
        """
        self._inner_transaction.commit()

    def _rollback(self):
        """
        rolls back the inner transaction
        """
        self._inner_transaction.rollback()


class NULLTransaction(InputTransaction):
    """
    a transaction object that does nothing. used as placeholder for some places
    """

    def _commit(self):
        pass

    def _rollback(self):
        pass
