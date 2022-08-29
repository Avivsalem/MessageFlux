from abc import ABCMeta, abstractmethod
from threading import Event
from typing import Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from baseservice.iodevices.base.input_devices import InputDevice, ReadResult


class InputTransaction(metaclass=ABCMeta):
    """
    this is the base class for input transaction objects.

    after reading a message from the device (using 'with_transaction=True') the reader should commit/rollback
    the transaction, to signal the device if the message is done processing or not
    """

    def __init__(self, device: Optional['InputDevice'] = None):
        """

        :param device: the input device that returned that transaction
        """
        self._device: Optional['InputDevice'] = device
        self._finished: Event = Event()

    @property
    def device(self) -> Optional['InputDevice']:
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
        if (exc_type is None) and (exc_val is None) and (exc_tb is None):
            self.commit()  # no exception, commit the transaction
        else:
            self.rollback()  # exception, rollback the transaction

    def wait_for_finish(self, timeout: Optional[float] = None) -> bool:
        """
        waits for the transaction to finish (commit/rollback).
        if timeout is not None, after timeout seconds, it will return finished

        :param timeout: the timeout (in seconds) to wait for the transaction to finish

        :return: the value of finished
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


class InputTransactionScope(InputTransaction):
    """
    a helper class for reading several messages inside a transaction scope.
    """

    def __init__(self, device: 'InputDevice', with_transaction: bool = True):
        """

        :param device: the input device to read the messages from
        :param with_transaction: 'True' if we should actually use transactions, 'False' otherwise
        """
        super().__init__(device)
        self._with_transaction = with_transaction
        self._transactions: List[InputTransaction] = []

    def read_message(self, timeout: Optional[float] = 0) -> Optional['ReadResult']:
        read_result = self.device.read_message(timeout=timeout, with_transaction=self._with_transaction)

        if read_result is not None:
            self._transactions.append(read_result.transaction)

        return read_result

    def _commit(self):
        """
        commits all the transactions in scope
        """
        for transaction in self._transactions:
            transaction.commit()
        self._transactions.clear()

    def _rollback(self):
        """
        rolls back all the transaction in scope
        """
        for transaction in self._transactions:
            transaction.rollback()
        self._transactions.clear()


class NULLTransaction(InputTransaction):
    """
    a transaction object that does nothing. used as placeholder for some places
    """

    def _commit(self):
        pass

    def _rollback(self):
        pass


NULL_TRANSACTION = NULLTransaction()
