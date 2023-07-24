import threading
from abc import ABCMeta, abstractmethod
from enum import Enum, unique
from threading import Event
from typing import Optional, List, TYPE_CHECKING

from messageflux.utils import KwargsException

if TYPE_CHECKING:
    from messageflux.iodevices.base.input_devices import InputDevice, ReadResult


class WrongTransactionStateException(KwargsException):
    """
    this exception is raised when transaction is in the wrong state for operation
    """
    pass


@unique
class TransactionState(Enum):
    """
    represents the current state of the transaction
    """
    ACTIVE = 'ACTIVE'
    COMMITTED = 'COMMITTED'
    ROLLEDBACK = 'ROLLEDBACK'


class InputTransaction(metaclass=ABCMeta):
    """
    this is the base class for input transaction objects.

    after reading a message from the device (using 'with_transaction=True') the reader should commit/rollback
    the transaction, to signal the device if the message is done processing or not

    calling commit/rollback on a transaction, finishes it. calling commit or rollback multiple times on a transaction
    is legal but will only execute once. calling rollback after commit or vice versa, will raise an exception

    if using the transaction as context (i.e 'with transaction:') it will commit at the end of the context.
    if an exception was raised during the context, the transaction will be rolled back.
    """

    def __init__(self, device: 'InputDevice'):
        """
        :param device: the input device that returned that transaction
        """
        self._device: 'InputDevice' = device
        self._finished: Event = Event()
        self._state: TransactionState = TransactionState.ACTIVE

    @property
    def state(self) -> TransactionState:
        """
        the current state of the transaction
        """
        return self._state

    @property
    def device(self) -> 'InputDevice':
        """
        :return: the input device that returned that transaction
        """
        return self._device

    @property
    def finished(self) -> bool:
        """
        :return: 'True' if the transaction was committed/rolled-back, 'False' otherwise
        """
        return self._finished.is_set()

    def __enter__(self):
        if self.finished:
            raise WrongTransactionStateException('Cannot enter an already finished transaction')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.finished:
            return
        if (exc_type, exc_val, exc_tb) == (None, None, None):
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
        if self.state == TransactionState.ROLLEDBACK:
            raise WrongTransactionStateException('Transaction Was Already Rolled Back')
        if self.state == TransactionState.COMMITTED:
            return

        self._commit()
        self._state = TransactionState.COMMITTED
        self._finished.set()

    def rollback(self):
        """
        rolls back the transaction
        """
        if self.state == TransactionState.COMMITTED:
            raise WrongTransactionStateException('Transaction Was Already Committed')
        if self.state == TransactionState.ROLLEDBACK:
            return

        self._rollback()
        self._state = TransactionState.ROLLEDBACK
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

    def read_message(self,
                     cancellation_token: threading.Event,
                     timeout: Optional[float] = None) -> Optional['ReadResult']:
        """
        this method returns a message from the device, and adds its transaction to the transaction scope

        :param cancellation_token: the cancellation token for this service. this can be used to know if cancellation
        was requested

        :param timeout: an optional timeout (in seconds) to wait for the device to return a message.
        after 'timeout' seconds (None means block until message is available).
         if the device doesn't have a message to return after timeout seconds, it will return None

        :return: a ReadResult object or None if no message was available.
        the device headers can contain extra information about the device that returned the message
        """

        read_result = self.device.read_message(cancellation_token=cancellation_token,
                                               timeout=timeout,
                                               with_transaction=self._with_transaction)

        if read_result is not None:
            self._transactions.append(read_result.transaction)

        return read_result

    def _commit(self):
        """
        commits all the transactions in scope
        """
        for transaction in self._transactions:
            if not transaction.finished:  # allows someone to rollback individual transactions within committed scope
                transaction.commit()
        self._transactions.clear()

    def _rollback(self):
        """
        rolls back all the transaction in scope
        """
        for transaction in self._transactions:
            if not transaction.finished:  # allows someone to commit individual transactions within rolled back scope
                transaction.rollback()
        self._transactions.clear()


class NULLTransaction(InputTransaction):
    """
    a transaction object that does nothing. used as placeholder for reading with with_transaction=False
    """

    def _commit(self):
        pass

    def _rollback(self):
        pass
