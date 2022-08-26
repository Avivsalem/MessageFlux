from abc import ABCMeta, abstractmethod
from threading import Event
from types import TracebackType
from typing import Optional, Tuple, List, Union, TypeVar, Type, overload, Generic, TYPE_CHECKING

from typing_extensions import TypeAlias, TypeGuard

if TYPE_CHECKING:
    from baseservice.iodevices.base.common import Message, DeviceHeaders
    from baseservice.iodevices.base.input_devices import InputDevice

TInputDevice = TypeVar('TInputDevice', bound='InputDevice')


class InputTransaction(Generic[TInputDevice], metaclass=ABCMeta):
    """
    this is the base class for input transaction objects.

    after reading a message from the device (using 'with_transaction=True') the reader should commit/rollback
    the transaction, to signal the device if the message is done processing or not
    """

    Self = TypeVar("Self", bound='InputTransaction')

    def __init__(self, device: TInputDevice):
        """

        :param device: the input device that returned that transaction
        """
        self._device = device
        self._finished: Event = Event()

    @property
    def device(self) -> TInputDevice:
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

    def __enter__(self: Self) -> Self:
        return self

    @overload
    def __exit__(self, exc_type: None, exc_val: None, exc_tb: None) -> None:
        ...

    @overload
    def __exit__(self, exc_type: Type[Exception], exc_val: Exception, exc_tb: TracebackType) -> None:
        ...

    def __exit__(self,
                 exc_type: Optional[Type[Exception]],
                 exc_val: Optional[Exception],
                 exc_tb: Optional[TracebackType]
                 ) -> None:
        if (exc_type is None) and (exc_val is None) and (exc_tb is None):
            self.commit()  # no exception, commit the transaction
        else:
            self.rollback()  # exception, rollback the transaction

    def wait_for_finish(self, timeout: Optional[float] = None) -> bool:
        """
        waits for the transaction to finish (commit/rollback).
        if timeout is not None, after timeout seconds, it will return self.finished

        :param timeout: the timeout (in seconds) to wait for the transaction to finish

        :return: the value of self.finished
        """
        return self._finished.wait(timeout=timeout)

    def commit(self) -> None:
        """
        commits the transaction
        """
        if self.finished:
            return
        self._commit()
        self._finished.set()

    def rollback(self) -> None:
        """
        rolls back the transaction
        """
        if self.finished:
            return
        self._rollback()
        self._finished.set()

    @abstractmethod
    def _commit(self) -> None:
        """
        this method should be implemented by child classes to actually perform the commit
        """
        pass

    @abstractmethod
    def _rollback(self) -> None:
        """
        this method should be implemented by child classes to actually perform the rollback
        """
        pass


EmptyInputTransactionScopeMessage: TypeAlias = Tuple[None, None]
NonEmptyInputTransactionScopeMessage: TypeAlias = Tuple['Message', 'DeviceHeaders']
InputTransactionScoptMessage: TypeAlias = Union[EmptyInputTransactionScopeMessage, NonEmptyInputTransactionScopeMessage]
EMPTY_INPUT_TRANSACTION_MESSAGE = (None, None)


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

    def read_message(self, timeout: float = 0) -> InputTransactionScoptMessage:
        message, device_headers, transaction = self.device.read_message(timeout=timeout,
                                                                        with_transaction=self._with_transaction)

        if transaction is not None:
            self._transactions.append(transaction)

        return message, device_headers

    @staticmethod
    def is_empty_message(message: InputTransactionScoptMessage) -> TypeGuard[EmptyInputTransactionScopeMessage]:
        return message == EmptyInputTransactionScopeMessage

    @staticmethod
    def is_non_empty_message(message: InputTransactionScoptMessage) -> TypeGuard[NonEmptyInputTransactionScopeMessage]:
        return message != EmptyInputTransactionScopeMessage

    def _commit(self) -> None:
        """
        commits all the transactions in scope
        """
        for transaction in self._transactions:
            transaction.commit()
        self._transactions.clear()

    def _rollback(self) -> None:
        """
        rolls back all the transaction in scope
        """
        for transaction in self._transactions:
            transaction.rollback()
        self._transactions.clear()
