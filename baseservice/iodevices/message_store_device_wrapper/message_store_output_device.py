from baseservice.iodevices.base import OutputDeviceException, OutputDeviceManager
from baseservice.iodevices.base.common import MessageBundle, Message
from baseservice.iodevices.message_store_device_wrapper.message_store_base import MessageStoreBase
from baseservice.iodevices.message_store_device_wrapper.message_store_transformer_base import \
    _MessageStoreTransformerBase
from baseservice.iodevices.transformer_device_wrapper import OutputTransformerBase
from baseservice.iodevices.transformer_device_wrapper.transformer_output_device import TransformerOutputDevice, \
    TransformerOutputDeviceManager


class MessageStoreOutputTransformer(_MessageStoreTransformerBase, OutputTransformerBase):
    """
    this header can be added to send_stream in order to control whether the message store is used or not.
    'True' means that we always use the message store
    'False' means that we don't uses the message store (unless message size is over 'always_store_threshold')

    if the header does not exist, the message store is used only if the message size is over 'size_threshold'
    """

    USE_MESSAGE_STORE_HEADER_NAME = '__USE_MESSAGE_STORE_HEADER_NAME__'
    ORIGINAL_MESSAGE_SIZE_HEADER = '__ORIGINAL_MESSAGE_SIZE_HEADER__'

    def __init__(self,
                 message_store: MessageStoreBase,
                 size_threshold: int = -1,
                 always_store_threshold: int = -1
                 ):
        super().__init__(message_store=message_store)
        self._size_threshold = size_threshold
        self._always_store_threshold = always_store_threshold

    def transform_outgoing_message(self,
                                   output_device: TransformerOutputDevice,
                                   message_bundle: MessageBundle) -> MessageBundle:
        stream_size = message_bundle.message.stream.seek(0, 2)
        message_bundle.message.stream.seek(0)
        use_message_store = message_bundle.device_headers.get(self.USE_MESSAGE_STORE_HEADER_NAME, None)
        if 0 < self._always_store_threshold < stream_size:
            use_message_store = True

        if use_message_store is None:
            if stream_size > self._size_threshold:
                use_message_store = True
            else:
                use_message_store = False

        if use_message_store:
            # send using massage store
            result_message = self._send_over_message_store(output_device, message_bundle.message)
            result_message.headers[self.ORIGINAL_MESSAGE_SIZE_HEADER] = stream_size
            return MessageBundle(result_message, message_bundle.device_headers)
        else:
            # send without message store
            return message_bundle

    def _send_over_message_store(self,
                                 output_device: TransformerOutputDevice,
                                 message: Message) -> Message:
        try:
            # save to object storage and get the key
            key = self._message_store.put_message(output_device.name, message)
        except Exception as ex:
            raise OutputDeviceException('Error putting item into message store') from ex
        try:
            # send the key instead of the buffer over the inner device
            data = self.serialize_key(key)
            return Message(data, message.headers)
        except Exception:
            try:
                # delete from the object storage
                self._message_store.delete_message(key)
            except Exception as del_ex:
                raise OutputDeviceException(
                    'Error deleting item from message store') from del_ex  # ex will be in __context__
            raise


class MessageStoreOutputDeviceManagerWrapper(TransformerOutputDeviceManager):
    """
    This class is used to wrap IODeviceManager with message store functionality
    """

    def __init__(self,
                 inner_manager: OutputDeviceManager,
                 message_store: MessageStoreBase,
                 size_threshold: int = -1,
                 always_store_threshold: int = -1):
        """
        This class is used to wrap IODeviceManager with message store functionality

        :param inner_manager: the actual manager to wrap
        :param MessageStoreBase message_store: the message store to use to store files
        :param size_threshold: only use message store if the size of the product (in bytes) is larger than this.
                               set to -1 to always use message store (default: -1).
        :param always_store_threshold: above this size - the message will always be in the message store
                                       (even if USE_MESSAGE_STORE_HEADER_NAME is False)
                                       use -1 to always respect the USE_MESSAGE_STORE_HEADER_NAME header (default: -1)

        """
        transformer = MessageStoreOutputTransformer(message_store=message_store,
                                                    size_threshold=size_threshold,
                                                    always_store_threshold=always_store_threshold)

        super(MessageStoreOutputDeviceManagerWrapper, self).__init__(inner_manager, transformer)
        self.size_threshold = size_threshold
        self.always_store_threshold = always_store_threshold
