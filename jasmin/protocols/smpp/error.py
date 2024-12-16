from smpp.pdu import constants
from smpp.pdu import pdu_types
from smpp.pdu.error import SMPPProtocolError, SMPPError


class LongSubmitSmTransactionError(SMPPError):
    """
    Raised during a long message transaction if an error occurs
    (e.g., concatenated message submission issues).
    """

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class SubmitSmEventHandlerErrorNoShutdown(SMPPProtocolError):
    """
    Base exception for errors raised in SMPPServerFactory.submit_sm_event()
    that do not cause the connection to shut down.

    Subclasses must define self.status with an appropriate CommandStatus
    before calling this initializer.
    """

    def __init__(self, message=None):
        if message is None:
            # If no message is provided, build a default one based on status.
            super().__init__(f"{self.getStatusDescription()}", self.status)
        else:
            super().__init__(message, self.status)


class SubmitSmEventHandlerErrorShutdown(SMPPError):
    """
    Base exception for errors raised in SMPPServerFactory.submit_sm_event()
    that cause the connection to shut down.
    """


class SubmitSmInvalidArgsError(SubmitSmEventHandlerErrorNoShutdown):
    """
    Raised when arguments for submit_sm are invalid.
    E.g., missing required parameters or invalid parameter types.
    """

    def __init__(self):
        self.status = pdu_types.CommandStatus.ESME_RSYSERR
        super().__init__()


class SubmitSmWithoutDestinationAddrError(SubmitSmEventHandlerErrorNoShutdown):
    """
    Raised when submit_sm is missing a destination address.
    """

    def __init__(self):
        self.status = pdu_types.CommandStatus.ESME_RINVDSTADR
        super().__init__()


class SubmitSmRouteNotFoundError(SubmitSmEventHandlerErrorNoShutdown):
    """
    Raised when no routing is found for the given submit_sm (e.g., no available connector).
    """

    def __init__(self):
        self.status = pdu_types.CommandStatus.ESME_RINVDSTADR
        super().__init__()


class SubmitSmRoutingError(SubmitSmEventHandlerErrorNoShutdown):
    """
    Raised when failing to send SubmitSm to the routedConnector.
    """

    def __init__(self):
        self.status = pdu_types.CommandStatus.ESME_RSUBMITFAIL
        super().__init__()


class SubmitSmChargingError(SubmitSmEventHandlerErrorNoShutdown):
    """
    Raised when charging the user for sending submit_sm fails
    (e.g., insufficient balance).
    """

    def __init__(self):
        self.status = pdu_types.CommandStatus.ESME_RSYSERR
        super().__init__()


class SubmitSmThroughputExceededError(SubmitSmEventHandlerErrorNoShutdown):
    """
    Raised when throughput limits are exceeded.
    This can occur if the user sends messages too quickly.
    """

    def __init__(self):
        self.status = pdu_types.CommandStatus.ESME_RTHROTTLED
        super().__init__()


class CredentialValidationError(SubmitSmEventHandlerErrorShutdown):
    """
    Raised when user credential validation fails.
    This error causes the connection to shut down.
    """


class AuthorizationError(SubmitSmEventHandlerErrorNoShutdown):
    """
    Raised when user credentials are invalid or authorization fails
    (e.g., user not allowed to send messages).
    """

    def __init__(self, message):
        self.status = pdu_types.CommandStatus.ESME_RINVSYSID
        super().__init__(message)


class FilterError(SubmitSmEventHandlerErrorNoShutdown):
    """
    Raised when a filter check fails in user credential validation.
    Filter checks may include destination_address, source_address, or priority.
    """

    def __init__(self, message, filter_key=None):
        if filter_key == 'destination_address':
            self.status = pdu_types.CommandStatus.ESME_RINVDSTADR
        elif filter_key == 'source_address':
            self.status = pdu_types.CommandStatus.ESME_RINVSRCADR
        elif filter_key == 'priority':
            self.status = pdu_types.CommandStatus.ESME_RINVPRTFLG
        else:
            self.status = pdu_types.CommandStatus.ESME_RSYSERR

        super().__init__(message)


class InterceptorError(SMPPProtocolError):
    """
    Errors raised during interception of submit_sm or deliver_sm PDUs.
    These errors do not cause a connection shutdown.

    A numeric code corresponding to a known command status must be provided.
    If the code is invalid, ESME_RSYSERR is used as a fallback.
    """

    def __init__(self, code, message=None):
        if isinstance(code, int) and code > 0 and code in constants.command_status_value_map:
            self.status = getattr(pdu_types.CommandStatus, constants.command_status_value_map[code]['name'])
        else:
            # Fallback if code is not valid
            self.status = pdu_types.CommandStatus.ESME_RSYSERR

        if message is None:
            super().__init__(f"{self.getStatusDescription()}", self.status)
        else:
            super().__init__(message, self.status)


class DeliverSmInterceptionError(InterceptorError):
    """
    Raised during deliver_sm interception if an error occurs.
    """


class SubmitSmInterceptionError(InterceptorError):
    """
    Raised during submit_sm interception if an error occurs.
    """


class SubmitSmInterceptionSuccess(InterceptorError):
    """
    Indicates a successful interception with an ESME_ROK status.
    This may be used to return a success status after performing an interception.
    """

    def __init__(self, message=None):
        self.status = pdu_types.CommandStatus.ESME_ROK
        super().__init__(0, message)  # Using '0' here is a placeholder since status is already set.


class InterceptorNotSetError(InterceptorError):
    """
    Raised when the interceptor is not set.
    """

    def __init__(self, message=None):
        # Using code=8 as a fallback for ESME_RSYSERR
        super().__init__(code=8, message=message)


class InterceptorNotConnectedError(InterceptorError):
    """
    Raised when the interceptor is configured but not connected.
    """

    def __init__(self, message=None):
        # Using code=8 as a fallback for ESME_RSYSERR
        super().__init__(code=8, message=message)


class InterceptorRunError(InterceptorError):
    """
    Raised when an error occurs while running the interceptor script.
    """

    def __init__(self, message=None):
        # Using code=8 as a fallback for ESME_RSYSERR
        super().__init__(code=8, message=message)
