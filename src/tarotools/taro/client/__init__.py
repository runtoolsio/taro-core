"""
This module provides classes and functions for communicating with active job instances.
"""

import json
import logging
from dataclasses import dataclass
from enum import Enum, auto
from json import JSONDecodeError
from typing import List, Any, Dict, NamedTuple, Optional, TypeVar, Generic, Callable

from tarotools.taro.jobs.api import API_FILE_EXTENSION
from tarotools.taro.jobs.inst import JobInst, JobInstanceMetadata
from tarotools.taro.socket import SocketClient, ServerResponse, Error

log = logging.getLogger(__name__)


class InstanceResponse(NamedTuple):
    """
    Represents generic data for a single job instance,
    extracted from a successful de-serialized and pre-processed Instances API response.

    Note that the Instances API may manage several job instances and thus may
    return data for multiple instances, with each instance represented as a single instance response.

    Attributes:
        instance_meta: Metadata about the job instance.
        body: The JSON body of the response, as a dictionary.
    """
    instance_meta: JobInstanceMetadata
    body: Dict[str, Any]


class APIErrorType(Enum):
    """
    This enumeration defines the types of errors that can occur during communication with API.
    """

    SOCKET = auto()  # Errors related to the socket communication
    INVALID_RESPONSE = auto()  # Errors arising when the API's response cannot be processed correctly
    API_SERVER = auto()  # Errors signaled in the standard response indicating a problem on the server side
    API_CLIENT = auto()  # Errors resulting from client-side issues such as invalid request


class ErrorCode(Enum):
    INVALID_REQUEST = 400
    NOT_FOUND = 404
    INVALID_ENTITY = 422
    UNKNOWN = 1


@dataclass
class ResponseError:
    """
    Represents an error returned in the response from an API.

    This class encapsulates details about an error that the API itself has generated,
    either due to server-side issues or client-request related problems.

    Attributes:
        code: An enumeration member representing the type of error, as defined by the ErrorCode enumeration.
        reason: A human-readable string providing more details about the cause of the error.
    """

    code: ErrorCode
    reason: str


@dataclass
class APIError:
    """
    Represents an error that occurred during communication with an API.

    Attributes:
        api_id: Identifier of the API which generated the error.
        error_type: The type of error, as defined by the APIErrorType enumeration.
        socket_error: An optional object, only present in case of `APIErrorType.SOCKET` error.
        response_error: Details of the error returned by the API, only present in case of `APIErrorType.API_*` errors.
    """

    api_id: str
    error_type: APIErrorType
    socket_error: Optional[Error]
    response_error: Optional[ResponseError]


T = TypeVar('T')


@dataclass
class AggregatedResponse(Generic[T]):
    """
    Represents a collection of responses and errors from multiple APIs.

    This class is useful for handling API calls to several endpoints in a unified way, aggregating the responses
    and any potential errors into two distinct lists.

    Attributes:
        responses: A list of successful responses of type T.
        errors: A list of APIError instances representing errors that occurred during the API calls.

    Note: In the case of the Instances API, each API response may contain multiple instance responses.
          This means that the number of responses can exceed the number of contacted APIs.
    """

    responses: List[T]
    errors: List[APIError]

    def __iter__(self):
        return iter((self.responses, self.errors))


@dataclass
class JobInstanceResponse:
    instance_metadata: JobInstanceMetadata


class ReleaseResult(Enum):
    RELEASED = auto()
    NOT_APPLICABLE = auto()
    UNKNOWN = auto()


@dataclass
class ReleaseResponse(JobInstanceResponse):
    release_result: ReleaseResult


class StopResult(Enum):
    STOP_PERFORMED = auto()
    NOT_APPLICABLE = auto()
    UNKNOWN = auto()


@dataclass
class StopResponse(JobInstanceResponse):
    stop_result: StopResult


@dataclass
class TailResponse(JobInstanceResponse):
    tail: List[str]


@dataclass
class SignalProceedResponse(JobInstanceResponse):
    waiter_found: bool
    executed: bool


def read_instances(instance_match=None) -> AggregatedResponse[JobInst]:
    """
    Retrieves instance information for all active job instances for the current user.

    Args:
        instance_match (InstanceMatchCriteria, optional):
            A filter for instance matching. If provided, only instances that match will be included.

    Returns:
        A container holding the :class:`JobInst` objects that represent job instances.
        It also includes any errors that may have happened, each one related to a specific server API.

    Raises:
        PayloadTooLarge: If the payload size exceeds the maximum limit.
    """

    with APIClient() as client:
        return client.read_instances(instance_match)


def release_waiting_instances(waiting_state, instance_match) -> AggregatedResponse[ReleaseResponse]:
    """
    This function releases job instances that are waiting in the specified state and match the provided criteria.

    Args:
        waiting_state (ExecutionState, mandatory):
            Only instances that are waiting for this state will be released.
        instance_match (InstanceMatchCriteria, mandatory):
            The operation will affect only instances matching these criteria.

    Returns:
        A container holding :class:`ReleaseResponse` objects, each representing the result of the release operation
        for a respective job instance.
        It also includes any errors that may have happened, each one related to a specific server API.
    """

    with APIClient() as client:
        return client.release_waiting_instances(waiting_state, instance_match)


def release_pending_instances(pending_group, instance_match=None) -> AggregatedResponse[ReleaseResponse]:
    """
    This function releases job instances that are pending in the provided group
    and optionally match the provided criteria.

    Args:
        pending_group (str, mandatory):
            Only instances that are waiting in this group will be released.
        instance_match (InstanceMatchCriteria, optional):
            The operation will affect only instances matching these criteria or all instances if not provided.

    Returns:
        A container holding :class:`ReleaseResponse` objects, each representing the result of the release operation
        for a respective job instance.
        It also includes any errors that may have happened, each one related to a specific server API.
    """

    with APIClient() as client:
        return client.release_pending_instances(pending_group, instance_match)


def stop_instances(instance_match) -> AggregatedResponse[StopResponse]:
    """
    This function stops job instances that match the provided criteria.

    Args:
        instance_match (InstanceMatchCriteria, mandatory):
            The operation will affect only instances matching these criteria.

    Returns:
        A container holding :class:`StopResponse` objects, each representing the result of the stop operation
        for a respective job instance.
        It also includes any errors that may have happened, each one related to a specific server API.

    Note:
        The stop operation might not succeed if the instance doesn't correctly handle stop/terminate signals.
    """

    with APIClient() as client:
        return client.stop_instances(instance_match)


def read_tail(instance_match=None) -> AggregatedResponse[TailResponse]:
    """
    This function requests the last lines of the output from job instances that optionally match the provided criteria.

    Args:
        instance_match (InstanceMatchCriteria, optional):
            The operation will affect only instances matching these criteria.
            If not provided, the tail of all instances is read.

    Returns:
        A container holding :class:`TailResponse` objects, each containing last lines for a respective job instance.
        It also includes any errors that may have happened, each one related to a specific server API.
    """

    with APIClient() as client:
        return client.read_tail(instance_match)


def signal_proceed(instance_match) -> AggregatedResponse[SignalProceedResponse]:
    with APIClient() as client:
        return client.signal_ready(instance_match)


def _no_resp_mapper(api_instance_response: InstanceResponse) -> InstanceResponse:
    return api_instance_response


def _release_resp_mapper(inst_resp: InstanceResponse) -> ReleaseResponse:
    try:
        release_res = ReleaseResult[inst_resp.body["release_result"].upper()]
    except KeyError:
        release_res = ReleaseResult.UNKNOWN
    return ReleaseResponse(inst_resp.instance_meta, release_res)


class APIClient(SocketClient):

    def __init__(self):
        super().__init__(API_FILE_EXTENSION, bidirectional=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def send_request(self, api: str, instance_match=None, req_body=None,
                     resp_mapper: Callable[[InstanceResponse], T] = _no_resp_mapper) -> AggregatedResponse[T]:
        if not req_body:
            req_body = {}
        req_body["request_metadata"] = {"api": api}
        if instance_match and instance_match.id_criteria:
            req_body["request_metadata"]["instance_match"] = instance_match.to_dict()

        server_responses: List[ServerResponse] = self.communicate(json.dumps(req_body))
        return _process_responses(server_responses, resp_mapper)

    def read_instances(self, instance_match=None) -> AggregatedResponse[JobInst]:
        """
        Retrieves instance information for all active job instances for the current user.

        Args:
            instance_match (InstanceMatchCriteria, optional):
                A filter for instance matching. If provided, only instances that match will be included.

        Returns:
            A container holding the :class:`JobInst` objects that represent job instances.
            It also includes any errors that may have happened, each one related to a specific server API.

        Raises:
            PayloadTooLarge: If the payload size exceeds the maximum limit.
        """

        def resp_mapper(inst_resp: InstanceResponse) -> JobInst:
            return JobInst.from_dict(inst_resp.body["job_instance"])

        return self.send_request('/instances', instance_match, resp_mapper=resp_mapper)

    def release_waiting_instances(self, waiting_state, instance_match) -> AggregatedResponse[ReleaseResponse]:
        """
        This function releases job instances that are waiting in the specified state and match the provided criteria.

        Args:
            waiting_state (ExecutionState, mandatory):
                Only instances that are waiting for this state will be released.
            instance_match (InstanceMatchCriteria, mandatory):
                The operation will affect only instances matching these criteria.

        Returns:
            A container holding :class:ReleaseResponse objects, each representing the result of the release operation
            for a respective job instance.
            It also includes any errors that may have happened, each one related to a specific server API.
        """

        if not instance_match or not waiting_state:
            raise ValueError("Arguments cannot be empty")

        req_body = {"waiting_state": waiting_state.name}
        return self.send_request('/instances/release/waiting', instance_match, req_body, _release_resp_mapper)

    def release_pending_instances(self, pending_group, instance_match=None) -> AggregatedResponse[ReleaseResponse]:
        """
        This function releases job instances that are pending in the provided group
        and optionally match the provided criteria.

        Args:
            pending_group (str, mandatory):
                Only instances that are waiting in this group will be released.
            instance_match (InstanceMatchCriteria, optional):
                The operation will affect only instances matching these criteria or all instances if not provided.

        Returns:
            A container holding :class:`ReleaseResponse` objects, each representing the result of the release operation
            for a respective job instance.
            It also includes any errors that may have happened, each one related to a specific server API.
        """

        if not pending_group:
            raise ValueError("Missing pending group")

        req_body = {"pending_group": pending_group}
        return self.send_request('/instances/release/pending', instance_match, req_body, _release_resp_mapper)

    def stop_instances(self, instance_match) -> AggregatedResponse[StopResponse]:
        """
        This function stops job instances that match the provided criteria.

        Args:
            instance_match (InstanceMatchCriteria, mandatory):
                The operation will affect only instances matching these criteria.

        Returns:
            A container holding :class:`StopResponse` objects, each representing the result of the stop operation
            for a respective job instance.
            It also includes any errors that may have happened, each one related to a specific server API.

        Note:
            The stop operation might not succeed if the instance doesn't correctly handle stop/terminate signals.
        """

        if not instance_match:
            raise ValueError('Id matching criteria is mandatory for the stop operation')

        def resp_mapper(inst_resp: InstanceResponse) -> StopResponse:
            return StopResponse(inst_resp.instance_meta, StopResult[inst_resp.body["stop_result"].upper()])

        return self.send_request('/instances/stop', instance_match, resp_mapper=resp_mapper)

    def read_tail(self, instance_match=None) -> AggregatedResponse[TailResponse]:
        """
        This function requests the last lines of the output from job instances
        that optionally match the provided criteria.

        Args:
            instance_match (InstanceMatchCriteria, optional):
                The operation will affect only instances matching these criteria.
                If not provided, the tail of all instances is read.

        Returns:
            A container holding :class:`TailResponse` objects, each containing last lines for a respective job instance.
            It also includes any errors that may have happened, each one related to a specific server API.
        """

        def resp_mapper(inst_resp: InstanceResponse) -> TailResponse:
            return TailResponse(inst_resp.instance_meta, inst_resp.body["tail"])

        return self.send_request('/instances/tail', instance_match, resp_mapper=resp_mapper)

    def signal_ready(self, instance_match) -> AggregatedResponse[SignalProceedResponse]:
        def resp_mapper(inst_resp: InstanceResponse) -> SignalProceedResponse:
            return SignalProceedResponse(
                inst_resp.instance_meta, inst_resp.body["waiter_found"], inst_resp.body["executed"])
        
        return self.send_request('/instances/_signal/dispatch', instance_match, resp_mapper=resp_mapper)


def _process_responses(server_responses: List[ServerResponse], resp_mapper: Callable[[InstanceResponse], T]) \
        -> AggregatedResponse[T]:
    responses: List[T] = []
    errors: List[APIError] = []

    for server_id, resp, error in server_responses:
        if error:
            log.error("event=[api_error] type=[socket] error=[%s]", error)
            errors.append(APIError(server_id, APIErrorType.SOCKET, error, None))
            continue

        try:
            resp_body = json.loads(resp)
        except JSONDecodeError:
            # TODO Mostly when the resp is too long (i.e. server with many instances)
            raise
        resp_metadata = resp_body.get("response_metadata")
        if not resp_metadata:
            log.error("event=[api_error] type=[invalid_response] error=[missing_response_metadata]")
            errors.append(APIError(server_id, APIErrorType.INVALID_RESPONSE, None, None))
            continue
        if "error" in resp_metadata:
            code = resp_metadata.get('code')
            reason = resp_metadata['error'].get('reason')
            if not code or code < 400 or code >= 600:
                log.error("event=[api_error] type=[invalid_response] error=[invalid_response_code] code=[%s]", code)
                errors.append(APIError(server_id, APIErrorType.INVALID_RESPONSE, None, None))
                continue
            if not reason:
                log.error("event=[api_error] type=[invalid_response] error=[missing_error_reason] code=[%s]", code)
                errors.append(APIError(server_id, APIErrorType.INVALID_RESPONSE, None, None))
                continue

            error_type = APIErrorType.API_CLIENT if code < 500 else APIErrorType.API_SERVER
            try:
                err_code = ErrorCode(code)
            except ValueError:
                log.warning("event=[unknown_error_code] code=[%s]", code)
                err_code = ErrorCode.UNKNOWN
            log.error("event=[api_error] type=[%s] code=[%s] reason=[%s]", error_type, err_code, reason)
            errors.append(APIError(server_id, error_type, None, ResponseError(err_code, reason)))
            continue

        for instance_resp in resp_body['instance_responses']:
            instance_metadata = JobInstanceMetadata.from_dict(instance_resp['instance_metadata'])
            api_instance_response = InstanceResponse(instance_metadata, instance_resp)
            try:
                resp = resp_mapper(api_instance_response)
            except (KeyError, ValueError) as e:
                log.error("event=[api_error] type=[%s] reason=[%s]", APIErrorType.INVALID_RESPONSE, e)
                errors.append(APIError(server_id, APIErrorType.INVALID_RESPONSE, None, None))
                break
            responses.append(resp)

    return AggregatedResponse(responses, errors)
