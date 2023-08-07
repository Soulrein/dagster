from typing import Mapping, NamedTuple, Optional, Union

import dagster._check as check
from dagster._annotations import PublicAttr, public
from dagster._core.definitions.events import AssetMaterialization, AssetObservation
from dagster._core.events import DagsterEvent, DagsterEventType
from dagster._core.utils import coerce_valid_log_level
from dagster._serdes.serdes import (
    deserialize_value,
    serialize_value,
    whitelist_for_serdes,
)
from dagster._utils.error import SerializableErrorInfo
from dagster._utils.log import (
    JsonEventLoggerHandler,
    StructuredLoggerHandler,
    StructuredLoggerMessage,
    construct_single_handler_logger,
)


@whitelist_for_serdes(
    # These were originally distinguished from each other but ended up being empty subclasses
    # of EventLogEntry -- instead of using the subclasses we were relying on
    # EventLogEntry.is_dagster_event to distinguish events that originate in the logging
    # machinery from events that are yielded by user code
    old_storage_names={"DagsterEventRecord", "LogMessageRecord", "EventRecord"},
    old_fields={"message": ""},
    storage_field_names={"job_name": "pipeline_name"},
)
class EventLogEntry(
    NamedTuple(
        "_EventLogEntry",
        [
            ("error_info", PublicAttr[Optional[SerializableErrorInfo]]),
            ("level", PublicAttr[Union[str, int]]),
            ("user_message", PublicAttr[str]),
            ("run_id", PublicAttr[str]),
            ("timestamp", PublicAttr[float]),
            ("step_key", PublicAttr[Optional[str]]),
            ("job_name", PublicAttr[Optional[str]]),
            ("dagster_event", PublicAttr[Optional[DagsterEvent]]),
        ],
    )
):
    """Entries in the event log.

    Users should not instantiate this object directly. These entries may originate from the logging machinery (DagsterLogManager/context.log), from
    framework events (e.g. EngineEvent), or they may correspond to events yielded by user code
    (e.g. Output).

    Args:
        error_info (Optional[SerializableErrorInfo]): Error info for an associated exception, if
            any, as generated by serializable_error_info_from_exc_info and friends.
        level (Union[str, int]): The Python log level at which to log this event. Note that
            framework and user code events are also logged to Python logging. This value may be an
            integer or a (case-insensitive) string member of PYTHON_LOGGING_LEVELS_NAMES.
        user_message (str): For log messages, this is the user-generated message.
        run_id (str): The id of the run which generated this event.
        timestamp (float): The Unix timestamp of this event.
        step_key (Optional[str]): The step key for the step which generated this event. Some events
            are generated outside of a step context.
        job_name (Optional[str]): The job which generated this event. Some events are
            generated outside of a job context.
        dagster_event (Optional[DagsterEvent]): For framework and user events, the associated
            structured event.
    """

    def __new__(
        cls,
        error_info,
        level,
        user_message,
        run_id,
        timestamp,
        step_key=None,
        job_name=None,
        dagster_event=None,
    ):
        return super(EventLogEntry, cls).__new__(
            cls,
            check.opt_inst_param(error_info, "error_info", SerializableErrorInfo),
            coerce_valid_log_level(level),
            check.str_param(user_message, "user_message"),
            check.str_param(run_id, "run_id"),
            check.float_param(timestamp, "timestamp"),
            check.opt_str_param(step_key, "step_key"),
            check.opt_str_param(job_name, "job_name"),
            check.opt_inst_param(dagster_event, "dagster_event", DagsterEvent),
        )

    @public
    @property
    def is_dagster_event(self) -> bool:
        """bool: If this entry contains a DagsterEvent."""
        return bool(self.dagster_event)

    @public
    def get_dagster_event(self) -> DagsterEvent:
        """DagsterEvent: Returns the DagsterEvent contained within this entry. If this entry does not
        contain a DagsterEvent, an error will be raised.
        """
        if not isinstance(self.dagster_event, DagsterEvent):
            check.failed(
                "Not a dagster event, check is_dagster_event before calling get_dagster_event",
            )

        return self.dagster_event

    def to_json(self):
        return serialize_value(self)

    @staticmethod
    def from_json(json_str: str):
        return deserialize_value(json_str, EventLogEntry)

    @public
    @property
    def dagster_event_type(self) -> Optional[DagsterEventType]:
        """Optional[DagsterEventType]: The type of the DagsterEvent contained by this entry, if any."""
        return self.dagster_event.event_type if self.dagster_event else None

    @public
    @property
    def message(self) -> str:
        """Return the message from the structured DagsterEvent if present, fallback to user_message."""
        if self.is_dagster_event:
            msg = self.get_dagster_event().message
            if msg is not None:
                return msg

        return self.user_message

    @property
    def asset_materialization(self) -> Optional[AssetMaterialization]:
        if (
            self.dagster_event
            and self.dagster_event.event_type_value == DagsterEventType.ASSET_MATERIALIZATION
        ):
            materialization = self.dagster_event.step_materialization_data.materialization
            if isinstance(materialization, AssetMaterialization):
                return materialization

        return None

    @property
    def asset_observation(self) -> Optional[AssetObservation]:
        if (
            self.dagster_event
            and self.dagster_event.event_type_value == DagsterEventType.ASSET_OBSERVATION
        ):
            observation = self.dagster_event.asset_observation_data.asset_observation
            if isinstance(observation, AssetObservation):
                return observation

        return None

    @property
    def tags(self) -> Optional[Mapping[str, str]]:
        materialization = self.asset_materialization
        if materialization:
            return materialization.tags

        observation = self.asset_observation
        if observation:
            return observation.tags

        return None


def construct_event_record(logger_message: StructuredLoggerMessage) -> EventLogEntry:
    check.inst_param(logger_message, "logger_message", StructuredLoggerMessage)

    return EventLogEntry(
        level=logger_message.level,
        user_message=logger_message.meta["orig_message"],
        run_id=logger_message.meta["run_id"],
        timestamp=logger_message.record.created,
        step_key=logger_message.meta.get("step_key"),
        job_name=logger_message.meta.get("job_name"),
        dagster_event=logger_message.meta.get("dagster_event"),
        error_info=None,
    )


def construct_event_logger(event_record_callback):
    """Callback receives a stream of event_records. Piggybacks on the logging machinery."""
    check.callable_param(event_record_callback, "event_record_callback")

    return construct_single_handler_logger(
        "event-logger",
        "debug",
        StructuredLoggerHandler(
            lambda logger_message: event_record_callback(construct_event_record(logger_message))
        ),
    )


def construct_json_event_logger(json_path):
    """Record a stream of event records to json."""
    check.str_param(json_path, "json_path")
    return construct_single_handler_logger(
        "json-event-record-logger",
        "debug",
        JsonEventLoggerHandler(
            json_path,
            lambda record: construct_event_record(
                StructuredLoggerMessage(
                    name=record.name,
                    message=record.msg,
                    level=record.levelno,
                    meta=record.dagster_meta,
                    record=record,
                )
            ),
        ),
    )
