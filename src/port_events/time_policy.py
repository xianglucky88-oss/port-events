"""Time-zone interpretation contract for raw event timestamps.

A raw event time falls into one of two groups:

* it carries an explicit UTC offset (``+08:00``, ``Z``, ...): the value keeps
  its own meaning and no source declaration is consulted;
* it is offset-less local time: the zone must come from the source context.

For offset-less values the contract applies, in order of precedence:

1. a time zone declared on the raw record itself (``RawEvent.timezone``);
2. a time zone declared by the connector (``rule="source_declaration"``);
3. a per-source time zone mapping supplied by the caller
   (``rule="source_mapping"``);
4. the legacy global default passed to :func:`normalize_event` /
   ``PortEventsPipeline.run`` (``rule="global_default"``).

When two declaration levels name the same source with different zones, the
conflict is raised instead of silently preferring one.  A machine-local
default is never used.

Offset-less local times are also checked against daylight-saving transitions:
times that do not exist (spring-forward gap) are rejected, and times that
occur twice (fall-back) require explicit disambiguation (a ``fold`` of 0 or 1,
or an explicit offset in the value).  The chosen occurrence changes both the
UTC timestamp and the ``event_key`` derived from it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

#: Timestamp carried its own UTC offset (``Z`` or ``+HH:MM``).
RULE_EXPLICIT_OFFSET = "explicit_offset"
#: Time zone came from the raw record.
RULE_RECORD_DECLARATION = "record_declaration"
#: Time zone was declared for the source by the connector.
RULE_SOURCE_DECLARATION = "source_declaration"
#: Time zone came from a caller-supplied per-source mapping.
RULE_SOURCE_MAPPING = "source_mapping"
#: Time zone came from the legacy global default.
RULE_GLOBAL_DEFAULT = "global_default"

_DISAMBIGUATION_EXPLICIT = "explicit_offset"
_DISAMBIGUATION_UNAMBIGUOUS = "unambiguous"
_DISAMBIGUATION_FOLD_0 = "fold=0"
_DISAMBIGUATION_FOLD_1 = "fold=1"


class TimeContractError(ValueError):
    """Base class for time interpretation errors.

    Subclasses ``ValueError`` so existing callers that catch ``ValueError``
    keep working.
    """


class InvalidTimeZoneError(TimeContractError):
    """A declared or default time zone is not a valid IANA zone."""


class TimeZoneDeclarationConflict(TimeContractError):
    """Two sources of authority declared different zones for the same value."""


class InvalidFoldError(TimeContractError):
    """The record supplied a fold other than 0 or 1."""


class NonexistentLocalTimeError(TimeContractError):
    """The local time falls in a daylight-saving gap and has no UTC instant."""


class AmbiguousLocalTimeError(TimeContractError):
    """The local time occurs twice and needs explicit disambiguation."""


def _validate_zone_name(name: str, *, context: str) -> str:
    if not isinstance(name, str):
        raise InvalidTimeZoneError(
            f"invalid time zone {name!r} for {context}: expected an IANA zone name"
        )
    candidate = name.strip()
    if not candidate:
        raise InvalidTimeZoneError(
            f"invalid time zone {name!r} for {context}: expected an IANA zone name"
        )
    try:
        ZoneInfo(candidate)
    except (ZoneInfoNotFoundError, ValueError) as error:
        raise InvalidTimeZoneError(
            f"unknown IANA time zone {name!r} for {context}; "
            "use an IANA zone name such as 'America/New_York', or put an "
            "explicit UTC offset on the timestamp itself"
        ) from error
    return candidate


def _format_offset(offset: timedelta) -> str:
    total = int(offset.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    hours, remainder = divmod(total, 3600)
    minutes, seconds = divmod(remainder, 60)
    if seconds:
        return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{sign}{hours:02d}:{minutes:02d}"


@dataclass(frozen=True)
class TimeZoneDeclaration:
    """A declared IANA time zone and where it came from."""

    timezone: str
    rule: str
    origin: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "timezone",
            _validate_zone_name(self.timezone, context=self.origin),
        )


@dataclass(frozen=True)
class TimeZonePolicy:
    """Per-source time zone declarations for offset-less timestamps.

    Connectors build one with ``rule=RULE_SOURCE_DECLARATION``; callers may
    build one with :meth:`from_mapping` (``rule=RULE_SOURCE_MAPPING``).
    """

    declarations: Mapping[str, TimeZoneDeclaration] = field(default_factory=dict)
    origin: str = "caller"

    @classmethod
    def from_mapping(
        cls,
        mapping: Mapping[str, str | TimeZoneDeclaration] | None,
        *,
        origin: str,
        rule: str = RULE_SOURCE_MAPPING,
    ) -> "TimeZonePolicy":
        if not mapping:
            return cls({}, origin=origin)
        declarations: dict[str, TimeZoneDeclaration] = {}
        for source, value in mapping.items():
            if isinstance(value, TimeZoneDeclaration):
                declaration = value
            else:
                declaration = TimeZoneDeclaration(value, rule, f"{origin}:source={source}")
            declarations[source] = declaration
        return cls(declarations, origin=origin)

    def for_source(self, source: str) -> TimeZoneDeclaration | None:
        return self.declarations.get(source)


def combine_time_zone_policies(*policies: TimeZonePolicy | None) -> TimeZonePolicy:
    """Combine policy layers, raising when they disagree about a source.

    Identical declarations are allowed; the combined provenance records both
    origins.  Different declarations for the same source are never silently
    overridden.
    """

    combined: dict[str, TimeZoneDeclaration] = {}
    origins: list[str] = []
    for policy in policies:
        if policy is None:
            continue
        origins.append(policy.origin)
        for source, declaration in policy.declarations.items():
            previous = combined.get(source)
            if previous is None:
                combined[source] = declaration
                continue
            if previous.timezone != declaration.timezone:
                raise TimeZoneDeclarationConflict(
                    f"conflicting time zone declarations for source {source!r}: "
                    f"{previous.timezone!r} from {previous.origin} vs "
                    f"{declaration.timezone!r} from {declaration.origin}"
                )
            # Same zone from several layers: keep the more specific rule and
            # record that both layers agreed.
            rule = previous.rule
            if rule != RULE_SOURCE_DECLARATION and declaration.rule == RULE_SOURCE_DECLARATION:
                rule = RULE_SOURCE_DECLARATION
            combined[source] = TimeZoneDeclaration(
                previous.timezone,
                rule,
                "; ".join(
                    origin
                    for origin in (previous.origin, declaration.origin)
                    if origin
                ),
            )
    return TimeZonePolicy(combined, origin="; ".join(origins) or "caller")


@dataclass(frozen=True)
class TimeInterpretation:
    """How a raw timestamp was interpreted.

    ``applied_timezone`` is ``None`` when the value carried its own offset.
    ``rule`` and ``origin`` identify the authority that supplied the zone.
    ``disambiguation`` records the basis used for a daylight-saving overlap
    (``explicit_offset``, ``unambiguous``, ``fold=0`` or ``fold=1``).
    """

    applied_timezone: str | None
    rule: str
    origin: str
    disambiguation: str
    utc_offset: str
    fold: int | None = None


def _select_zone(
    *,
    source: str,
    record_timezone: str | None,
    policy: TimeZonePolicy | None,
    global_default: str | None,
) -> tuple[TimeZoneDeclaration, str]:
    """Pick the declaration governing an offset-less timestamp."""

    record_declaration: TimeZoneDeclaration | None = None
    if record_timezone is not None:
        record_declaration = TimeZoneDeclaration(
            record_timezone, RULE_RECORD_DECLARATION, "raw_event:timezone"
        )

    source_declaration = policy.for_source(source) if policy is not None else None

    if (
        record_declaration is not None
        and source_declaration is not None
        and record_declaration.timezone != source_declaration.timezone
    ):
        raise TimeZoneDeclarationConflict(
            f"conflicting time zone declarations for source {source!r}: "
            f"record declares {record_declaration.timezone!r} "
            f"({record_declaration.origin}) but "
            f"{source_declaration.timezone!r} was declared by "
            f"{source_declaration.origin}"
        )

    if record_declaration is not None:
        return record_declaration, record_declaration.rule
    if source_declaration is not None:
        return source_declaration, source_declaration.rule
    if global_default is None:
        raise InvalidTimeZoneError(
            f"no time zone declared for source {source!r} and no global "
            "default was supplied; offset-less timestamps need a source "
            "declaration, a source mapping or a global default"
        )
    return (
        TimeZoneDeclaration(
            global_default,
            RULE_GLOBAL_DEFAULT,
            "legacy:global_default",
        ),
        RULE_GLOBAL_DEFAULT,
    )


def _round_trip_candidates(
    naive: datetime, zone: ZoneInfo
) -> list[tuple[int, datetime, timedelta]]:
    """Return the distinct valid UTC instants for a naive local time.

    A candidate is valid when converting it to UTC and back reproduces the
    same wall-clock time.  Zero candidates means the literal is in a
    spring-forward gap; two distinct candidates means a fall-back overlap.
    """

    candidates: list[tuple[int, datetime, timedelta]] = []
    for fold in (0, 1):
        local = naive.replace(tzinfo=zone, fold=fold)
        utc = local.astimezone(timezone.utc)
        round_tripped = utc.astimezone(zone)
        if round_tripped.replace(tzinfo=None) != naive:
            continue
        if any(existing_utc == utc for _, existing_utc, _ in candidates):
            continue
        offset = local.utcoffset()
        assert offset is not None  # a valid candidate always has an offset
        candidates.append((fold, utc, offset))
    return candidates


def interpret_timestamp(
    raw_time: str,
    *,
    source: str,
    record_timezone: str | None = None,
    fold: int | None = None,
    policy: TimeZonePolicy | None = None,
    global_default: str | None = "Asia/Shanghai",
) -> tuple[datetime, TimeInterpretation]:
    """Interpret a raw timestamp and return its UTC instant plus provenance.

    Explicit offsets keep their own meaning.  Offset-less values go through
    :func:`_select_zone` and daylight-saving validation.
    """

    parsed = datetime.fromisoformat(raw_time.strip().replace("Z", "+00:00"))

    if fold is not None and fold not in (0, 1):
        raise InvalidFoldError(
            f"invalid fold {fold!r} for source {source!r}: expected 0 or 1"
        )

    if parsed.tzinfo is not None and parsed.utcoffset() is not None:
        # Explicit offset: no source declaration applies, and a fold is
        # irrelevant because the occurrence is already pinned down.
        utc = parsed.astimezone(timezone.utc)
        interpretation = TimeInterpretation(
            applied_timezone=None,
            rule=RULE_EXPLICIT_OFFSET,
            origin="raw_event:event_time",
            disambiguation=_DISAMBIGUATION_EXPLICIT,
            utc_offset=_format_offset(parsed.utcoffset()),
        )
        return utc, interpretation

    declaration, rule = _select_zone(
        source=source,
        record_timezone=record_timezone,
        policy=policy,
        global_default=global_default,
    )
    zone = ZoneInfo(declaration.timezone)
    naive = parsed.replace(tzinfo=None)

    candidates = _round_trip_candidates(naive, zone)
    literal = naive.isoformat()

    if not candidates:
        raise NonexistentLocalTimeError(
            f"local time {literal!r} for source {source!r} does not exist in "
            f"time zone {declaration.timezone!r} (daylight-saving spring-forward "
            "gap); no UTC instant or event_key can be derived from it. "
            "Correct the source timestamp or use an explicit UTC offset"
        )

    if len(candidates) > 1:
        utc_values = ", ".join(
            f"{utc.isoformat()} (offset {_format_offset(offset)})"
            for _, utc, offset in candidates
        )
        if fold is None:
            raise AmbiguousLocalTimeError(
                f"local time {literal!r} for source {source!r} occurs twice in "
                f"time zone {declaration.timezone!r} (daylight-saving fall-back): "
                f"{utc_values}. Disambiguate with fold=0 or fold=1 on the "
                "record, or put an explicit UTC offset on the timestamp. The "
                "chosen occurrence changes both the UTC time and the event_key"
            )
        chosen = next(
            (candidate for candidate in candidates if candidate[0] == fold), None
        )
        if chosen is None:  # defensive; both folds are normally present
            raise AmbiguousLocalTimeError(
                f"local time {literal!r} for source {source!r} cannot be "
                f"resolved with fold={fold}: {utc_values}"
            )
        chosen_fold, utc, offset = chosen
        disambiguation = (
            _DISAMBIGUATION_FOLD_0 if chosen_fold == 0 else _DISAMBIGUATION_FOLD_1
        )
        interpretation = TimeInterpretation(
            applied_timezone=declaration.timezone,
            rule=rule,
            origin=declaration.origin,
            disambiguation=disambiguation,
            utc_offset=_format_offset(offset),
            fold=chosen_fold,
        )
        return utc, interpretation

    chosen_fold, utc, offset = candidates[0]
    interpretation = TimeInterpretation(
        applied_timezone=declaration.timezone,
        rule=rule,
        origin=declaration.origin,
        disambiguation=_DISAMBIGUATION_UNAMBIGUOUS,
        utc_offset=_format_offset(offset),
    )
    return utc, interpretation
