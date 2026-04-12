"""BLS programs registry — series ID field definitions for each survey."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FieldSpec:
    name: str
    length: int


@dataclass(frozen=True)
class BLSProgram:
    prefix: str
    name: str
    fields: list[FieldSpec] = field(default_factory=list)

    @property
    def series_id_length(self) -> int:
        return sum(f.length for f in self.fields)

    def field_slices(self) -> list[tuple[str, int, int]]:
        """Return ``(name, offset, length)`` for each field in the series ID.

        Useful for positional substring extraction in Polars::

            for name, offset, length in program.field_slices():
                df = df.with_columns(
                    pl.col("series_id").str.slice(offset, length)
                      .str.strip_chars().alias(name)
                )
        """
        result: list[tuple[str, int, int]] = []
        pos = 0
        for f in self.fields:
            result.append((f.name, pos, f.length))
            pos += f.length
        return result


PROGRAMS: dict[str, BLSProgram] = {}


def _register(program: BLSProgram) -> BLSProgram:
    PROGRAMS[program.prefix] = program
    return program


# -- CES (Current Employment Statistics) --
EN = _register(
    BLSProgram(
        prefix="CE",
        name="Current Employment Statistics",
        fields=[
            FieldSpec("prefix", 2),
            FieldSpec("seasonal", 1),
            FieldSpec("supersector", 2),
            FieldSpec("industry", 6),
            FieldSpec("data_type", 2),
        ],
    )
)

# -- SAE (State and Area Employment) --
SM = _register(
    BLSProgram(
        prefix="SM",
        name="State and Area Employment",
        fields=[
            FieldSpec("prefix", 2),
            FieldSpec("seasonal", 1),
            FieldSpec("state", 2),
            FieldSpec("area", 5),
            FieldSpec("supersector", 2),
            FieldSpec("industry", 6),
            FieldSpec("data_type", 2),
        ],
    )
)

# -- BD (Business Employment Dynamics) — 28 chars --
BD = _register(
    BLSProgram(
        prefix="BD",
        name="Business Employment Dynamics",
        fields=[
            FieldSpec("prefix", 2),
            FieldSpec("seasonal", 1),
            FieldSpec("area_code", 10),
            FieldSpec("industry_code", 6),
            FieldSpec("unit_analysis", 1),
            FieldSpec("data_element", 1),
            FieldSpec("size_class", 2),
            FieldSpec("data_class", 2),
            FieldSpec("rate_level", 1),
            FieldSpec("record_type", 1),
            FieldSpec("ownership", 1),
        ],
    )
)

# -- JT (Job Openings and Labor Turnover Survey) — 21 chars --
JT = _register(
    BLSProgram(
        prefix="JT",
        name="Job Openings and Labor Turnover Survey",
        fields=[
            FieldSpec("prefix", 2),
            FieldSpec("seasonal", 1),
            FieldSpec("industry_code", 6),
            FieldSpec("state_code", 2),
            FieldSpec("area_code", 5),
            FieldSpec("size_class", 2),
            FieldSpec("data_element", 2),
            FieldSpec("rate_level", 1),
        ],
    )
)
