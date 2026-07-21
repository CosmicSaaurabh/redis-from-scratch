# Redis From Scratch Documentation

This folder is the single source of truth for every product and architecture decision in the project.

## Structure

- `prd/`: product requirement documents describing what we build and why.
- `high-level-design/`: system-level designs (`HLD-<nnn>-<slug>.md`) with flow diagrams, trade-offs, and rejected alternatives.
- `low-level-design/`: detailed designs (`LLD-<nnn>-<slug>.md`) with package/crate layout, on-disk formats, and concurrency strategy.
- `adr/`: short architecture decision records (`ADR-<nnn>-<slug>.md`) capturing one decision each, including options rejected.
- `tasks/`: the phased MVP task breakdown with definitions of done and edge cases.
- `learning-log.md`: running record of new concepts learned, kept for revision.

## Process

1. A feature starts with a GitHub issue labelled `high-level-design`.
2. The design is discussed interview-style, then documented here before any code is written.
3. A `low-level-design` issue follows, then a `feature` issue.
4. Issues close strictly in that order.

## Writing Conventions

- One sentence per line.
- Plain dash only, never em dash.
- Every design doc has a "Rejected Alternatives" section.
- Every design doc ends with a "Failure Modes" section listing at least two ways the design can break.
