# Display Title Rules: GX/GX-R/KTM Impurity-Turbulence Campaign

These rules are a reusable template for Datamak Lite campaigns studying
impurity dynamics in turbulent plasma fields.  They are not tied to a specific
shot, tokamak, or workspace.

## General Rule

Use semantic titles in campaign overviews:

```text
[Physics role]: [species/model] in [parent turbulence] -- [main distinguishing parameter]
```

Keep raw folder names, input files, and remote paths as details, not titles.

If the metadata are insufficient:

```text
Uncurated [simulation|pool|analysis]: needs semantic title metadata
```

and add a todo note listing the missing metadata.

## Main Plasma GX

Use this for simulations that generate the main turbulence.

Template:

```text
Main Plasma GX: e-/D+ turbulence [ES|EM|full-EM] [kinetic e-|adiabatic e-]
```

If impurities are included:

```text
Main Plasma GX with [species] Z=[Z list] impurities: e-/D+ turbulence
```

Useful suffixes:

```text
-- high a/LTi
-- a/LTi=a/LTe
-- Ti=Te
-- trace impurities
-- coupled impurities
```

Show as details:

- main-plasma gradients and temperatures that differ;
- species included in the turbulence simulation;
- whether impurities are trace or coupled;
- whether turbulent fields were saved;
- saved fields, time window, stride, and time-step mode.

## Saved Turbulent Field History

Use this for reusable field histories or file patterns.

Template:

```text
Saved turbulent fields: [kinetic e-|adiabatic e-] [ES|EM|full-EM], t=[start-end], stride=[N]
```

If saved fields are known:

```text
Saved turbulent fields: kinetic e- full-EM, phi/Apar/Bpar, t=600-700, stride=1
```

This is often the most useful grouping object.  Replay, tracer, and KTM studies
are directly comparable when they reuse the same saved turbulent field.

Show as details:

- parent simulation;
- fields saved;
- time window;
- save stride;
- effective replay stride if different;
- local/remote/pattern availability.

## GX-R Replay

Use this for GX-R replay pools or replay simulations.

Template:

```text
Replay of [species or species set] Z=[Z or Z range] in [parent turbulence]
```

Examples:

```text
Replay of W Z=74 in kinetic-e turbulence
Replay of fully stripped impurities in kinetic-e turbulence
Replay of B/Ne/Ar/Kr/Xe/W at Zmax in equal-gradient turbulence
```

Model suffixes:

```text
-- full model
-- w/o turbulent vE.grad g
-- no v_parallel
-- no magnetic drift
-- ES stride scan
-- EM stride scan
-- collision scan
```

If the pool contains reference, flat-density-gradient, and
flat-temperature-gradient variants, do not repeat all variants in the title.
Use details:

```text
gradient variants: reference, flat a/Ln, flat a/LT
```

Show as details:

- parent saved turbulent history;
- species, mass number, charge state;
- impurity gradients;
- model terms included or removed;
- fields used;
- save stride, replay stride, and effective stride;
- pool status and available variants.

## Tracer Diagnostics

Use this for tracer-particle, phase-factor, or cumulant diagnostics.

Template:

```text
Tracer diagnostic: [phase factor|orbit cumulants|NL/QL kernels] in [parent turbulence]
```

Examples:

```text
Tracer diagnostic: phase-factor NL/QL in kinetic-e turbulence
Tracer diagnostic: orbit cumulants in kinetic-e turbulence, skip=2
```

Show as details:

- parent saved turbulent history;
- particles per theta plane;
- theta/parallel treatment;
- tau window;
- stride/skip;
- kx/ky filtering;
- whether diffusion spectra use `2 Re[D(ky)]`;
- comparable GX-R replay studies using the same parent turbulence.

## KTM / Field-Only Model

Use this for KTM or related field-only impurity-response scans.

Template:

```text
Kinetic Trace Model: [field|tracer] response of [species set] in [parent turbulence]
```

Examples:

```text
Kinetic Trace Model: fully stripped impurity response in kinetic-e turbulence
Kinetic Trace Model: W/Ar/Ne response in adiabatic-e turbulence
```

Useful suffixes:

```text
-- with turbulent vE
-- w/o turbulent vE
-- doubled impurity temperature gradient
-- equal-gradient turbulence
```

Show as details:

- parent turbulent field;
- species set, mass number, charge state;
- field-only or tracer-based approximation;
- model terms included or removed;
- comparable GX-R replay studies using the same parent turbulence.

## Comparison Groups

Create comparison titles when several downstream studies share the same parent
history.

Template:

```text
Comparison on [saved history title]: [models compared]
```

Examples:

```text
Comparison on kinetic-e t=600-700 fields: GX-R full vs no-vE vs tracer
Comparison on equal-gradient ITG fields: KTM vs GX-R replay
```

Useful comparison axes:

- nonlinear vs quasi-linear closure;
- with vs without turbulent `vE.grad g`;
- full vs reduced dynamics;
- parallel streaming on/off;
- magnetic drift on/off;
- tracer vs GX-R replay;
- KTM vs GX-R replay;
- same physics but different numerical stride or resolution.
