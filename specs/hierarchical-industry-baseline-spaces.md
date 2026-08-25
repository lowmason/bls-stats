# Fast Inference for a Hierarchical Dynamic-Factor Nowcasting Model on a Small Box — A Standalone Optimization Prompt

## What this document is, and what I am asking you to do

This is a **self-contained specification of a Bayesian state-space nowcasting model**,
written so that you can reason about it **without any access to the codebase, data, or
environment it comes from**. Everything you need — the model math, the data structure,
the inference procedure, the objective, and the compute budget — is defined below in
notation that stands on its own.

**The question I want you to answer:**

> How should this model, and especially its **inference procedure**, be modified so that
> a full fit runs **quickly on a small, CPU-only machine** (≈4 CPU cores, ~25 GB RAM of
> which 15–20 GB is effectively usable, no GPU), while preserving as much of its
> **probabilistic-forecast quality** — calibrated predictive densities — as possible?

I have a working implementation that uses **Rao–Blackwellized NUTS** (the No-U-Turn
Sampler operating on the static parameters, with the latent states analytically
marginalized by an exact Kalman filter). It is accurate but too slow to iterate on
comfortably at this hardware scale. I want a concrete, justified recommendation for a
faster inference scheme — including, if you judge it worthwhile, a different sampler
family or a re-parameterization — together with the trade-offs it implies for
forecast calibration.

I have **hypotheses** about the answer (switch to Gibbs sampling; make the priors
Gaussian or otherwise conjugate). I state them in §4 as candidate directions, **not as
constraints**, and I flag a genuine tension between them and my hard constraints that I
want you to resolve rather than paper over. Treat the hypotheses as leads to evaluate,
confirm, refine, or reject on the merits.

Please give me:

1. A recommended inference approach (sampler family, parameterization, and any model
   changes it requires), with the reasoning and the trade-offs you weighed.
2. An honest account of what it costs in **predictive-density quality** (calibration,
   interval coverage, tail behavior) relative to the current NUTS fit, and how to
   measure that cost.
3. A menu of orthogonal speed levers ranked by expected payoff on *this* hardware, so I
   can compose them.
4. Where relevant, pseudo-code or the key update equations, at a level of detail I could
   implement.

---

## 1. Compute budget and environment (the binding constraint)

- **CPU:** ~4 physical cores. No GPU, no TPU.
- **RAM:** ~25 GB nominal; assume **15–20 GB effectively usable** after overhead.
- **Software stack (hard constraint):** the implementation is in **JAX** with
  **NumPyro** on top. I want to keep this ecosystem — I am not looking to rewrite in a
  different probabilistic-programming framework. JAX's `jit`, `vmap`, `scan`,
  `associative_scan`, `pmap`, and NumPyro's inference machinery (NUTS/HMC, and the
  building blocks for custom kernels / Gibbs) are all fair game.
- **Numerics:** the current implementation runs in **float64** for Kalman-filter
  numerical stability. Whether that is necessary, or whether float32 (with a
  square-root / Joseph-stabilized filter) is safe here, is one of the questions on the
  table — see §7.
- **Parallelism reality check:** with only ~4 cores, "run 4 chains in parallel" already
  saturates the machine. Levers that assume many cores or a GPU (e.g. massively parallel
  associative-scan Kalman filtering, large-batch vectorization) may not help and could
  hurt via memory pressure. Please account for this explicitly.

The practical target is **fast enough to iterate**: minutes-to-low-tens-of-minutes per
fit rather than many hours, at a fit size defined in §6.

---

## 2. The forecasting problem

The model produces a **monthly nowcast of the change in total private-sector employment**
(a "jobs added this month" number, in thousands), built up from a disaggregation into
**14 industry segments** ("leaves"). It is a real-time nowcast: at each monthly origin,
only data available as of that date is used.

The forecast is **probabilistic** — the deliverable is a *predictive distribution* for
the target, not just a point estimate. This is the crux for inference choice: the model
is judged primarily on **density quality**, not point accuracy.

**Objective / evaluation metrics** (so you can judge what a faster sampler may sacrifice):

- **Primary:** CRPS (Continuous Ranked Probability Score) of the predictive distribution
  for the aggregate target's first-release value. Lower is better; it rewards sharp,
  well-calibrated densities and penalizes overconfidence.
- **Secondary:** RMSE / MAE (point accuracy); PIT (probability-integral-transform)
  uniformity; central-interval coverage at the 50/80/95% levels plus interval sharpness.
- **Bar to clear:** the model must beat a strong external point/consensus benchmark on
  density terms. A sampler that is fast but produces miscalibrated or overconfident
  posteriors defeats the purpose — so "just lower `target_accept` and pray" is not an
  acceptable answer without a calibration argument.

This is *why* the current implementation uses NUTS rather than a cheaper sampler: NUTS on
the marginalized static parameters gives well-calibrated posterior densities, which flow
directly into CRPS and coverage. Any replacement must be defensible on those terms.

---

## 3. Hard constraints and soft preferences

**Hard constraints (do not relax):**

- **Stay in JAX + NumPyro.**
- **Keep the exact Kalman-filter marginalization of the latent state trajectory** — the
  states are integrated out analytically; the sampler does not explore the full
  time-by-state latent grid directly. (See §5.6 and the important consequence in §5.5 /
  §7 about how this interacts with Gibbs and with the Student-t likelihood.)
- Preserve the **probabilistic** nature of the output (predictive densities, not point
  forecasts).

**Soft preferences / hypotheses (evaluate, don't assume):** see §4.

---

## 4. My hypotheses (candidate directions, not constraints)

I suspect the speedup requires:

1. **Gibbs sampling instead of NUTS.** Intuition: each NUTS transition costs many
   gradient evaluations, and each gradient is a full Kalman pass; a Gibbs sweep might be
   far cheaper per iteration.
2. **Gaussian (or otherwise conjugate) priors** in place of the current mix of truncated,
   log-normal, half-normal, and inverse-gamma priors — on the theory that conjugacy
   yields closed-form, cheap sampler updates.

**The tension I want you to resolve.** These two hypotheses sit awkwardly against the
hard constraint that the Kalman filter marginalizes the states:

- If the states are **marginalized**, the static parameters' conditional posteriors are a
  **nonlinear, non-conjugate** function of them (each evaluation is a Kalman forward
  pass). Conjugate priors then buy **no** closed-form Gibbs updates; you would be doing
  Metropolis-within-Gibbs, slice-within-Gibbs, or gradient-based blocking regardless.
- Classic **Carter–Kohn / FFBS Gibbs** *is* cheap and *does* exploit conjugacy — but only
  because it **samples the states** (forward-filter, backward-sample) rather than
  marginalizing them. That is the opposite of the hard constraint.

So "Gibbs + conjugate + keep the marginalization" does not straightforwardly cohere. I
want you to tell me which of the coherent regimes is the right bet on this hardware, and
why. The two regimes I can see are laid out in §7; there may be a hybrid (partially
collapsed / interweaving) scheme I haven't considered — if so, propose it.

One structural fact that helps the Gibbs case, described fully in §5.5: the heavy-tailed
(Student-t) observation likelihood is implemented as a **scale mixture of normals**, whose
auxiliary scale variables have **conjugate (inverse-gamma) full conditionals**. So even
in a marginalized-state regime, part of the model is naturally Gibbs-friendly.

---

## 5. The model (complete, self-contained)

The model is a **hierarchical dynamic-factor linear-Gaussian state-space model** for the
monthly latent employment-growth rates of `N = 14` industry leaves, with several noisy
observation channels per leaf plus aggregate channels, a deterministic seasonal component,
a deterministic covariate, heavy-tailed observation noise, and partial pooling of
parameters across a nesting tree.

Index notation:

- `i = 1, …, N` indexes the `N = 14` industry **leaves**.
- `t = 1, …, T` indexes months. `T` is on the order of ~100–200 (roughly a decade or two
  of monthly history; see §6).
- Superscripts index observation **channels** ("slots").

### 5.1 Latent state — common factor plus idiosyncratic components

The latent monthly growth rate of leaf `i` at time `t` is

```
    g_{i,t} = λ_i · f_t + u_{i,t}
```

with two dynamic pieces:

- **Common cyclical factor** `f_t`, shared across all `N` leaves, following an AR(1):

  ```
      f_t = φ_f · f_{t-1} + η_t,        η_t ~ Normal(0, σ_f²)
  ```

  Identified by fixing the innovation scale `σ_f ≡ 1` (the loadings `λ_i` absorb overall
  scale), with the sign ambiguity resolved by pinning one loading positive (a large,
  stable leaf is chosen for this).

- **Idiosyncratic AR(1)** per leaf:

  ```
      u_{i,t} = φ_{u,i} · u_{i,t-1} + ε_{i,t},        ε_{i,t} ~ Normal(0, σ_{u,i}²)
  ```

`λ_i` are the **factor loadings**; `φ_f` and `φ_{u,i}` are persistences (each constrained
to the stationary region `|φ| < 1`); `σ_{u,i}` are idiosyncratic innovation scales.

**This defines the latent state vector.** Stacking the factor and the `N` idiosyncratic
components (plus a small number of AR(1) *observation-error* states, §5.4) gives a state
of dimension

```
    d  ≈  1 (factor)  +  14 (idiosyncratic)  +  (0–4 provider AR(1) error states)  ≈  16–19.
```

The state evolves as a linear-Gaussian VAR(1): `x_t = A x_{t-1} + w_t`, `w_t ~ Normal(0, Q)`,
where `A` is block-diagonal in `(φ_f, φ_{u,1}, …, φ_{u,N}, …)` and `Q` is diagonal in the
innovation variances. **Crucially, `d` is small and fixed** — this is what makes the exact
Kalman filter affordable (see §5.6), and it is a deliberate design choice discussed next.

### 5.2 Seasonal — deterministic, static Fourier (not a state)

Each leaf has a **deterministic** seasonal path built from `K = 6` Fourier harmonics on a
12-month cycle:

```
    s_{i,t} = Σ_{k=1}^{K} [ a_{i,k} · cos(2π k t / 12) + b_{i,k} · sin(2π k t / 12) ]
```

The Fourier coefficients `{a_{i,k}, b_{i,k}}` are **static sampled parameters** (constant
over the sample), so `s_{i,t}` is a *fixed function of calendar month* given those
coefficients. **There is deliberately no seasonal state.**

This is the single biggest simplicity lever already taken: a time-varying (stochastic)
seasonal would add ~10 states per leaf and push `d` from ~16–19 up past ~130, which would
make a naive joint Kalman filter far more expensive and force block-structured filtering.
Holding the seasonal static keeps `d` small at the cost of a seasonal pattern that cannot
evolve year-to-year. **You may treat the static-seasonal choice as fixed for the purpose
of the speed question**, but note that the seasonal coefficients dominate the *static
parameter count* (see §5.7), which matters for sampler cost.

### 5.3 Deterministic covariate

A per-leaf, per-month **known covariate** `bd_{i,t}` (an exogenous adjustment term,
supplied as data — think of it as a deterministic regressor with a structural break at a
known date) enters certain observation channels additively. It carries **no free
parameters and no state** — it is a fixed input.

### 5.4 Observation channels ("slots")

At each `t`, the model stacks a fixed-order vector of noisy observations. Every channel
observes a **linear combination of the latent signal** plus a bias and heavy-tailed noise,
so conditional on the static parameters (and the scale-mixture auxiliaries of §5.5) the
whole system is **linear-Gaussian** and the Kalman filter applies exactly.

**Per-leaf channels** (each leaf `i` may have up to four):

| Channel | Observes (mean) | Free params | Noise scale |
|---|---|---|---|
| Survey-NSA (primary) | `λ^{s}_i · (g_{i,t} + s_{i,t} + bd_{i,t}) + α^{s}_i` | loading `λ^{s}_i` (near 1), bias `α^{s}_i` (near 0) | `σ^{s}_i`, externally calibrated |
| Survey-SA | `g_{i,t} + bd_{i,t}` | — (masked by default to avoid double-counting) | — |
| Census-lagged | `g_{i,t}` + scaled seasonal + scope offset, with a *soft* level anchor at a lag of ~6 months | seasonal-scale, offset | month-position × revision-tier multiplier |
| Provider-single | `λ^{p}_i · (g_{i,t} + s_{i,t}) + α^{p}_i` | `α^{p}_i`, `λ^{p}_i` (near 1), `σ^{p}_i` | `σ^{p}_i`; **iid or AR(1)** per leaf |

**Aggregate channels** (on the total private-sector target):

| Channel | Observes (mean) | Notes |
|---|---|---|
| Survey-aggregate | `Σ_i w_{i,t-1} · g_{i,t}` + aggregate seasonal | tight noise; a lagged-share **linear map** kept *inside the filter* so the system stays conditionally linear-Gaussian |
| Provider-aggregate | `Σ_i w_{i,t-1} · g_{i,t}` | one channel; captures signal that cannot be cleanly keyed to a single leaf |

The `w_{i,t-1}` are **known lagged share weights** (previous-period employment shares),
treated as fixed constants each month. The aggregate channels observe a share-weighted sum
of the leaf growth rates via a linear map applied *within* the Kalman observation equation,
which preserves the linear-Gaussian structure (any convexity gap versus an exact level-space
sum is negligible, ~1e-6).

**Missing data / masking.** Not every channel is available every month (e.g. the
census-lagged channel publishes on a delayed calendar; some provider cells are too thin to
use). Missing observations are handled by **inflating the corresponding noise-variance
diagonal entry by a large factor** (~1e6) rather than by changing the vector's dimension.
This keeps all array shapes **static across months** — important for JAX `jit`/`scan`
compilation and for the batch invariant (§6). Any faster scheme you propose must preserve
static shapes across time and across batch dates, or explicitly justify a dynamic-shape
alternative.

### 5.5 Heavy-tailed observation noise via a scale mixture (the Gibbs-friendly part)

Observation noise is **Student-t**, not Gaussian, to bound the influence of contaminated
months (a single bad print in one series should not exert leverage proportional to its
size). The Student-t is implemented as a **Geweke (1993) inverse-gamma scale mixture of
normals**:

```
    y_obs | ξ_obs  ~  Normal( mean,  ξ_obs · σ² )
    ξ_obs          ~  InverseGamma( ν/2,  ν/2 )
```

Marginalizing `ξ_obs` gives a Student-t with `ν` degrees of freedom. The degrees of freedom
`ν` are **pooled per channel type** (one `ν` for the survey channels, one for the census
channels, one for the provider channels), not per observation. Gaussian noise is the
`ν → ∞` limit and is available as an ablation.

**Why this matters for inference — read carefully, it drives §7:**

- To keep a **Gaussian** Kalman filter (which the hard constraint requires), you must
  **condition on the scale auxiliaries `ξ_obs`**: given them, each observation is Gaussian
  with a known inflated variance and the exact linear-Gaussian Kalman filter applies. So
  the auxiliaries `{ξ_obs}` are **sampled quantities** in this formulation.
- If you instead used the Student-t density *directly* (marginalizing `ξ_obs`
  analytically), the observation model would be non-Gaussian and the exact Kalman filter
  would no longer apply — you'd need an approximate/non-Gaussian filter. **That would
  violate the hard constraint.** So the scale-mixture-with-sampled-auxiliaries is
  effectively *forced* by "keep the Kalman marginalization."
- The upside: the auxiliaries `{ξ_obs}` have **conjugate inverse-gamma full conditionals**
  given the model's fitted residuals. This is the one place where conjugacy is real and a
  Gibbs update is genuinely cheap and exact. Any hybrid sampler should probably exploit it.

### 5.6 Current inference — Rao–Blackwellized NUTS + exact Kalman

- The **latent state trajectory is marginalized analytically** by a numerically stabilized
  (Joseph-form / Cholesky) Kalman filter running over the `T` months with the small state
  dimension `d ≈ 16–19`. One forward pass yields the **exact marginal log-likelihood** of
  all observations given the static parameters (and the scale auxiliaries), via the
  prediction-error decomposition.
- **NUTS** samples the **static parameters** (and the scale auxiliaries), using JAX
  autodiff to differentiate the Kalman log-likelihood. The latent states are *never*
  sampled during inference; if per-month latent paths are needed, they are reconstructed
  *after* sampling by a backward simulation pass.
- **Current sampler settings** (production preset): 4000 warmup + 3000 draws × 4 chains,
  `target_accept = 0.95`, max tree depth 10. This is the expensive part: at tree depth 10,
  a single NUTS transition can require up to ~1000 leapfrog steps, **each one a full Kalman
  forward pass** (plus its gradient). Multiply by warmup + draws × chains.

### 5.7 Parameter inventory (so you can size the samplers)

Static parameters the sampler must handle (order of magnitude for `N = 14`):

- Factor persistence `φ_f`: 1. (Innovation scale fixed.)
- Loadings `λ_i`: 14, plus a handful of pooling hyperparameters (§5.8).
- Idiosyncratic AR: persistences `φ_{u,i}` (14) and scales `σ_{u,i}` (14).
- **Seasonal Fourier coefficients:** up to ~11 per leaf × 14 leaves ≈ **~150** — this
  dominates the count.
- Per-channel loadings/biases/scales: survey (`λ^{s}_i, α^{s}_i` ≈ 28), provider-single
  (`α^{p}_i, λ^{p}_i, σ^{p}_i` ≈ 42), census scale/offset, aggregate-channel `α, λ, σ`.
- Degrees of freedom `ν`: 3 (pooled per channel type).
- Pooling hyperparameters (means and scales down the nesting tree): a few dozen.
- **Scale-mixture auxiliaries `{ξ_obs}`:** one per *observed* (unmasked) data point ≈
  `T × (number of active channels)` — this can be **large** (hundreds to a few thousand).

So the static-parameter space is **a few hundred smooth dimensions**, plus a **large block
of conditionally-conjugate auxiliaries**. It is emphatically *not* a ~15–20 dimensional
problem; the small number is the *state* dimension `d`, not the parameter count. A realistic
speed plan must handle the seasonal-coefficient block and the auxiliary block well, not just
the low-dimensional dynamics.

### 5.8 Priors and hierarchical pooling

Parameters are **partially pooled down a nesting tree**: `leaf → segment group → domain`.
Loadings, persistences, idiosyncratic scales, and seasonal-amplitude scales shrink toward
group-level means, which shrink toward domain-level means. The pooling is **non-centered**
(e.g. `λ_i = μ_{λ,group(i)} + τ_λ · z_i` with `z_i ~ Normal(0,1)`), which is the standard
device for making hierarchical geometry tractable for gradient samplers.

Current priors are an **informative, non-Gaussian mix**:

- Loadings: (near-)Gaussian hyper-means with half-normal pooling scales; some loadings
  truncated-normal (positivity / near-1 constraints).
- Persistences `φ`: constrained to `|φ| < 1` (stationarity) — effectively truncated.
- Innovation and noise scales `σ`: log-normal or inverse-gamma (positivity + calibration).
- Survey noise scales: log-normal centered on externally supplied relative-standard-error
  values (an empirical calibration input), i.e. genuinely informative.
- Degrees of freedom `ν`: weakly-informative positive priors.

**These priors are a soft target for change** (your hypothesis #2). Note two things when you
consider "make them all Gaussian/conjugate":

1. Several parameters are **intrinsically constrained** (`σ > 0`, `|φ| < 1`, some loadings
   positive). A plain Gaussian prior is either wrong (puts mass in forbidden regions) or
   must be composed with a transform (softplus/tanh), which *reintroduces* nonlinearity and
   kills conjugacy. So "Gaussian priors" and "conjugate updates" are not the same request,
   and neither is free.
2. The **calibration content** of the informative scale priors (especially the survey-noise
   priors tied to published standard errors) is doing real work for coverage. Flattening
   them to generic Gaussians could degrade the very density quality the model exists to
   deliver. If you propose changing them, say what is lost.

### 5.9 Aggregation to the target

The reported nowcast is assembled **per posterior draw** by an exact bottom-up sum in
levels:

```
    ΔE_target,t^(s)  =  Σ_i  E_{i,t-1}^{pub} · ( exp(g_{i,t}^(s)) − 1 )
```

for draw `s`, summed over the `N = 14` leaves, using published previous-period levels
`E_{i,t-1}^{pub}` as known constants. Because the sum is taken *within* each draw, the
predictive distribution of the aggregate automatically carries the factor-induced
cross-leaf correlation — no post-hoc reconciliation. This step is cheap and is not the
bottleneck; it is included so you understand how the leaf-level posterior becomes the
aggregate predictive density that CRPS scores.

---

## 6. What a "fit" is, for sizing

- **State dimension** `d ≈ 16–19`, fixed across all months.
- **Time length** `T` on the order of ~100–200 monthly observations.
- **Channels** per month: up to ~4 per leaf × 14 leaves + 2 aggregate, with masking; the
  number of *active* (unmasked) observations per month varies but the *array shape* is
  fixed.
- **Batch invariant.** The production use fits **many monthly origins** ("vintages") — each
  a real-time snapshot with its own as-of masking — and requires that a **batched**
  multi-origin fit reproduce the **serial** single-origin fits (a hard parity check). Any
  faster scheme must preserve this: **padding + masks, never shape changes across batch
  dates.** Vectorizing across origins with `vmap` is expected; changing a sampled site's
  dimension across origins is not allowed.
- A single fit today is the 4000+3000 × 4-chain NUTS run described in §5.6. "Fast enough"
  means bringing one such fit (or its replacement) down to minutes-to-low-tens-of-minutes
  on the §1 hardware, at equal or acceptably-degraded density quality.

---

## 7. The core question — inference on a small box

Here is the decision I need help with, framed as the two coherent regimes the hard
constraints allow, plus a menu of orthogonal levers. **Recommend a regime, justify it
against the CRPS/coverage objective, and rank the levers.**

### Regime A — Keep the state marginalization; make the *sampler* over static params cheaper

Stay Rao–Blackwellized: the Kalman filter integrates out the states; you sample the static
parameters (and the scale auxiliaries). The parameters' conditionals are **non-conjugate**
(each is a Kalman pass), so this regime is **not** classic conjugate Gibbs. Options within it:

- **Cheaper NUTS/HMC.** Lower `target_accept` (0.95 → ~0.8–0.9), cap tree depth lower,
  shorten warmup with a better initial mass-matrix estimate, weigh a dense vs diagonal mass
  matrix. Each reduces leapfrog steps (⇒ Kalman passes) per transition. **Cost:** more
  divergences / worse geometry ⇒ risk to calibration. Quantify the CRPS/coverage hit.
- **Metropolis-within-Gibbs / slice-within-Gibbs on parameter blocks**, still using the
  Kalman marginal likelihood but updating blocks (dynamics; each channel's loadings/scales;
  the seasonal block per leaf; the `ν`s) in turn. This is the honest reading of "Gibbs while
  keeping the marginalization." **Question:** does block-wise Metropolis actually beat NUTS
  in ESS-per-second here, given the strong posterior correlations the factor structure and
  non-centered pooling induce? My prior is that it usually does *not* — NUTS's gradient use
  is hard to beat on a few-hundred-dim smooth target — but each NUTS gradient is expensive
  here, so the balance is genuinely unclear. Tell me which way it breaks and why.
- **Exploit the conjugate auxiliaries regardless.** The scale-mixture `{ξ_obs}` (§5.5) have
  exact inverse-gamma conditionals. Even in a mostly-NUTS scheme, drawing them in a Gibbs
  step (rather than sampling them with NUTS) removes a large, awkward block from the
  gradient sampler. Is this worth it?
- **Make the Kalman pass itself cheaper** (helps *every* option above): see the levers in
  §8 — steady-state gain, square-root/float32 filtering, exploiting the block-diagonal /
  low-rank structure of `A` and `Q`, and the fact that only a share-weighted linear
  functional of the state is observed at the aggregate.

### Regime B — Relax the marginalization: data-augmented (FFBS) Gibbs

This is the classic fast state-space sampler and the natural home for your two hypotheses,
**but it drops the "keep the Kalman marginalization" hard constraint** — so only consider
it if you argue the trade is worth it. Here the Kalman filter is used for
**forward-filter-backward-sample (FFBS)** rather than for marginalization:

- **Sample the full state trajectory** `{x_t}` in one FFBS block (one forward Kalman pass +
  one backward sampling pass — no gradients, no 1000-leapfrog trees).
- **Sample the static parameters conditional on the states**, where **Gaussian priors give
  genuinely conjugate closed-form draws**: AR coefficients and loadings from Normal
  conditionals, innovation/noise variances from inverse-gamma conditionals, seasonal Fourier
  coefficients from Normal conditionals (the seasonal is linear in its coefficients).
- **Sample the scale-mixture auxiliaries** `{ξ_obs}` from their inverse-gamma conditionals
  (Student-t data augmentation — this is exactly the Geweke construction).
- The constrained parameters (`|φ|<1`, positive loadings) need a Metropolis or
  truncated/rejection step, so it is *Metropolis-within-Gibbs* in practice, not pure Gibbs.

**Per-iteration cost** is dramatically lower than a NUTS transition (no leapfrog trees, no
gradients; one FFBS pass + cheap conjugate draws). **The trade-offs I want you to weigh:**

- **Mixing / autocorrelation.** FFBS-Gibbs can mix slowly when states and parameters are
  highly correlated (the factor loadings ↔ factor path is the classic pathology). Slow
  mixing ⇒ many more iterations for the same ESS ⇒ the per-iteration win can evaporate.
  Interweaving / ancillarity–sufficiency (ASIS) or partially-centered tricks may be needed.
- **Density calibration.** Does the resulting predictive distribution match the RB-NUTS one
  closely enough on CRPS / PIT / 50-80-95 coverage? The whole justification for NUTS was
  cleaner density calibration; if FFBS-Gibbs matches it at a fraction of the cost, that is a
  strong result — but it must be *demonstrated*, e.g. by a parameter-recovery + simulated-
  coverage check on synthetic data before trusting it.
- **Memory.** Sampling `{x_t}` materializes a `T × d` trajectory per draw (× chains). At
  `T ≈ 150`, `d ≈ 18`, this is small — but confirm it stays within the 15–20 GB budget once
  batched across origins (§6) and across chains.
- **JAX fit.** FFBS with conjugate + Metropolis blocks is expressible in NumPyro but is more
  bespoke than calling `NUTS` — it likely means a custom sampler / `numpyro` custom kernel or
  a hand-rolled `lax.scan`-based FFBS. Flag the implementation risk.

### The comparison I actually want

Which regime — A (marginalized, cheaper sampler over static params) or B (data-augmented
FFBS-Gibbs) — gives the best **effective-samples-per-second at acceptable calibration** on
the §1 hardware, for a model of this size (§5.7, §6)? Is there a **hybrid** — e.g. keep the
marginalization but Gibbs-draw the conjugate auxiliaries and Fourier block while NUTS-ing the
dynamics; or a partially-collapsed sampler that marginalizes the factor but samples
idiosyncratic states — that dominates both? Please reason about ESS/sec, not just per-iteration
cost, and be explicit about the calibration risk of each.

---

## 8. Orthogonal speed levers (rank these for the §1 hardware)

These compose with either regime. I want them **ranked by expected wall-clock payoff on 4
CPU cores / 15–20 GB, with the calibration/accuracy risk of each stated**. Note which assume
parallelism this machine does not have.

**Sampler / schedule**

1. Fewer chains or shorter chains — and whether 4 chains on 4 cores is the right split, or
   whether 2 longer chains give better ESS/sec once compilation and memory contention are
   counted.
2. Warmup reduction via a good initial mass matrix / step size (e.g. from a pilot run or a
   Laplace/MAP pre-solve), since warmup is a large share of NUTS cost.
3. Lower `target_accept` and tree-depth cap (Regime A) — with the calibration caveat.
4. **SVI / Laplace / INLA-style approximation as a *warm start or fallback*.** A variational
   or Laplace approximation to the static-parameter posterior is cheap; could it (a) initialize
   the sampler, (b) provide the mass matrix, or (c) even *replace* MCMC for iteration-time work
   if calibration holds? INLA-style deterministic integration is a real contender for a
   marginalized-state model — evaluate it, including its weaknesses for the hierarchical /
   heavy-tailed pieces.

**Kalman-pass cost (the inner loop)**

5. **Steady-state Kalman gain.** For a time-invariant `A, Q` and constant observation
   structure, the filter covariance converges; using the steady-state gain after a burn-in
   avoids per-step Riccati updates. Complication: the *masking* and the lagged-share aggregate
   map make the observation operator time-varying, so the gain is not globally constant —
   assess how much of the series is effectively stationary and whether a piecewise steady-state
   gain is safe.
6. **Square-root / Joseph-form filter in float32.** Can precision be dropped to float32 with a
   numerically stable square-root filter without destabilizing the log-likelihood or its
   gradient? This roughly halves memory and can speed CPU linear algebra. Give the risk.
7. **Exploit structure in `A`, `Q`, and the observation map.** `A` is block-diagonal
   (factor + independent AR(1)s), `Q` diagonal, and the state is low-dimensional; the aggregate
   channels observe only a *linear functional* of the state. Are there `O(d²)`-instead-of-`O(d³)`
   or rank-structured updates worth exploiting at `d ≈ 18`? (At this small `d`, dense may
   already be fine — say so if it is.)
8. **Associative-scan / parallel-in-time Kalman filtering** (`jax.lax.associative_scan`). This
   is a large win on GPUs/many cores but the parallel work needs cores to cash in — on 4 CPU
   cores it may not beat a sequential `lax.scan`. Judge whether it helps *here*.

**Model-side simplifications (only if they pay for themselves)**

9. Reduce the seasonal harmonic count `K` (fewer Fourier coefficients ⇒ smaller static-param
   block) if it barely moves the fit.
10. Reduce channel redundancy where two channels carry near-identical information.
11. Anything else that shrinks the static-parameter or auxiliary blocks without materially
    hurting the aggregate predictive density.

For each lever: **estimated speedup, risk to CRPS/coverage, and whether it needs cores/GPU
this box lacks.**

---

## 9. How I will judge a proposed scheme (please design for this)

Any replacement inference scheme must be **verifiable** against the current RB-NUTS fit:

- **Parameter recovery on synthetic data.** Simulate from the generative model with known
  parameters; confirm the new sampler recovers `(φ_f, λ_i, φ_{u,i}, σ's, seasonal amplitudes,
  ν's)` within tolerance.
- **Simulated-coverage check.** On simulated data, confirm nominal 50/80/95% coverage of both
  the latent path and the target's predictive distribution. This is what proves the scheme is
  a correct posterior approximation and not an overconfident shortcut — it is the single most
  important gate, because the model's value *is* its calibration.
- **Agreement with RB-NUTS** on a real fit: posteriors and the aggregate predictive density
  should match RB-NUTS within Monte Carlo error (CRPS, PIT, 50/80/95 coverage). A faster
  scheme that shifts these is only acceptable if you can argue the shift is toward, not away
  from, ground truth.
- **Standard MCMC diagnostics** must still pass: no divergences (or a defensible few), R-hat
  ≤ ~1.01, adequate ESS (bulk and tail), reasonable BFMI. If a scheme trades these for speed,
  say so quantitatively.

Design your recommendation so these checks are cheap to run.

---

## 10. What I want back

1. **A recommended inference scheme** — regime (A, B, or a hybrid), sampler family,
   parameterization, and any model/prior changes it entails — with the reasoning that led you
   there and the trade-offs you weighed. Address my Gibbs / conjugate-prior hypotheses head-on:
   confirm, refine, or reject them, and resolve the marginalization tension in §4.
2. **The expected wall-clock and ESS/sec** on the §1 hardware, at least to an order of
   magnitude, versus the current NUTS fit — and the **calibration cost**, if any.
3. **A ranked list of the §8 levers** for this hardware, composable with the recommendation.
4. **Key equations or pseudo-code** for the non-obvious parts (e.g. the conjugate full
   conditionals if you recommend Gibbs; the FFBS recursion; the scale-auxiliary update; any
   custom NumPyro kernel structure) — enough that I could implement it.
5. **The verification plan** (§9) instantiated for your scheme.
6. **Honest failure modes** — where your recommendation could mix poorly, miscalibrate, or
   blow the memory budget, and how I would detect each.

Be a critical thinking partner: if my premise (that a sampler change is the main lever) is
wrong — e.g. if the real win is a cheaper Kalman pass, a better warm start, or a variational
approximation, and the sampler choice barely matters — **say so and make that case.**

---

## Appendix — notation quick reference

| Symbol | Meaning |
|---|---|
| `N = 14` | number of industry leaves |
| `T` | number of monthly time steps (~100–200) |
| `d ≈ 16–19` | latent state dimension per time step (marginalized by the Kalman filter) |
| `g_{i,t}` | latent monthly growth rate of leaf `i` |
| `f_t`, `φ_f` | common cyclical factor and its AR(1) persistence (`σ_f ≡ 1`) |
| `λ_i` | factor loading of leaf `i` |
| `u_{i,t}`, `φ_{u,i}`, `σ_{u,i}` | idiosyncratic AR(1) level, persistence, innovation scale |
| `s_{i,t}`, `{a_{i,k}, b_{i,k}}`, `K = 6` | deterministic Fourier seasonal and its static coefficients |
| `bd_{i,t}` | deterministic known covariate (exogenous input) |
| `α`, `λ^{·}`, `σ^{·}` | per-channel bias, loading, noise scale |
| `w_{i,t-1}` | known lagged share weights for the aggregate linear map |
| `ξ_obs`, `ν` | per-observation scale-mixture auxiliary and pooled degrees of freedom (Student-t) |
| `E_{i,t-1}^{pub}` | known published previous-period level, used in level-space assembly |

**Objective:** calibrated predictive density for the aggregate target, scored primarily by
**CRPS**, secondarily by PIT uniformity and 50/80/95% interval coverage — while running fast
on ~4 CPU cores and 15–20 GB RAM, staying in JAX/NumPyro, and keeping the exact Kalman
treatment of the latent states (subject to the Regime-A/B trade-off in §7).
