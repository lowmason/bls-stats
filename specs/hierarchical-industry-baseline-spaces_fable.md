# Fast Inference for a Hierarchical Dynamic-Factor Nowcast on a 4-Core Box: A Decision-Ready Recommendation

## TL;DR

- **Keep Rao–Blackwellized NUTS (Regime A), but restructure it into a hybrid: NUTS on the few-hundred smooth static parameters with the states marginalized by an exact Joseph-form Kalman filter, plus a gradient-free Gibbs block (via `numpyro.infer.HMCGibbs`) that draws the Student-t scale auxiliaries `{ξ_obs}` from their exact inverse-gamma conditionals.** This resolves your §4 tension: conjugacy is real *only* for the auxiliaries, so that is the only place a Gibbs step is worth taking; the smooth static block stays with NUTS, where gradient use is hard to beat on a few-hundred-dimensional correlated target.
- **The sampler family is not your main lever.** The evidence says the dominant wall-clock wins on this hardware come from (1) a Pathfinder/Laplace warm start that collapses NUTS warmup and seeds the mass matrix, (2) shrinking the NUTS dimension by Gibbs-drawing the auxiliary block, and (3) a cheaper inner Kalman pass — not from switching to Gibbs wholesale. Full FFBS-Gibbs (Regime B) is a legitimate fallback but it *drops* your marginalization constraint, needs ASIS/interweaving to avoid the loadings↔factor-path mixing pathology, and forces you to flatten the informative survey-noise priors that are doing real calibration work.
- **Expected effect: roughly an order of magnitude in ESS/second** versus your current 4000+3000×4 preset — realistically bringing a fit from hours to ~10–30 minutes — at essentially unchanged calibration if you gate the change behind the §9 simulated-coverage check. Reject hypothesis #2 (blanket Gaussian/conjugate priors); confirm hypothesis #1 only in its narrow, auxiliary-block form.

## Key Findings

**1. Under state marginalization, conjugate priors buy you almost nothing — so the "Gibbs + conjugate" hypothesis is mostly a non-lever.** Your own diagnosis in §4 is correct and I confirm it: once the Kalman filter integrates out the state trajectory, the static parameters' conditionals are a nonlinear, non-conjugate function of them (each evaluation is a full forward pass), so a conjugate prior does not produce a closed-form draw. The one exception is exactly the one you identified: the Geweke (1993) inverse-gamma scale-mixture auxiliaries `{ξ_obs}`, whose full conditionals are genuinely conjugate. That is the only block where "Gibbs because conjugate" is true, and it is where the hybrid puts its Gibbs step.

**2. The literature now contains a near-exact template for your situation, and it favors keeping NUTS + marginalization.** Øystein Sørensen (Dept. of Psychology, University of Oslo), "Efficient Bayesian Estimation of Dynamic Structural Equation Models via State Space Marginalization" (arXiv:2603.04003), benchmarks precisely the three samplers in your decision: NUTS with Kalman marginalization ("NUTS-Kalman"), brute-force NUTS over all states ("NUTS-Joint"), and Asparouhov-style Metropolis-within-Gibbs. For a scalar-latent AR(1) multilevel model, NUTS-Kalman was **8–19× more efficient (ESS per unit time) than Metropolis-within-Gibbs, and 10–18× more efficient than NUTS-Joint**; the paper is explicit that the advantage "was not due to Stan or NUTS in and of itself, but rather the use of the Kalman filter to marginalize over the within-level latent variables," and its abstract states the approach "can be orders of magnitude more efficient than standard Metropolis-within-Gibbs approaches." Crucially, NUTS-Kalman hit R-hat < 1.01 on ~100% of runs while Metropolis-within-Gibbs cleared 1.01 on only about 39–59% of runs — a direct calibration/convergence argument for the marginalized sampler.

**3. But your model sits in the regime where the margin narrows — which is why the hybrid, not pure NUTS, is the bet.** The same paper shows the NUTS-Kalman advantage *shrinks as the number of indicators per latent grows* (with ≥2–3 indicators, Metropolis-within-Gibbs can actually win) and as AR order grows (bulk-ESS multipliers of 18, 11, 6, 4 for AR lags 1–4). Your model has up to four channels per leaf plus two aggregate channels — a "many indicators per latent" structure. In its most complex cross-classified VAR(1) example (state dimensions comparable to yours, `T=150`), NUTS-Kalman's edge fell to **1.50× bulk / 1.62× tail** over Gibbs (median wall time 5.3 h vs 6.1 h). So the pure-NUTS margin over Gibbs is real but not enormous for a model shaped like yours; the leverage is in making each NUTS transition cheaper and pulling the awkward auxiliary block out — which is exactly the hybrid.

**4. The hybrid NUTS-Gibbs structure is already validated in NumPyro/JAX and yields ~5× on the awkward block.** The companion paper — Sørensen & McCormick, "A Hybrid NUTS-Gibbs Sampler with State Space Marginalization for Estimation of Dynamic Structural Equation Models with Binomial Outcomes" (arXiv:2603.29647) — implements exactly this pattern in NumPyro: the Gibbs step "naturally handles Pólya-Gamma distributed latent variables … and the NUTS step utilizes a Kalman filter to exactly marginalize over latent states," reducing the parameters sampled per iteration from O(N·T) to O(N+T). On a nine-indicator binomial VAR(1) it delivered **5.1× bulk-ESS/s and 4.7× tail-ESS/s over pure NUTS** (0.51 vs 0.10 bulk-ESS/s; ~33 min vs ~2.8 h to reach bulk-ESS 1,000), the gain "was even more pronounced" with three latent traits and participant-varying dynamics, and the authors note "NumPyro is consistently more efficient than Stan." For you the conjugate block is inverse-gamma (Geweke) rather than Pólya-Gamma, but the architecture is identical — direct evidence that your §7 "exploit the conjugate auxiliaries regardless" option is worth it.

**5. Warm starts are the cheapest large win and are first-class in this stack.** Zhang, Carpenter, Gelman & Vehtari (2022), "Pathfinder: Parallel Quasi-Newton Variational Inference" (JMLR 23(306):1–49), report that "compared to ADVI and short dynamic HMC runs, Pathfinder requires one to two orders of magnitude fewer log density and gradient evaluations, with greater reductions for more challenging posteriors," and its inverse-covariance estimate can seed the HMC mass matrix directly. Since your production preset spends 4000 of 7000 iterations (~57%) on warmup, and each warmup gradient is a full Kalman pass, cutting warmup is a direct, low-risk multiplier.

## Details

### Recommended scheme (Regime A, hybrid): the "what" and the "why"

Run a single `numpyro.infer.HMCGibbs` kernel with:

- **NUTS (inner kernel) over the smooth static parameters** — factor persistence `φ_f`, loadings `λ_i` and their pooling hyperparameters, idiosyncratic `φ_{u,i}`/`σ_{u,i}`, the ~150 seasonal Fourier coefficients, per-channel `α/λ/σ`, and the three pooled `ν`s — with the latent state trajectory analytically marginalized by the Joseph-form Kalman filter inside the model's likelihood. This preserves your hard constraint exactly and keeps the expensive gradient sampler operating on a low-dimensional, smooth, well-identified marginal target, which is where NUTS dominates Gibbs.
- **A Gibbs step for the scale-mixture auxiliaries `{ξ_obs}`**, drawn from their exact inverse-gamma conditionals. This removes a large (hundreds-to-thousands), heavy-tailed, geometrically awkward block from the gradient sampler — the block that most degrades NUTS step size and tree depth.

**Resolving the §4 marginalization tension precisely.** There is one subtlety you should implement deliberately: the inverse-gamma conditional for `ξ_obs` depends on the realized model residual for that observation, which depends on the latent state. With the state marginalized you do not have that residual in hand. The clean, correct fix is a *partially collapsed* (van Dyk & Park, 2008) construction: inside the Gibbs step, draw one state trajectory with a Durbin–Koopman (2002) simulation smoother (equivalently Carter–Kohn/FFBS) conditional on the current static parameters, form the residuals, then draw `ξ_obs` from inverse-gamma. The states are still *marginalized for the NUTS gradient* (so the smooth block enjoys the Rao–Blackwellized geometry), and are only instantiated transiently to service the conjugate auxiliary draw. You already run a backward simulation pass post-hoc to reconstruct latent paths, so this reuses code you have. Heed van Dyk & Jiao (2013): with any Metropolis-in-the-loop, the *ordering* of partially-collapsed steps can break stationarity, so keep the auxiliary draw as a genuine conditional draw and do not permute a marginalized quantity into a conditioned-upon position.

**Verdict on your two hypotheses.**
- *Hypothesis #1 (Gibbs instead of NUTS): refined, not accepted wholesale.* Gibbs is the right tool only for the conjugate auxiliaries. Block Metropolis-/slice-within-Gibbs over the *dynamics and loadings* would fight the strong factor-induced posterior correlations and the non-centered hierarchical geometry; your prior that it usually loses to NUTS on a few-hundred-dimensional smooth target is correct, and the Sørensen numbers support it.
- *Hypothesis #2 (Gaussian/conjugate priors): reject as a speed lever, keep a subset for other reasons.* Under marginalization, conjugacy does not yield closed-form static-parameter updates, so making priors Gaussian does not speed the sampler; and your constrained parameters (`σ>0`, `|φ|<1`, positive loadings) would need softplus/tanh transforms that reintroduce nonlinearity and destroy conjugacy anyway. Worse, flattening the informative survey-noise priors (tied to published relative standard errors) would degrade exactly the interval coverage the model exists to deliver. Keep them.

### Why Regime B (FFBS-Gibbs) is the fallback, not the recommendation

FFBS-Gibbs has a genuinely lower per-iteration cost (one forward Kalman pass + one backward sample + cheap conjugate draws, no leapfrog trees, no gradients), and it is the natural home for your hypotheses. But three things count against it *for this model*:

1. **It drops your hard constraint** (it samples the states rather than marginalizing them).
2. **Mixing.** The factor-loading↔factor-path correlation is the classic FFBS pathology; naive Gibbs can mix so slowly that the per-iteration win evaporates. Kastner, Frühwirth-Schnatter & Lopes (2017), "Efficient Bayesian Inference for Multivariate Factor Stochastic Volatility Models" (J. Computational & Graphical Statistics 26(4):905–917, doi:10.1080/10618600.2017.1322091), show that for factor stochastic-volatility models the fix — ancillarity–sufficiency interweaving (ASIS/Yu–Meng 2011) — is essential: their interweaving strategies "are easy to implement and come at almost no extra computational cost; nevertheless, they can boost estimation efficiency by several orders of magnitude." That is real implementation work (two parameterizations, interwoven).
3. **Calibration risk from prior flattening.** Conjugate closed-form draws require Gaussian/inverse-gamma priors, i.e. abandoning the informative survey-noise calibration.

Because your `d` is small (~18) and `T` short (~150), the marginalized sampler's per-pass cost is modest, so the FFBS per-iteration advantage is smaller than in the large-`d` settings where FFBS shines. Net: keep FFBS-Gibbs as a benchmarked alternative, and only switch if the simulated-coverage gate (below) shows it matches RB-NUTS *and* the ESS/sec wins on your hardware.

### Order-of-magnitude expectations on 4 cores / 15–20 GB

Your current preset is 4000 warmup + 3000 draws × 4 chains at `target_accept=0.95`, max tree depth 10 → up to ~1023 leapfrog steps per transition, each a full Kalman pass plus its gradient. The dominant costs and expected multipliers:

- **Warm start (Pathfinder or `AutoLaplaceApproximation`)** → cut warmup from 4000 to ~500–1000 and seed the mass matrix: ~**1.5–2×** on total wall clock, low calibration risk.
- **Gibbs-draw the auxiliaries + lower `target_accept` (0.95→0.9) + tree-depth cap (~7–8)** → fewer leapfrogs per transition and a smaller, better-conditioned gradient target: ~**2–4×** in ESS/sec, per the ~5× the companion paper reports for pulling the conjugate block out plus the cheaper trees.
- **Cheaper Kalman pass** (below): ~**1.3–2×**, composable.

Composed, an order-of-magnitude improvement in ESS/sec is realistic, plausibly moving a multi-hour fit to ~10–30 minutes. Treat these as multiplicative planning estimates, not guarantees — the honest ceiling on the marginalization benefit itself is the 8–19× that Sørensen measured, and your many-channel structure sits toward the lower end of that band.

**Chains on 4 cores.** With `chain_method='parallel'` NumPyro maps chains across XLA devices, so you must call `numpyro.set_host_device_count(4)` (or set `XLA_FLAGS=--xla_force_host_platform_device_count=4`) or it silently falls back to sequential. On a 4-physical-core box, 4 parallel chains saturate the machine and contend for cache/memory bandwidth; **2 longer chains often give better ESS/sec** once compilation and contention are counted, and still permit a usable (if lower-powered) R-hat. `chain_method='vectorized'` runs chains via `vmap` on one device — attractive because it compiles once, but it raises peak memory (all chains' state live simultaneously); watch the 15–20 GB budget. Recommendation: benchmark 2 parallel × long vs 4 parallel, and prefer 2×long if R-hat/ESS hold.

### The §8 levers, ranked for 4 cores / 15–20 GB

1. **Warmup reduction via Pathfinder/Laplace warm start (lever #2/#4).** Highest payoff, lowest risk. Pathfinder needs one-to-two orders of magnitude fewer gradient evals than Phase-I warmup and supplies both an init and an inverse-mass-matrix estimate. No extra cores needed. Risk to CRPS/coverage: negligible (it only initializes exact MCMC). *Speedup ~1.5–2×.*
2. **Gibbs-draw the conjugate auxiliaries `{ξ_obs}` (lever within Regime A).** Shrinks the NUTS dimension by the largest, most awkward block. ~5× on that block in the companion paper. Risk: implementation correctness (partially-collapsed ordering) — mitigated by the coverage gate. No extra cores. *Speedup ~2–4× overall.*
3. **Fewer/longer chains matched to 4 cores (lever #1).** 2×long vs 4×parallel; and consider `chain_method='vectorized'` if memory allows. Risk: fewer chains weakens R-hat power — keep ≥2. *Speedup ~1.2–1.5× ESS/sec.*
4. **Lower `target_accept` (0.95→0.9) and cap tree depth (~7–8) (lever #3).** Directly cuts leapfrog steps ⇒ Kalman passes. Risk: divergences/geometry — must be gated on divergence count and coverage; do *not* go below ~0.85 without re-checking PIT. *Speedup ~1.3–2×.*
5. **Cheaper Kalman inner loop — sequential scalar updates & structure (lever #7).** At `d≈18` dense linear algebra is already fine, so do **not** chase `O(d²)` rank tricks first. The higher-value move is to process the up-to-~58 observations per month **one at a time** (Koopman–Durbin univariate treatment), turning each `m×m` innovation-covariance inversion into `m` scalar updates at `O(d²)` each and making the missing-data masking trivial (skip masked scalars instead of inflating a diagonal). Risk: low, exact. *Speedup ~1.3–2×.*
6. **Square-root / Joseph-form filter, and only then consider float32 (lever #6).** Keep float64 as the safe default (`jax.config.update('jax_enable_x64', True)` / `numpyro.enable_x64()` at startup — required before any array creation, it is a global flag). A QR-based square-root filter — Kevin S. Tracy, "A Square-Root Kalman Filter Using Only QR Decompositions" (arXiv:2208.06452, 2022) — is "able to achieve the same accuracy as the traditional Kalman filter with half as many bits of precision," i.e. "benefiting from double the working precision," so it is the *precondition* for any float32 experiment; naive covariance-form float32 will destabilize the log-likelihood gradient and cause divergences. Only move the filter to float32 after the square-root rewrite and only if the coverage gate passes. Risk: high if done without the square-root form; medium with it. *Speedup ~1.5–2× on CPU linear algebra + memory halving — but treat as experimental.*
7. **Steady-state Kalman gain (lever #5).** Tempting for time-invariant `A,Q`, but your masking and lagged-share aggregate map make the observation operator time-varying, so the gain is *not* globally constant. Only the interior, fully-observed stretches are effectively stationary. A piecewise steady-state gain (converge once on a representative fully-observed window, reuse until the mask/weights change) can help but adds bookkeeping and a correctness risk. Risk: medium (silent bias if applied across a mask change). *Speedup: modest, situational.*
8. **Model-side shrinkage: reduce seasonal harmonics `K` (levers #9–11).** The ~150 Fourier coefficients dominate the static-parameter count. If dropping `K=6→4` barely moves held-out CRPS, you remove ~50 dimensions from the NUTS target for free. Risk: a too-aggressive cut degrades the seasonal fit and thus coverage — test on held-out CRPS. *Speedup: small-to-moderate, and it also helps memory.*
9. **Associative-scan / parallel-in-time Kalman filtering (lever #8).** Särkkä & García-Fernández's `jax.lax.associative_scan` filter is `O(log T)` span *given enough parallel workers*. On 4 CPU cores with `T≈150` there is not enough parallelism to cash that in, and the larger constant factors and memory traffic typically make it **lose to a sequential `lax.scan`** here. Skip it on this box (it is a GPU/many-core lever). Risk: n/a (don't use). *Speedup: none expected on 4 cores.*

### Key equations and pseudo-code

**Joseph-form Kalman prediction-error decomposition (the marginal log-likelihood NUTS differentiates).** For state `x_t = A x_{t-1} + w_t`, `w_t ~ N(0, Q)`, and observation `y_t = H_t x_t + b_t + v_t`, `v_t ~ N(0, R_t)` where `R_t = diag(ξ_t · σ²)` carries the scale-mixture inflation and the ~1e6 masking inflation:

```
def kalman_loglik(params, xi, y, mask):
    A, Q, H, R0, m0, P0 = build_system(params)
    d = m0.shape[0]

    def step(carry, inputs):
        m, P, ll = carry
        y_t, H_t, b_t, r_t = inputs
        # predict
        m_pred = A.dot(m)
        P_pred = A.dot(P).dot(A.T).add(Q)
        # innovation (masked entries get inflated r_t, contributing ~0 information)
        v = y_t.subtract(H_t.dot(m_pred)).subtract(b_t)
        S = H_t.dot(P_pred).dot(H_t.T).add(jnp.diag(r_t))
        L = jnp.linalg.cholesky(S)
        Kt = jsp.linalg.cho_solve((L, True), H_t.dot(P_pred)).T
        m_upd = m_pred.add(Kt.dot(v))
        # Joseph-form covariance update (numerically stable, PSD-preserving)
        ImKH = jnp.eye(d).subtract(Kt.dot(H_t))
        P_upd = ImKH.dot(P_pred).dot(ImKH.T).add(Kt.dot(jnp.diag(r_t)).dot(Kt.T))
        # prediction-error decomposition
        alpha = jsp.linalg.solve_triangular(L, v, lower=True)
        ll_t = jnp.log(jnp.diag(L)).sum().multiply(-1.0) \
            .subtract(0.5 * alpha.dot(alpha)) \
            .subtract(0.5 * v.shape[0] * jnp.log(2.0 * jnp.pi))
        return (m_upd, P_upd, ll.add(ll_t)), None

    (mT, PT, ll), _ = jax.lax.scan(step, (m0, P0, 0.0), (y, H, b, R0 * xi))
    return ll
```

**Scale-auxiliary inverse-gamma full conditional (Geweke 1993).** For a Student-t channel with `ν` degrees of freedom, given the realized residual `r_obs = (y_obs − mean_obs)` and its Gaussian scale `σ`:

```
    xi_obs | .  ~  InverseGamma( (nu + 1) / 2 ,  (nu + (r_obs / sigma) ** 2) / 2 )
```

Pool the `ν` update per channel type as in your current model. In the partially-collapsed step, `r_obs` is formed from a single Durbin–Koopman state draw.

**Durbin–Koopman (2002) simulation smoother, to service the auxiliary draw (Jarociński 2015 correction: zero the initial-state mean in the simulated system).**

```
def dk_simulation_smoother(params, xi, y, key):
    # 1. draw a synthetic (x+, y+) forward from the model with zero-mean initial state
    x_plus, y_plus = simulate_forward(params, xi, key)
    # 2. smoother mean of the real data and of the synthetic data (same Kalman code)
    xhat = kalman_smoother_mean(params, xi, y)
    xhat_plus = kalman_smoother_mean(params, xi, y_plus)
    # 3. the draw
    return xhat.add(x_plus).subtract(xhat_plus)
```

**NumPyro `HMCGibbs` wiring (validated at numpyro>=0.16.1; API stable through current 0.20).** `HMCGibbs` takes an inner NUTS kernel, a `gibbs_fn(rng_key, gibbs_sites, hmc_sites)` returning a dict of the Gibbs-updated sites, and a `gibbs_sites` list. It is marked EXPERIMENTAL but has been stable for several releases.

```
import numpyro
import numpyro.distributions as dist
from numpyro.infer import MCMC, NUTS, HMCGibbs


def gibbs_fn(rng_key, gibbs_sites, hmc_sites):
    params = hmc_sites  # current static params (constrained space)
    x_draw = dk_simulation_smoother(params, gibbs_sites['xi'], y_obs, rng_key)
    resid = compute_residuals(params, x_draw, y_obs)
    nu = params['nu']  # pooled per channel type
    shape = (nu + 1.0) / 2.0
    rate = (nu + (resid / sigma_of(params)) ** 2) / 2.0
    xi_new = dist.InverseGamma(shape, rate).sample(rng_key)
    return {'xi': xi_new}


inner = NUTS(model, target_accept_prob=0.9, max_tree_depth=8, dense_mass=False)
kernel = HMCGibbs(inner, gibbs_fn=gibbs_fn, gibbs_sites=['xi'])
mcmc = MCMC(kernel, num_warmup=800, num_samples=1500, num_chains=2,
            chain_method='parallel')
```

**Non-centered pooling stays as-is** via `numpyro.infer.reparam.LocScaleReparam(centered=0)` (or `TransformReparam`) applied to the tree-structured loadings/persistences/scales — this is the standard, correct device (per NumPyro's own funnel/eight-schools guidance) and interacts well with the warm start.

**Pathfinder / Laplace warm start.** Use NumPyro's `AutoLaplaceApproximation` (or a Pathfinder implementation over the model's `potential_fn`) to get a MAP-region init and an inverse-covariance estimate; pass the init via `init_params` and the metric via the NUTS `inverse_mass_matrix`, then run a short warmup to refine the step size.

### Verification plan (§9), instantiated

1. **Parameter recovery on synthetic data.** Simulate from the generative model with known `(φ_f, λ_i, φ_{u,i}, σ's, seasonal amplitudes, ν's)`; confirm the hybrid recovers them within Monte Carlo error. Sørensen's Online-Resource figures show all three samplers targeting the same posterior means (low absolute bias) — replicate that check for your hybrid vs your existing RB-NUTS.
2. **Simulated-coverage gate (the single most important test).** On simulated data, confirm nominal 50/80/95% coverage of both the latent path and the target's predictive density. This is the gate that separates a correct posterior approximation from an overconfident shortcut; do not ship any speed change (lower `target_accept`, float32, `K` reduction, FFBS-Gibbs) that fails it.
3. **Agreement with RB-NUTS on a real fit.** CRPS, PIT uniformity, and 50/80/95 coverage of the aggregate predictive density should match your current fit within Monte Carlo error. Because the hybrid targets the identical posterior, any shift beyond MC error signals an implementation bug (most likely in the partially-collapsed ordering).
4. **Standard diagnostics.** R-hat ≤ 1.01 (bulk and tail), adequate bulk- and tail-ESS, no divergences (or a defensible few), reasonable BFMI. The marginalized sampler should clear R-hat < 1.01 broadly, matching Sørensen's ~100% pass rate; if you see the ~40–60% pass rate characteristic of Metropolis-within-Gibbs, that is a signal you have drifted toward a poorly-mixing block update.

### Honest failure modes and how to detect each

- **Partially-collapsed ordering breaks stationarity.** If the auxiliary Gibbs step conditions on a quantity that a later step marginalizes, the chain can converge to the wrong stationary distribution (van Dyk & Jiao 2013). *Detect:* the synthetic parameter-recovery and coverage gates will show systematic bias; also cross-check the hybrid's posterior means against your existing RB-NUTS on a fixed dataset.
- **float32 destabilizes the gradient.** Reduced precision without the square-root/Joseph form produces non-PSD covariances and NaN/exploding gradients. *Detect:* divergence spikes, NaN log-likelihoods, filter covariance eigenvalues going negative — add an assertion on `S`'s Cholesky.
- **Auxiliary block still mixes slowly if `ν` is small.** Very heavy tails (`ν` near its lower prior bound) make `ξ_obs` highly variable and correlated with the residuals. *Detect:* low tail-ESS on `ν` and on tail quantiles of the predictive density; mitigate by keeping `ν` in NUTS (not Gibbs) and only Gibbs-drawing `ξ`.
- **Memory blow-up when batched across vintages.** A `T×d` trajectory per draw × chains × origins is small individually, but `chain_method='vectorized'` and multi-origin `vmap` materialize them simultaneously. At `T≈150`, `d≈18` this is well within budget, but confirm peak RSS after batching; if it approaches 15–20 GB, drop to `chain_method='parallel'` with fewer chains or thin the stored trajectories.
- **Fewer chains weakens convergence diagnosis.** Going to 2 chains for ESS/sec reduces R-hat's power to catch multimodality. *Detect:* run an occasional 4-chain audit fit; compare per-chain posterior means.
- **Over-aggressive `target_accept`/tree-depth cuts.** Silent under-coverage. *Detect:* PIT histograms developing ∪- or ∩-shapes and 80/95% interval coverage falling below nominal on the held-out set.

## Recommendations

**Stage 1 — do now (low risk, high payoff):** Add a Pathfinder/Laplace warm start (init + mass matrix); cut warmup to ~800–1000; set `numpyro.enable_x64()` explicitly at startup; benchmark 2 long parallel chains vs 4. Gate on R-hat/ESS. Expected ~2–3× wall-clock for zero calibration cost.

**Stage 2 — the structural change:** Move `{ξ_obs}` into a Gibbs step via `HMCGibbs` with the partially-collapsed Durbin–Koopman-conditioned inverse-gamma draw; drop `target_accept` to 0.9 and cap tree depth at ~8. Run the full §9 verification (especially the simulated-coverage gate) before adopting. Expected additional ~2–4× ESS/sec.

**Stage 3 — inner-loop and model economy (compose as needed):** Switch the Kalman update to Koopman–Durbin sequential scalar observation processing (also cleans up masking); test dropping seasonal harmonics `K=6→4` against held-out CRPS. Optionally prototype a QR square-root filter as the precondition for a *gated* float32 experiment.

**Stage 4 — only if Stages 1–3 miss the target:** Prototype Regime B (FFBS-Gibbs with ASIS interweaving and Gaussian/inverse-gamma priors) as a *separate* sampler, and adopt it only if it both matches RB-NUTS on the coverage gate and beats the hybrid on ESS/sec on your hardware. Expect meaningful implementation effort (two parameterizations, interweaving) and a calibration argument for the flattened priors.

**Benchmarks that would change the recommendation:** (a) if the simulated-coverage gate shows FFBS-Gibbs matching RB-NUTS *and* it wins ESS/sec, promote Regime B; (b) if profiling shows the Kalman pass (not warmup or the auxiliary block) is the bottleneck, prioritize Stage 3 over Stage 2; (c) if peak memory approaches the budget under batching, abandon `chain_method='vectorized'`.

## Caveats

- The headline efficiency multipliers (8–19× for marginalized NUTS over Gibbs; ~5× for the hybrid's auxiliary block) come from *related but not identical* models (dynamic structural equation models, factor SV models). They are strong directional evidence, not a promise for your exact specification — which is why every stage is gated on your own §9 checks.
- All ESS/sec estimates here are order-of-magnitude planning figures. Actual gains on a specific 4-core box depend heavily on XLA compilation, host–device traffic, and cache behavior; profile with the compile/warmup/sample phases separated.
- The `HMCGibbs` interface is officially marked EXPERIMENTAL in NumPyro; it has been stable across recent releases (through 0.20) but pin your version and re-run the verification suite on any upgrade.
- The parallel-in-time (associative-scan) and float32 levers are genuinely hardware/precision-contingent; the recommendation to skip associative-scan is specific to 4 CPU cores and would flip on a GPU.
- Sørensen's specific "Gibbs can win with ≥2–3 indicators per latent" finding means the pure-NUTS-vs-Gibbs margin is genuinely close for a many-channel model like yours; the recommendation leans on the hybrid precisely because it is robust to which side of that margin you land on.