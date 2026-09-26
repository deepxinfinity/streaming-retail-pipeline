# ADR-001: Price elasticity is not identifiable from the forecasting model

**Status:** accepted
**Date:** 2026-09-27

## Context

The simulator draws a price elasticity per SKU and writes it to `sim/params.json`,
which is gitignored and never ingested. The idea was that `ml/elasticity.py` could
then recover those numbers from the trained demand model and check them against the
real ones, which is something you can almost never do.

It does not work. Measured over 30 categories:

| | |
|---|---|
| true elasticity (planted) | -1.01 to -2.20 |
| recovered (log-log slope) | -0.17 to -0.38 |
| MAE | **1.40** |

The estimate is off by more than the magnitude of the thing being estimated.

The cause is visible in the feature importances of the champion model:

| feature | share of gain |
|---|---|
| units_ma_28 | 78.5% |
| units_lag_14 | 6.1% |
| discount_depth | 5.0% |
| units_lag_7 | 4.3% |
| **rel_price** | **0.9%** |
| promo_flag | 0.5% |

The lag and moving-average features carry about 89% of the signal. That makes sense:
if a SKU's price went up two weeks ago, its recent units are already lower, so
`units_ma_28` has the price effect baked into it. The model never needs to learn a
direct price response, and it doesn't.

`predict_price_grid` then sweeps `rel_price` while holding the lags fixed. That is
not a counterfactual that can happen: in reality a price change moves future demand,
which moves the lags. So the measured slope is close to flat by construction.

This is the usual reason a forecasting model cannot be read as a causal model. Good
autoregressive features are exactly what destroys identification of the price
coefficient.

## Options considered

1. **Tune the existing model.** More iterations, stronger monotone constraint,
   upweighting price. Does not address the cause: the lags will still explain the
   variance first.
2. **Drop the lag features and re-fit.** Tested. MAE improves from 1.40 to 1.08 and
   recovered magnitudes roughly double, but it is still not a usable estimate, and
   the model loses most of its forecasting accuracy.
3. **Two models for two questions.** Keep the current model for forecasting. Fit a
   separate model for elasticity: no lags, SKU fixed effects, and price variation
   measured within a SKU over time rather than across SKUs.
4. **Fit the log-log regression directly** rather than reading a slope out of a GBM.
   A Poisson GLM on `log(units) ~ log(price) + sku + dow + promo` gets the
   coefficient directly instead of inferring it from partial dependence.

## Decision

Keep the forecasting model as it is, and **stop claiming the elasticity is
recovered**. `ml/elasticity.py` stays in the repo as the measurement that showed the
problem, with its real MAE reported.

Options 3 and 4 are the actual fix and are not done yet. They are a separate piece
of modelling work, not a parameter change.

## Consequences

The forecasting result stands on its own and is unaffected: WMAPE 0.373, stable
across six rolling-origin folds (0.376 to 0.380), 43% better than a naive lag-7
baseline and about 7% better than a 28-day moving average, with bias near zero.

What we lose is the headline that the model rediscovers the planted elasticities. It
does not, and the README no longer says so.

What we gain is worth more: the ground truth is what made this findable at all. On
real data the elasticity would have been wrong by the same margin and there would
have been no way to know. That is the argument for building a simulator with known
parameters in the first place.

If this were a real pricing system, shipping the optimizer on top of this model would
be the actual risk. `ml/optimize.py` reads a price response out of a model that has
barely learned one, so its recommendations are close to "leave the price alone" and
its expected-margin deltas should not be trusted. That needs option 3 or 4 before it
means anything.
