# openIMIS Backend Monitoring and Evaluation module

This module manages indicators, indicator values, monitoring submissions and
automatic calculations for openIMIS social-protection programs.

## Runtime dependencies

- `openimis-be-core`
- `openimis-be-grievance_social_protection`
- `openimis-be-individual`
- `openimis-be-location`
- `openimis-be-payroll`
- `openimis-be-social_protection`

The common openIMIS Django dependencies are declared in `setup.py`.

## Integration points

- GraphQL queries and mutations are exposed from
  `monitoring_evaluation.schema`.
- Scheduled recalculation is exposed as
  `monitoring_evaluation.tasks.run_recalculate_indicators_job`.
- Manual recalculation is available through the `recalc_indicators` management
  command and the Django admin action.
