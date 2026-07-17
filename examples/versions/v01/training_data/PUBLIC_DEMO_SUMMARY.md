# Public Synthetic Demo Summary

This report is generated from eight fully invented records in
`examples/versions/v01/training_data/input.synthetic.jsonl`. It contains no private video,
transcript, evidence snapshot, or human annotation content.

## Reproduction identity

- dataset version: `truthfulness_v0.1.1_public_synthetic_demo`
- input records: 8
- input SHA-256: `53180f3f09f22a631664bd32087b49b35115e2f8e63cb5f2544721be30c1d11f`
- pack status: `pass`
- formal training: `false`
- RLHF completed: `false`

## Derived outputs

| artifact | records |
| --- | ---: |
| quality records | 8 |
| claim-triage SFT examples | 8 |
| controlled synthetic examples | 6 |
| pending preference pairs | 4 |

## Task gates

| task | pass | quarantine | reject |
| --- | ---: | ---: | ---: |
| claim triage SFT | 8 | 0 | 0 |
| evidence-grounded SFT | 7 | 1 | 0 |
| public release | 8 | 0 | 0 |
| synthetic parent | 6 | 2 | 0 |
| truthfulness evaluation | 1 | 7 | 0 |

The demo had no hard errors, duplicate clusters, or missing evidence metadata.
The four preference pairs remain pending and therefore do not constitute a
completed preference dataset or an RLHF result.
