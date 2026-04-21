# Foundation Platform Python SDK

Python SDK for interacting with the [Foundation Platform](https://www.foundationplatform.com) API. Provides a simple, Pythonic interface for accessing reinsurance and insurance program data, layers, loss analysis, custom fields, and reference data.

## Installation

```bash
pip install git+https://github.com/Treefrog-consulting/foundation-python-sdk.git
```

Requires Python 3.12+ and Foundation Platform v38 or later for loss export features.

## Loss Set Upload (v0.8+)

Upload YELT/ELT loss sets directly to Foundation from pandas or polars DataFrames. See `example_loss_upload.py` for a complete example.

```python
import foundation
client = foundation.get_client(...)
program = client.get_program(2610)

result = (
    program.create_loss_group("2026 UW Portfolio")
    .add_yelt(name="RMS EQ", data=yelt_df, currency_code="USD",
              event_set_id=5, perspective_id="Gross", variant_id=1,
              vendor_id="RMS", sim_years=10000)
    .add_elt(name="AIR EQ", data=elt_df, currency_code="USD",
             event_set_id=5, perspective_id="Default", variant_id=1,
             vendor_id="AIR", sim_years=10000)
    .upload(poll_interval=10, timeout=600)
)
print(f"{len(result.successes)} ok, {len(result.errors)} failed")
```

## Documentation

Full documentation is available at [docs.foundationplatform.com](https://docs.foundationplatform.com).
