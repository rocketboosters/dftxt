"""dftxt package root that exposes public interface for standard use cases."""
from ._io import read
from ._io import read_to_pandas
from ._io import read_to_polars
from ._io import reads
from ._io import reads_to_pandas
from ._io import reads_to_polars
from ._io import write
from ._io import writes

__all__ = [
    "read",
    "read_to_pandas",
    "read_to_polars",
    "reads",
    "reads_to_pandas",
    "reads_to_polars",
    "write",
    "writes",
]
