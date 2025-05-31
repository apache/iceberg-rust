from __future__ import annotations

from typing import Any

class IcebergDataFusionTable:
    def __init__(
        self,
        identifier: list[str],
        metadata_location: str,
        storage_options: dict[str, str] | None,
    ) -> None: ...
    def __datafusion_table_provider__(self) -> Any:
        """Return the DataFusion table provider PyCapsule interface.

        To support DataFusion features such as push down filtering, this function will return a PyCapsule
        interface that conforms to the FFI Table Provider required by DataFusion. From an end user perspective
        you should not need to call this function directly. Instead you can use ``register_table_provider`` in
        the DataFusion SessionContext.

        Returns:
            A PyCapsule DataFusion TableProvider interface.

        Example:
            ```python
            from pyiceberg_core.datafusion import IcebergDataFusionTable
            from datafusion import SessionContext

            ice_tbl = IcebergDataFusionTable()
            ctx = SessionContext()
            ctx.register_table_provider("test", ice_tbl)
            ctx.table("test").show()
            ```
            Results in
            ```
            DataFrame()
            +----+----+----+
            | c3 | c1 | c2 |
            +----+----+----+
            | 4  | 6  | a  |
            | 6  | 5  | b  |
            | 5  | 4  | c  |
            +----+----+----+
            ```
        """
