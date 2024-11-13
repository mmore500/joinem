from . import __version__ as joinem_version
from ._dataframe_cli import dataframe_cli


def main() -> None:
    dataframe_cli(
        description=(
            "CLI for fast, flexbile concatenation of tabular data "
            "using Polars."
        ),
        module="joinem",
        version=joinem_version,
        input_dataframe_op=lambda x: x,
        output_dataframe_op=lambda x: x,
    )


if __name__ == "__main__":
    main()
