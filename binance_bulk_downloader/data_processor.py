import pandas as pd
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
)

console = Console()


class BinanceDataProcessor:
    """
    A class for processing Binance data.
    Merges daily/monthly CSV files into a single consolidated file.
    """

    def __init__(self, input_dir: str, output_dir: Optional[str] = None):
        """
        Initialize the processor

        Args:
            input_dir (str): Input directory path
            output_dir (str, optional): Output directory path. If None, saves in the same location as input
        """
        self.input_path = Path(input_dir)
        self.output_path = Path(output_dir) if output_dir else self.input_path
        if output_dir:
            self.output_path.mkdir(parents=True, exist_ok=True)

    def merge_files(self, progress: Optional[Progress] = None) -> Optional[Path]:
        """
        Merge CSV files in the specified directory into a single file.
        Skips time-limited contracts (e.g. BTCUSDT_210326) and SETTLED contracts.
        After successful merge, original files are deleted.

        Args:
            progress: Optional Progress instance for updates

        Returns:
            Path: Path to the output file, or None if skipped
        """
        # Get list of CSV files
        csv_files = sorted(self.input_path.glob("*.csv"))
        if not csv_files:
            console.print(
                f"[yellow]Warning:[/yellow] No CSV files found in {self.input_path}"
            )
            return None

        # Define possible time column names
        time_columns = ["open_time", "time", "timestamp"]

        # Skip if this is a time-limited or SETTLED contract
        symbol = self.input_path.parts[-2]
        if "_" in symbol or "SETTLED" in symbol:
            console.print(
                f"[blue]Info:[/blue] Skipping time-limited or SETTLED contract: {symbol}"
            )
            return None

        # Filter out the all.csv file
        csv_files = [f for f in csv_files if not f.name.endswith("-all.csv")]
        if not csv_files:
            console.print(
                f"[yellow]Warning:[/yellow] No data files found in {self.input_path}"
            )
            return None

        # Get columns from the latest file
        latest_df = pd.read_csv(csv_files[-1])
        columns = latest_df.columns.tolist()

        # Read and combine all CSV files
        dfs = []
        for file in csv_files:
            df = pd.read_csv(file)

            # Handle different time column names
            time_col = None
            for col in time_columns:
                if col in df.columns:
                    time_col = col
                    break

            if time_col and time_col != "open_time":
                # Rename time column to open_time if needed
                df = df.rename(columns={time_col: "open_time"})

            # Ensure all dataframes have the same columns
            df = df.reindex(columns=columns)
            dfs.append(df)

        # Concatenate all dataframes
        merged_df = pd.concat(dfs, axis=0, ignore_index=True)

        try:
            # Sort by open_time
            merged_df = merged_df.sort_values("open_time")
            # Remove duplicates
            merged_df = merged_df.drop_duplicates(subset=["open_time"])
        except KeyError:
            console.print(
                f"[yellow]Warning:[/yellow] Could not sort by open_time for {self.input_path}"
            )
            # Try alternative time columns if open_time is not available
            for col in time_columns[1:]:
                if col in merged_df.columns:
                    merged_df = merged_df.sort_values(col)
                    merged_df = merged_df.drop_duplicates(subset=[col])
                    break

        # Get output filename
        parts = self.input_path.parts
        symbol = parts[-2] if len(parts) >= 2 else ""
        interval = parts[-1] if len(parts) >= 3 else ""

        filename = f"{symbol}-{interval}-all.csv" if interval else f"{symbol}-all.csv"
        output_file = self.output_path / filename

        # Save merged file without index
        merged_df.to_csv(output_file, index=False, float_format="%.8f")

        # Delete original files after successful merge
        for file in csv_files:
            file.unlink()

        return output_file

    @classmethod
    def consolidate_csv_files(cls, base_path: str, data_frequency: str = "1d") -> None:
        """
        Consolidate all CSV files in each symbol directory into single files.
        Original files are deleted after successful merge.

        Args:
            base_path: Base directory path containing symbol directories
            data_frequency: Data frequency/interval (e.g., "1d", "4h", "1m")
        """
        symbols = [d for d in Path(base_path).iterdir() if d.is_dir()]

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            overall_task = progress.add_task(
                f"[cyan]Processing all symbols ({data_frequency})...",
                total=len(symbols),
            )

            for symbol_path in symbols:
                symbol = symbol_path.name
                try:
                    processor = cls(str(symbol_path / data_frequency))
                    output_file = processor.merge_files()
                    if output_file:
                        console.print(f"[green]✓[/green] {symbol} -> {output_file}")
                    else:
                        console.print(f"[blue]•[/blue] {symbol} (skipped)")
                except Exception as e:
                    console.print(f"[red]✗[/red] {symbol}: {str(e)}")

                progress.advance(overall_task)
