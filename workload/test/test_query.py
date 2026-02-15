#!/usr/bin/env python3
"""
TPC-H Vector Query Benchmark for SeekDB
Reads SQL queries from tpch_queries.sql and executes them with vector embeddings.
"""

import os
import re
import time
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import numpy as np
import mysql.connector
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

# Configuration
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'db_name': 'tpch01',
    'vector_file': '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/queries.fbin',
    'sql_file': '/data/dzh/seekdb/workload/tpch_queries.sql',
    'num_runs': 10,
    'distance_threshold': 0.925,
    'result_limit': 10,
    'vector_limit': 10,  # Set to an int to limit vectors tested
    'show_plan': False,
    'show_results': False,
    'output_file': None,  # Set to a path to save results to JSON
}

console = Console()


class VectorReader:
    """Read vector embeddings from .fbin files."""
    
    @staticmethod
    def read_fbin(filename: str, start_idx: int = 0, chunk_size: Optional[int] = None) -> np.ndarray:
        """Read *.fbin file that contains float32 vectors."""
        with open(filename, "rb") as f:
            nvecs, dim = np.fromfile(f, count=2, dtype=np.int32)
            # Seek to the correct position: 8 bytes (header) + start_idx * dim * 4 bytes (float32)
            f.seek(8 + start_idx * dim * 4)
            # Read all available data
            arr = np.fromfile(f, dtype=np.float32)
            # Calculate actual number of vectors that can be formed
            actual_nvecs = len(arr) // dim
            if chunk_size is not None:
                actual_nvecs = min(actual_nvecs, chunk_size)
            n_fetch = min(actual_nvecs, nvecs - start_idx) if chunk_size is None else min(actual_nvecs, chunk_size)
            console.print(f"[dim]Loaded vectors: {n_fetch}/{nvecs} vectors, dimension: {dim}[/dim]")
            # Only keep the vectors we can fully form
            arr = arr[:n_fetch * dim]
        return arr.reshape(-1, dim)
    
    @staticmethod
    def vector_to_string(vector: np.ndarray) -> str:
        """Convert numpy array to SeekDB vector string format: '[1.0,2.0,3.0]'"""
        return '[' + ','.join(str(float(x)) for x in vector) + ']'


class QueryLoader:
    """Load and parse SQL queries from file."""
    
    @staticmethod
    def load_queries(sql_file: str) -> Dict[str, str]:
        """Load queries from SQL file, separated by --Q{number} markers."""
        queries = {}
        current_query = None
        current_sql = []
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            for line in f:
                # Check for query marker
                match = re.match(r'^--Q(\d+)', line.strip())
                if match:
                    # Save previous query if exists
                    if current_query and current_sql:
                        queries[current_query] = '\n'.join(current_sql).strip()
                    # Start new query
                    current_query = f"Q{match.group(1)}"
                    current_sql = []
                elif current_query:
                    current_sql.append(line)
        
        # Save last query
        if current_query and current_sql:
            queries[current_query] = '\n'.join(current_sql).strip()
        
        return queries
    
    @staticmethod
    def prepare_query(query_template: str, vector_str: str, limit: int = 10) -> str:
        """Replace placeholders in query template."""
        query = query_template.replace('{VECTOR}', vector_str)
        query = query.replace('{LIMIT}', str(limit))
        return query


class QueryExecutor:
    """Execute queries and collect statistics."""
    
    def __init__(self, connection):
        self.conn = connection
        self.cur = connection.cursor()
        self.conn.autocommit = True
    
    def execute_query(self, query: str, show_plan: bool = False, show_results: bool = False) -> Tuple[List, float]:
        """Execute a query and return results with execution time."""
        # Clear any unread results from previous queries to avoid "Commands out of sync" error
        try:
            while self.cur.nextset():
                self.cur.fetchall()
        except:
            pass
        
        # Remove EXPLAIN for actual execution
        query_exec = query.replace('EXPLAIN', '').strip()
        
        # Show plan if requested
        if show_plan:
            try:
                self.cur.execute(query)
                plan = self.cur.fetchall()
                # Consume all result sets from EXPLAIN query
                while self.cur.nextset():
                    self.cur.fetchall()
                console.print("[cyan]Query Plan:[/cyan]")
                for row in plan[:10]:  # Limit plan output
                    console.print(f"  {row[0] if isinstance(row, (list, tuple)) and len(row) > 0 else row}")
                console.print()
            except Exception as e:
                console.print(f"[yellow]Warning: Could not show plan: {e}[/yellow]")
                # Clear any partial results
                try:
                    while self.cur.nextset():
                        self.cur.fetchall()
                except:
                    pass
        
        # Execute and measure time
        start_time = time.time()
        try:
            self.cur.execute(query_exec)
            results = self.cur.fetchall()
            
            # Consume all remaining result sets (important for multi-statement queries)
            while self.cur.nextset():
                additional_results = self.cur.fetchall()
                # For multi-statement queries, we typically only care about the last SELECT result
                # But we still need to consume all intermediate results
                if additional_results:
                    results = additional_results
        except Exception as e:
            # Clear any partial results on error
            try:
                while self.cur.nextset():
                    self.cur.fetchall()
            except:
                pass
            raise
        
        end_time = time.time()
        exec_time_ms = (end_time - start_time) * 1000
        
        # Show results if requested
        if show_results:
            console.print(f"[green]Results ({len(results)} rows):[/green]")
            for i, row in enumerate(results[:5], 1):
                console.print(f"  {i}. {row[0] if len(row) > 0 else row}")
            if len(results) > 5:
                console.print(f"  ... ({len(results)} total rows)")
            console.print()
        
        return results, exec_time_ms
    
    def run_benchmark(self, query: str, num_runs: int, show_plan: bool = False, 
                     show_results: bool = False) -> Dict:
        """Run a query multiple times and collect statistics."""
        times = []
        results_count = 0
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Running {num_runs} iterations...", total=num_runs)
            
            for i in range(num_runs):
                results, exec_time = self.execute_query(
                    query, 
                    show_plan=(show_plan and i == 0),  # Only show plan for first run
                    show_results=(show_results and i == 0)  # Only show results for first run
                )
                times.append(exec_time)
                if i == 0:
                    results_count = len(results)
                progress.update(task, advance=1)
        
        # Calculate statistics
        stats = {
            'times': times,
            'count': len(times),
            'mean': np.mean(times),
            'std': np.std(times),
            'min': np.min(times),
            'max': np.max(times),
            'first': times[0] if times else 0.0,
            'results_count': results_count,
        }
        
        # Calculate trimmed mean (remove min and max)
        if len(times) > 2:
            trimmed = sorted(times)[1:-1]
            stats['trimmed_mean'] = np.mean(trimmed)
            stats['trimmed_std'] = np.std(trimmed)
        else:
            stats['trimmed_mean'] = stats['mean']
            stats['trimmed_std'] = stats['std']
        
        return stats
    
    def run_vector_latencies(self, query_template: str, vectors: np.ndarray, limit: int,
                             show_plan: bool = False, show_results: bool = False) -> Dict:
        """Run a query once per vector and collect latency statistics."""
        times = []
        results_count = 0
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Running vector latency checks...", total=len(vectors))
            
            for i, vector in enumerate(vectors):
                query = QueryLoader.prepare_query(
                    query_template,
                    VectorReader.vector_to_string(vector),
                    limit
                )
                # print(query)
                results, exec_time = self.execute_query(
                    query,
                    show_plan=(show_plan and i == 0),
                    show_results=(show_results and i == 0)
                )
                if i == 0:
                    results_count = len(results)
                times.append(exec_time)
                progress.update(task, advance=1)
        
        if not times:
            return {
                'times': [],
                'count': 0,
                'mean': 0.0,
                'std': 0.0,
                'min': 0.0,
                'max': 0.0,
                'p50': 0.0,
                'p95': 0.0,
                'results_count': 0,
            }
        
        return {
            'times': times,
            'count': len(times),
            'mean': np.mean(times),
            'std': np.std(times),
            'min': np.min(times),
            'max': np.max(times),
            'p50': np.percentile(times, 50),
            'p95': np.percentile(times, 95),
            'results_count': results_count,
        }


class ResultReporter:
    """Format and display benchmark results."""
    
    @staticmethod
    def print_statistics(query_name: str, stats: Dict):
        """Print formatted statistics for a query."""
        table = Table(title=f"Query {query_name} Statistics", show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="green")
        
        table.add_row("Samples", str(stats['count']))
        table.add_row("Results", str(stats['results_count']))
        table.add_row("Mean (ms)", f"{stats['mean']:.2f}")
        table.add_row("Std Dev (ms)", f"{stats['std']:.2f}")
        table.add_row("Min (ms)", f"{stats['min']:.2f}")
        table.add_row("Max (ms)", f"{stats['max']:.2f}")
        table.add_row("P50 (ms)", f"{stats['p50']:.2f}")
        table.add_row("P95 (ms)", f"{stats['p95']:.2f}")
        
        console.print(table)
        console.print()
    
    @staticmethod
    def print_summary(all_results: Dict[str, Dict]):
        """Print summary table of all queries."""
        table = Table(title="Benchmark Summary", show_header=True, header_style="bold blue")
        table.add_column("Query", style="cyan", no_wrap=True)
        table.add_column("Samples", style="green", justify="right")
        table.add_column("Mean (ms)", style="yellow", justify="right")
        table.add_column("Std (ms)", style="yellow", justify="right")
        table.add_column("Min (ms)", style="green", justify="right")
        table.add_column("Max (ms)", style="red", justify="right")
        table.add_column("P95 (ms)", style="red", justify="right")
        
        for query_name, stats in sorted(all_results.items()):
            table.add_row(
                query_name,
                str(stats['count']),
                f"{stats['mean']:.2f}",
                f"{stats['std']:.2f}",
                f"{stats['min']:.2f}",
                f"{stats['max']:.2f}",
                f"{stats['p95']:.2f}"
            )
        
        console.print(table)
        console.print()
    
    @staticmethod
    def save_results(all_results: Dict[str, Dict], output_file: str):
        """Save results to JSON file."""
        output_data = {
            'timestamp': datetime.now().isoformat(),
            'config': CONFIG,
            'results': all_results
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        console.print(f"[green]Results saved to {output_file}[/green]")


def main():
    """Main execution function."""
    console.print(Panel.fit(
        "[bold blue]TPC-H Vector Query Benchmark for SeekDB[/bold blue]",
        border_style="blue"
    ))
    console.print()
    
    # Load vector embeddings
    console.print("[bold]Loading vector embeddings...[/bold]")
    if not Path(CONFIG['vector_file']).exists():
        console.print(f"[red]✗ Vector file not found: {CONFIG['vector_file']}[/red]")
        return
    vector_file_size = os.path.getsize(CONFIG['vector_file'])
    vector_load_start = time.time()
    vectors = VectorReader.read_fbin(CONFIG['vector_file'])
    vector_load_ms = (time.time() - vector_load_start) * 1000
    vector_limit = CONFIG.get('vector_limit')
    if vector_limit is not None:
        vectors = vectors[:vector_limit]
    console.print(
        f"[green]✓ Loaded {len(vectors)} vectors "
        f"(file size: {vector_file_size} bytes, load time: {vector_load_ms:.2f} ms), "
        "using all vectors for latency checks[/green]"
    )
    console.print()
    
    # Load SQL queries
    console.print("[bold]Loading SQL queries...[/bold]")
    queries = QueryLoader.load_queries(CONFIG['sql_file'])
    console.print(f"[green]✓ Loaded {len(queries)} queries: {', '.join(sorted(queries.keys()))}[/green]")
    console.print()
    
    # Connect to database
    console.print("[bold]Connecting to database...[/bold]")
    try:
        conn = mysql.connector.connect(
            host=CONFIG['db_host'],
            port=CONFIG['db_port'],
            user=CONFIG['db_user'],
            database=CONFIG['db_name']
        )
        conn.autocommit = True
        console.print(f"[green]✓ Connected to {CONFIG['db_name']}@{CONFIG['db_host']}:{CONFIG['db_port']}[/green]")
        console.print()
    except Exception as e:
        console.print(f"[red]✗ Failed to connect: {e}[/red]")
        return
    
    # Execute queries
    executor = QueryExecutor(conn)
    all_results = {}
    
    console.print(Panel.fit(
        f"[bold]Running {len(queries)} queries across {len(vectors)} vectors[/bold]",
        border_style="yellow"
    ))
    console.print()
    
    for query_name in sorted(queries.keys()):
        console.print(f"[bold cyan]Executing {query_name}...[/bold cyan]")
        
        # Prepare query
        query_template = queries[query_name]
        stats = executor.run_vector_latencies(
            query_template,
            vectors,
            CONFIG['result_limit'],
            show_plan=CONFIG['show_plan'],
            show_results=CONFIG['show_results']
        )
        
        all_results[query_name] = stats
        ResultReporter.print_statistics(query_name, stats)
    
    # Print summary
    ResultReporter.print_summary(all_results)
    
    # Save results if output file is specified
    if CONFIG['output_file']:
        ResultReporter.save_results(all_results, CONFIG['output_file'])
    
    # Close connection
    conn.close()
    console.print("[green]✓ Benchmark completed![/green]")


if __name__ == '__main__':
    main()
