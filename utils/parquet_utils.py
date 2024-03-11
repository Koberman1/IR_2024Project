from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
from typing import List, Iterator, Dict

from tqdm import tqdm
import pyarrow.parquet as pa



def parquet_file_iterator(
        file_name: str,
        columns: List[str] = None,
        batch_size: int = 64,
        descr: str = f"Reading file..."
) -> Iterator[Dict]:
    parquet_file = pa.ParquetFile(file_name)
    n_batches = ((parquet_file.metadata.num_rows - 1) // batch_size) + 1
    for record_batch in tqdm(parquet_file.iter_batches(batch_size=batch_size, columns=columns), total=n_batches,
                             desc=descr):
        for d in record_batch.to_pylist():
            yield d


def for_each_document_in_dataset(dataset_dir: Path, columns: List[str] = None, batch_size: int = 64) -> Iterator[Dict]:
    files = [f for f in dataset_dir.iterdir() if f.name.endswith(".parquet")]
    for i, f in enumerate(files):
        print(f"Processing file [{i + 1}/{len(files)}]")
        for r in parquet_file_iterator(str(f.absolute()), columns, batch_size, f"Reading file : {f.name}"):
            yield r

