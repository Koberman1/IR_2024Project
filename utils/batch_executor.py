from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Callable, List, Dict

from tokenizer import tokenize
from utils.parquet_utils import for_each_document_in_dataset


def execute_in_batches(batch_size: int, max_workers: int, data_provider, data_processor: Callable):
        results = []
        with ProcessPoolExecutor(max_workers=max_workers) as pp:
            batch = []
            for record in data_provider:
                batch.append(record)
                if len(batch) >= batch_size:
                    future = pp.submit(data_processor, batch)
                    batch = []
                    results.append(future)
                    if len(results) >= max_workers:
                        future = results.pop(0)
                        for item in future.result():
                            yield item
            if len(batch) > 0:
                future = pp.submit(data_processor, batch)
                results.append(future)
            for future in results:
                for item in future.result():
                    yield item


def _tokenize(dataset: List[Dict]) -> List[Dict]:
    return [{
        'id': record['id'],
        'tokens': tokenize(record['text'])
    } for record in dataset]


def tokenize_in_batches(data_dir: Path, batch_size: int, max_workers: int):
    supplier = for_each_document_in_dataset(data_dir, columns=["id", "text"], batch_size=batch_size)
    for item in execute_in_batches(batch_size, max_workers, supplier, _tokenize):
        yield item