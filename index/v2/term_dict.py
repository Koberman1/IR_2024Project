import pickle
from collections import Counter
from pathlib import Path
from typing import Dict, Optional

from index.v2.transform_dataset import read_json_records
from utils.file_utils import _open


class TermDict:
    def __init__(self, index: Dict[str, int]):
        self.index = index

    @staticmethod
    def create(parquet_dir: Path) -> "TermDict":
        counts = Counter()
        for record in read_json_records(parquet_dir):
            terms = set(record['tokens'])
            doc_counts = Counter(terms)
            counts.update(doc_counts)

        td = TermDict(counts)

        with _open("../../term.index", 'wb', None) as f:
            pickle.dump(td, f)

        return td

    @staticmethod
    def open(path: Path, bucket_name: Optional[str] = None) -> "TermDict":
        with _open(str(path.absolute()), 'rb', bucket_name) as f:
            return pickle.load(f)


if __name__ == '__main__':
    td = TermDict.open(Path("../../term.index"))

    for k, c in list(td.index.items()):
        if c < 50:
            del td.index[k]

    with _open("../../term.index", 'wb', None) as f:
        pickle.dump(td, f)

    # print()
    # TermDict.create(Path("/home/vadim/Data/wikidump.tokenized.jsonl"))
