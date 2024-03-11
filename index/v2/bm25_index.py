import math
import pickle
import time
from collections import defaultdict, Counter
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from tqdm import tqdm

from index.v2.inverted_index_gcp import InvertedIndex
from index.v2.term_dict import TermDict
from index.v2.tokenizer import tokenize
from index.v2.transform_dataset import read_json_records
from utils.file_utils import _open

NUM_BUCKETS = 1024


def _write_binary_record(writer, widx: int, doc_id: int, cnt: int):
    data = b''.join((widx.to_bytes(4, 'big'), doc_id.to_bytes(4, 'big'), cnt.to_bytes(2, 'big')))
    assert len(data) == 10
    writer.write(data)


def _read_binary_records(reader, inverse_index: Dict[int, str]) -> Dict[str, List[Tuple[int, int]]]:
    result = defaultdict(list)
    while True:
        data = reader.read(10)
        if not data:
            break
        widx = int.from_bytes(data[:4], 'big')
        w = inverse_index[widx]
        doc_id = int.from_bytes(data[4:8], 'big')
        cnt = int.from_bytes(data[8:10], 'big')
        result[w].append((doc_id, cnt))
    return result


class BM25Index:

    def __init__(self, index: InvertedIndex, posting_reader, k: float, b: float):
        self.index = index
        self.posting_reader = posting_reader
        self.b = b
        self.k = k
        self.N = len(self.index.dl)
        self.avgdl = sum(self.index.dl.values()) / self.N

    @staticmethod
    def create(parquet_dir: Path, term_dict: TermDict, limit: int):
        index = dict()
        for w, cnt in term_dict.index.items():
            if cnt >= limit:
                index[w] = len(index)
        i_index = {v: k for k, v in index.items()}
        chunk_dir = Path("index")
        chunk_dir.mkdir(parents=True, exist_ok=True)
        buckets_dir = chunk_dir / "buckets"
        buckets_dir.mkdir(parents=True, exist_ok=True)
        files = [_open(str((buckets_dir / f"{i:05d}.dat").absolute()), "wb") for i in range(NUM_BUCKETS)]
        ii = InvertedIndex()
        for record in read_json_records(parquet_dir):
            tokens = [x for x in record['tokens'] if x in index]
            doc_id = record['id']
            ii.dl[doc_id] = ii.dl.get(doc_id, 0) + len(tokens)
            w2cnt = Counter(tokens)
            ii.term_total.update(w2cnt)
            for w, cnt in w2cnt.items():
                ii.df[w] = ii.df.get(w, 0) + 1
                _write_binary_record(files[index[w] % NUM_BUCKETS], index[w], doc_id, cnt)
                # self._posting_list[w].append((doc_id, cnt))
        for file in files:
            file.close()
        with _open("../../posting.list", "wb") as writer:
            for i in tqdm(range(NUM_BUCKETS), total=NUM_BUCKETS, desc="Writing posting lists"):
                with _open(str((buckets_dir / f"{i:05d}.dat").absolute()), "rb") as reader:
                    postings = _read_binary_records(reader, i_index)
                for w, pl in postings.items():
                    ii.write_posting_list(writer, w, pl)
        ii.write_index(chunk_dir.absolute(), "index", None)

    @staticmethod
    def open(b: float, k: float, index_file: Path, posting_file: Path,
             bucket_name: Optional[str] = None) -> "BM25Index":
        with _open(str(index_file.absolute()), "rb", bucket_name) as fp:
            ii = pickle.load(fp)
        postings_reader = _open(str(posting_file.absolute()), "rb", bucket_name)
        return BM25Index(ii, postings_reader, k, b)

    def _list_postings(self, term: str) -> List[Tuple[int, int]]:
        if term not in self.index.posting_locs:
            return []
        return self.index.read_posting_list(self.posting_reader, term)

    def _idf(self, q: str) -> float:
        n_q = self.index.df[q]
        return math.log(((self.N - n_q + 0.5) / (n_q + 0.5)) + 1)

    def _score_term_doc(self, idf: float, f_q: int, d: int) -> float:
        num = f_q * (self.k + 1)
        denom = f_q + self.k * (1 - self.b + self.b * (d / self.avgdl))
        return idf * num / denom

    def query(self, text: str) -> List[Tuple[int, float]]:
        terms = [x for x in tokenize(text) if x in self.index.term_total]
        query = Counter(terms)
        data = {term: self._list_postings(term) for term in query}

        doc_ids = set(doc_id for pl in data.values() for doc_id, _ in pl)

        result = {doc_id: 0.0 for doc_id in doc_ids}
        for q, cnt in query.items():
            idf = self._idf(q)
            # print(len(data[q]))
            for doc_id, f_q in data[q]:
                result[doc_id] = result[doc_id] + self._score_term_doc(idf, f_q, self.index.dl[doc_id]) * cnt
            for doc_id in doc_ids.difference(d for d, _ in data[q]):
                result[doc_id] = result[doc_id] + self._score_term_doc(idf, 0, self.index.dl[doc_id]) * cnt
        return list(result.items())


if __name__ == '__main__':
    # BM25Index.create(
    #     Path("/home/vadim/Data/wikidump.tokenized.jsonl"),
    #     # Path("/home/vadim/Data/test.jsonl"),
    #     TermDict.open(Path("../../term.index")),
    #     50
    # )
    # print("created")
    index = BM25Index.open(
        0.75,
        1.5,
        Path("db/v2/index.pkl"),
        Path("db/v2/posting.list")
    )
    while True:
        query = input("enter query: ")
        result = index.query(query)
        for k, v in sorted(result, key=lambda x: x[1], reverse=True)[:100]:
            print(f"https://en.wikipedia.org/?curid={k}  => {v:.3f}")

    print(sr)
