import json
from pathlib import Path

from tqdm import tqdm

from index.v2.tokenizer import tokenize



def process(docs):
    return [{
        'id': record['id'],
        'tokens': tokenize(record['text'])
    } for record in docs]


def read_json_records(path: Path):
    with path.open("rt") as fp:
        for line in tqdm(fp, total=6348910, desc=f"Reading record of file '{path.name}'"):
            yield json.loads(line)


if __name__ == '__main__':
    # parquet_dir = Path("/home/vadim/Data/wiki")
    # output_path = Path("/home/vadim/Data/wikidump.tokenized.jsonl")
    # supplier = for_each_document_in_dataset(parquet_dir, columns=["id", "text"], batch_size=10)
    # with output_path.open("wt") as writer:
    #     for record in execute_in_batches(1000, 20, supplier, process):
    #         writer.write(json.dumps(record) + "\n")

    for record in read_json_records(Path("/home/vadim/Data/wikidump.tokenized.jsonl")):
        pass
