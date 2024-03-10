import csv
import gzip

filename = "db/pr/page_rank.csv.gz"


class PageRankScores:
    def __init__(self):
        print("Loading Page Rank Scores...")
        self.pageRankScores = {}
        with gzip.open(filename, mode="rt") as f:
            reader = csv.DictReader(f, fieldnames=["doc_id", "score"])
            for row in reader:
                doc_id = int(row["doc_id"])
                score = float(row["score"])
                self.pageRankScores[doc_id] = score
                if score < 15.0:
                    break
        print("Page Rank Scores loaded")

    def is_doc_id_in_pagerank(self, doc_id):
        if doc_id in self.pageRankScores:
            return True
        return False


if __name__ == '__main__':
    page_rank_scores = PageRankScores()
    print()
