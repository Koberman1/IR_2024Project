import json
import requests
from time import time

url = 'http://34.67.56.108:8080/'


class Testing:

    def __init__(self):
        with open('queries_train.json', 'rt') as f:
            self.queries = json.load(f)


def average_precision(true_list: list, predicted_list: list, k=40):
    true_set = frozenset(true_list)
    predicted_list = predicted_list[:k]
    precisions = []
    for i, doc_id in enumerate(predicted_list):
        if doc_id in true_set:
            prec = (len(precisions) + 1) / (i + 1)
            precisions.append(prec)
    if len(precisions) == 0:
        return 0.0
    return round(sum(precisions) / len(precisions), 3)


def precision_at_k(true_list: list, predicted_list: list, k: int) -> float:
    true_set = frozenset(true_list)
    predicted_list = predicted_list[:k]
    if len(predicted_list) == 0:
        return 0.0
    return round(len([1 for doc_id in predicted_list if doc_id in true_set]) / len(predicted_list), 3)


def recall_at_k(true_list: list, predicted_list: list, k: int) -> float:
    true_set = frozenset(true_list)
    predicted_list = predicted_list[:k]
    if len(true_set) < 1:
        return 1.0
    return round(len([1 for doc_id in predicted_list if doc_id in true_set]) / len(true_set), 3)


def f1_at_k(true_list: list, predicted_list: list, k: int) -> float:
    p = precision_at_k(true_list, predicted_list, k)
    r = recall_at_k(true_list, predicted_list, k)
    if p == 0.0 or r == 0.0:
        return 0.0
    return round(2.0 / (1.0 / p + 1.0 / r), 3)


def results_quality(true_list, predicted_list):
    p5 = precision_at_k(true_list, predicted_list, 5)
    f1_30 = f1_at_k(true_list, predicted_list, 30)
    if p5 == 0.0 or f1_30 == 0.0:
        return 0.0
    return round(2.0 / (1.0 / p5 + 1.0 / f1_30), 3)


if __name__ == '__main__':
    testing = Testing()
    print("query,duration,precision,recall,f1-score")
    for q, true_wids in testing.queries.items():
        duration, ap = None, None
        t_start = time()
        try:
            res = requests.get(url + '/search', {'query': q}, timeout=35)
            duration = time() - t_start
            if res.status_code == 200:
                pred_wids, _ = zip(*res.json())
                precision = precision_at_k(true_wids, pred_wids, 10)
                recall = recall_at_k(true_wids, pred_wids, 10)
                f1 = f1_at_k(true_wids, pred_wids, 10)
                print(f'{q},{duration},{precision},{recall},{f1}')
        except:
            pass
