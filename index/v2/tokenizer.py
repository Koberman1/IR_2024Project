import re
from typing import List

from nltk import SnowballStemmer
from nltk.corpus import stopwords

# import nltk
#
# nltk.download("stopwords")

_english_stopwords = frozenset(stopwords.words('english'))
_corpus_stopwords = ['category', 'references', 'also', 'links', 'extenal', 'see', 'thumb']
_RE_WORD = re.compile(r"""[\#\@\w](['\-]?\w){2,24}""", re.UNICODE)

_all_stopwords = _english_stopwords.union(_corpus_stopwords)

from nltk.stem.porter import *

# stemmer = PorterStemmer()
stemmer = SnowballStemmer("english")


def tokenize(text: str) -> List[str]:
    tokens = [token.group() for token in _RE_WORD.finditer(text.lower())]
    filtered = [token for token in tokens if token not in _all_stopwords]
    stemmed = [stemmer.stem(x) for x in filtered]
    return stemmed
