import re
import numpy as np
import nltk
from nltk import sent_tokenize, word_tokenize
from nltk.cluster.util import cosine_distance
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.luhn import LuhnSummarizer as Summarizer
from sumy.nlp.stemmers import Stemmer
from sumy.utils import get_stop_words

from transformers import BartTokenizer, BartForConditionalGeneration
from nltk import sent_tokenize

MULTIPLE_WHITESPACE_PATTERN = re.compile(r"\s+", re.UNICODE)


## TextRank : 라이브러리보다 더 뛰어남

def normalize_whitespace(text):
    """
    Translates multiple whitespace into single space character.
    If there is at least one new line character chunk is replaced
    by single LF (Unix new line) character.
    """
    return MULTIPLE_WHITESPACE_PATTERN.sub(_replace_whitespace, text)


def _replace_whitespace(match):
    text = match.group()

    if "\n" in text or "\r" in text:
        return "\n"
    else:
        return " "


def is_blank(string):
    """
    Returns `True` if string contains only white-space characters
    or is empty. Otherwise `False` is returned.
    """
    return not string or string.isspace()


def get_symmetric_matrix(matrix):
    """
    Get Symmetric matrix
    :param matrix:
    :return: matrix
    """
    return matrix + matrix.T - np.diag(matrix.diagonal())


def core_cosine_similarity(vector1, vector2):
    """
    measure cosine similarity between two vectors
    :param vector1:
    :param vector2:
    :return: 0 < cosine similarity value < 1
    """
    return 1 - cosine_distance(vector1, vector2)

class TextRank4Sentences():
    def __init__(self):
        self.damping = 0.85  # damping coefficient, usually is .85
        self.min_diff = 1e-5  # convergence threshold
        self.steps = 100  # iteration steps
        self.text_str = None
        self.sentences = None
        self.pr_vector = None

    def _sentence_similarity(self, sent1, sent2, stopwords=None):
        if stopwords is None:
            stopwords = []

        sent1 = [w.lower() for w in sent1]
        sent2 = [w.lower() for w in sent2]

        all_words = list(set(sent1 + sent2))

        vector1 = [0] * len(all_words)
        vector2 = [0] * len(all_words)

        # build the vector for the first sentence
        for w in sent1:
            if w in stopwords:
                continue
            vector1[all_words.index(w)] += 1

        # build the vector for the second sentence
        for w in sent2:
            if w in stopwords:
                continue
            vector2[all_words.index(w)] += 1

        return core_cosine_similarity(vector1, vector2)

    def _build_similarity_matrix(self, sentences, stopwords=None):
        # create an empty similarity matrix
        sm = np.zeros([len(sentences), len(sentences)])

        for idx1 in range(len(sentences)):
            for idx2 in range(len(sentences)):
                if idx1 == idx2:
                    continue

                sm[idx1][idx2] = self._sentence_similarity(sentences[idx1], sentences[idx2], stopwords=stopwords)

        # Get Symmeric matrix
        sm = get_symmetric_matrix(sm)

        # Normalize matrix by column
        norm = np.sum(sm, axis=0)
        sm_norm = np.divide(sm, norm, where=norm != 0)  # this is ignore the 0 element in norm

        return sm_norm

    def _run_page_rank(self, similarity_matrix):

        pr_vector = np.array([1] * len(similarity_matrix))

        # Iteration
        previous_pr = 0
        for epoch in range(self.steps):
            pr_vector = (1 - self.damping) + self.damping * np.matmul(similarity_matrix, pr_vector)
            if abs(previous_pr - sum(pr_vector)) < self.min_diff:
                break
            else:
                previous_pr = sum(pr_vector)

        return pr_vector

    def _get_sentence(self, index):

        try:
            return self.sentences[index]
        except IndexError:
            return ""

    def get_top_sentences(self, number=5):

        top_sentences = []

        if self.pr_vector is not None:

            sorted_pr = np.argsort(self.pr_vector)
            sorted_pr = list(sorted_pr)
            sorted_pr.reverse()

            index = 0
            for epoch in range(number):
                sent = self.sentences[sorted_pr[index]]
                sent = normalize_whitespace(sent)
                top_sentences.append(sent)
                index += 1

        return top_sentences

    def analyze(self, text, stop_words=None):
        self.text_str = text
        self.sentences = sent_tokenize(self.text_str)

        tokenized_sentences = [word_tokenize(sent) for sent in self.sentences]

        similarity_matrix = self._build_similarity_matrix(tokenized_sentences, stop_words)

        self.pr_vector = self._run_page_rank(similarity_matrix)


## TextRank

def textrank_summary(texts):
    tr4sh = TextRank4Sentences()
    tr4sh.analyze(texts)
    textrank_sum = tr4sh.get_top_sentences(3)

    tsum = ''
    for t in range(len(textrank_sum)):
        if t == 0:
            tsum = textrank_sum[t]
        else:
            tsum = tsum + ' ' + textrank_sum[t]
    return tsum


# print(tsum )


## LexRank
## 오픈 라이브러리 사용


def laxrank_summary(texts):
    LANGUAGE = "english"
    SENTENCES_COUNT = 3

    parser = PlaintextParser.from_string(texts, Tokenizer(LANGUAGE))
    stemmer = Stemmer(LANGUAGE)
    summarizer = Summarizer(stemmer)
    summarizer.stop_words = get_stop_words(LANGUAGE)

    lexrank_sum = summarizer(parser.document, SENTENCES_COUNT)

    lsum = ''
    for l in range(len(lexrank_sum)):
        if l == 0:
            lsum = str(lexrank_sum[l])
        else:
            lsum = lsum + ' ' + str(lexrank_sum[l])

    return lsum




### BERT 추출 요약 ver 1.0




### BART 생성 요약 ver 1.0

model = BartForConditionalGeneration.from_pretrained('facebook/bart-large-cnn')
tokenizer = BartTokenizer.from_pretrained('facebook/bart-large-cnn')

torch_device = 'cpu'

def bart_summarize(text, num_beams, length_penalty, max_length, min_length, no_repeat_ngram_size):
    text = text.replace('\n', '')
    text_input_ids = tokenizer.batch_encode_plus([text], return_tensors='pt', max_length=1024)['input_ids'].to(
        torch_device)
    summary_ids = model.generate(text_input_ids, num_beams=int(num_beams), length_penalty=float(length_penalty),
                                 max_length=int(max_length), min_length=int(min_length),
                                 no_repeat_ngram_size=int(no_repeat_ngram_size))
    summary_txt = tokenizer.decode(summary_ids.squeeze(), skip_special_tokens=True)
    return summary_txt


def bart_generate(doc):
    #target = sent_tokenize(doc)
    #target_sent = target[0] + ' ' + target[1] + ' ' + target[2]
    #print(len(target),len(target_sent ))
    target_sent = doc

    ## bart
    num_beams = 4
    length_penalty = 2.0
    max_length =  142
    min_length = 56
    no_repeat_ngram_size = 3
    # 문장별 입력... 별로 좋은 결과는 x
    #bart_1 = bart_summarize(target[0] , num_beams, length_penalty, max_length, min_length, no_repeat_ngram_size)
    #bart_2 = bart_summarize(target[1], num_beams, length_penalty, max_length, min_length, no_repeat_ngram_size)
    #bart_3 = bart_summarize(target[1], num_beams, length_penalty, max_length, min_length, no_repeat_ngram_size)
    #bart = bart_1 + ' ' + bart_2 + ' ' + bart_3

    bart = bart_summarize(target_sent , num_beams, length_penalty, max_length, min_length, no_repeat_ngram_size)

    return bart