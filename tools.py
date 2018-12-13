from itertools import zip_longest
from functools import reduce


char_map = {
    'а': 'a',
    'б': 'b',
    'в': 'v',
    'г': 'g',
    'д': 'd',
    'е': 'e',
    'ё': 'e',
    'ж': 'zh',
    'з': 'z',
    'и': 'i',
    'й': 'y',
    'к': 'k',
    'л': 'l',
    'м': 'm',
    'н': 'n',
    'о': 'o',
    'п': 'p',
    'р': 'r',
    'с': 's',
    'т': 't',
    'у': 'u',
    'ф': 'f',
    'х': 'h',
    'ц': 'c',
    'ч': 'ch',
    'ш': 'sch',
    'щ': 'sch',
    'ъ': '',
    'ы': 'y',
    'ь': '',
    'э': 'e',
    'ю': 'yu',
    'я': 'ya',
    'і': 'i',
    'ї': 'yi',
    'є': 'e',

    'q': 'k',
    'w': 'w',
    'e': 'e',
    'r': 'r',
    't': 't',
    'y': 'y',
    'u': 'u',
    'i': 'i',
    'o': 'o',
    'p': 'p',
    'a': 'a',
    's': 's',
    'd': 'd',
    'f': 'f',
    'g': 'g',
    'h': 'h',
    'j': 'j',
    'k': 'k',
    'l': 'l',
    'z': 'z',
    'c': 'c',
    'v': 'v',
    'b': 'b',
    'n': 'n',
    'm': 'm',
    'x': 'ks'
}


def normalize_pib(pib, ordered=False):
    pib_normalized = []

    for word in pib.split():
        if len(word) > 2:
            pib_normalized.append(''.join([char_map[c] for c in word if c.isalpha()]))

    if ordered:
        pib_normalized.sort()

    return ' '.join(pib_normalized)


def calculate_pib_similarity(pib1, pib2):
    if len(pib1) > len(pib2):
        pib1, pib2 = pib2, pib1

    words_cnt = len(pib2)

    num_of_similar_symbols = lambda cnt, x: cnt + (x[0] == x[1])
    total_sim = 0

    for w1 in pib1:
        _sim, _w2 = 0, pib2[0]
        for w2 in pib2:
            sim = reduce(num_of_similar_symbols, [0, *zip_longest(w1, w2, fillvalue='')]) / max(len(w1), len(w2))
            if sim > _sim:
                _sim = sim
                _w2 = w2

        total_sim += _sim

        pib2.remove(_w2)

    return total_sim / words_cnt
