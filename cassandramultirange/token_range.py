TWO_POWER_OF_63 = 2 ** 63
TWO_POWER_OF_64 = 2 * TWO_POWER_OF_63
LONG_MAX_VALUE = 9223372036854775807  # java.lang.Long.MAX_VALUE
LONG_MIN_VALUE = -9223372036854775808  # java.lang.Long.MIN_VALUE


def split_long(n: int) -> list[int]:
    return [((TWO_POWER_OF_64 // n) * i) - TWO_POWER_OF_63 for i in range(n)]


def token_range_inclusive(splits: list[int], i: int) -> tuple[int, int]:
    return splits[i], LONG_MAX_VALUE if i == len(splits) - 1 else splits[i + 1] - 1


# tests

def test_splits():
    assert split_long(30) == [
        -9223372036854775808,
        -8608480567731124088,
        -7993589098607472368,
        -7378697629483820648,
        -6763806160360168928,
        -6148914691236517208,
        -5534023222112865488,
        -4919131752989213768,
        -4304240283865562048,
        -3689348814741910328,
        -3074457345618258608,
        -2459565876494606888,
        -1844674407370955168,
        -1229782938247303448,
        -614891469123651728,
        -8,
        614891469123651712,
        1229782938247303432,
        1844674407370955152,
        2459565876494606872,
        3074457345618258592,
        3689348814741910312,
        4304240283865562032,
        4919131752989213752,
        5534023222112865472,
        6148914691236517192,
        6763806160360168912,
        7378697629483820632,
        7993589098607472352,
        8608480567731124072,
    ]


def test_empty_splits():
    assert split_long(0) == []


def test_single_split():
    assert split_long(1)[0] == LONG_MIN_VALUE


def test_two_splits():
    assert split_long(2) == [LONG_MIN_VALUE, 0]


def test_single_token_range():
    low, high = token_range_inclusive(split_long(1), 0)
    assert low == LONG_MIN_VALUE
    assert high == LONG_MAX_VALUE


def test_two_tokens_range():
    low0, high0 = token_range_inclusive(split_long(2), 0)
    assert low0 == LONG_MIN_VALUE
    assert high0 == -1

    low1, high1 = token_range_inclusive(split_long(2), 1)
    assert low1 == 0
    assert high1 == LONG_MAX_VALUE

