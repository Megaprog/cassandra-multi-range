TWO_POWER_OF_63 = 2 ** 63
TWO_POWER_OF_64 = 2 * TWO_POWER_OF_63
LONG_MAX_VALUE = 9223372036854775807  # java.lang.Long.MAX_VALUE
LONG_MIN_VALUE = -9223372036854775808  # java.lang.Long.MIN_VALUE


def split_long(n: int) -> list[int]:
    return [((TWO_POWER_OF_64 // n) * i) - TWO_POWER_OF_63 for i in range(n)]


def token_range_inclusive(splits: list[int], i: int) -> tuple[int, int]:
    return splits[i], LONG_MAX_VALUE if i == len(splits) - 1 else splits[i + 1] - 1
