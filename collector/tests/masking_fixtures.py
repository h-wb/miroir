import random
import string
import zlib

from miroir_collector.masking import Masker

MASKER = Masker()


def github_token() -> str:
    alphabet = string.digits + string.ascii_letters
    rng = random.Random(42)
    body = "".join(rng.choices(alphabet, k=30))
    crc = zlib.crc32(body.encode("ascii"))
    checksum = ""
    for _ in range(6):
        crc, digit = divmod(crc, 62)
        checksum = alphabet[digit] + checksum
    return "ghp_" + body + checksum
