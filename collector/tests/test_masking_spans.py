import random
import re
import string
import unittest
from collections import Counter
from types import SimpleNamespace
from unittest.mock import patch

from kingfisher_sdk import Scanner

from .masking_fixtures import MASKER, github_token

_AWS_ID = "AKIA" + "Q3EGRV7XHPKZ2M5D"
_GITLAB = "glpat-" + "Ab3dEf6hIj9kLm2oPq5sTu8wXy1zAbCdEf34"


def _covered(spans):
    return {pos for start, end, _ in spans for pos in range(start, end)}


class SpanTests(unittest.TestCase):
    def test_partial_scanner_matches_do_not_expose_legacy_matches(self):
        github = github_token()
        digits = github + "4111111111111111"
        suffix = github + "Zq9xW2vB7nK4mR8tY1uE"
        aws_github = _AWS_ID + "_" + github
        for method, text, secret in (
            (MASKER.text, f"GITLAB_TOKEN {_GITLAB} done", _GITLAB),
            (MASKER.text, aws_github, aws_github),
            (MASKER.tokens, digits, digits),
            (MASKER.url, "https://example.com/p/" + digits, digits),
            (MASKER.url, "https://example.com/p/" + suffix, suffix),
        ):
            with self.subTest(method=method.__name__):
                masked, _ = method(text)
                self.assertFalse(
                    any(secret[i : i + 6] in masked for i in range(len(secret) - 5))
                )

    def test_scanner_only_adds_coverage(self):
        rng = random.Random(36)

        def random_run(length):
            return "".join(rng.choices(string.ascii_letters + string.digits, k=length))

        corpus = []
        for _ in range(300):
            chunks = rng.sample(
                [
                    "ordinary prose",
                    github_token(),
                    _GITLAB,
                    _AWS_ID,
                    "AIza" + random_run(35),
                    "sk_live_" + random_run(24),
                    "AGE-SECRET-KEY-1" + random_run(58).upper(),
                    "POSTGRES_PASSWORD=" + random_run(29),
                    random_run(45),
                    "4111-1111-1111-1111",
                ],
                rng.randint(2, 8),
            )
            corpus.append(
                "".join(
                    chunk + rng.choice(("", " ", "_", "/", "\n")) for chunk in chunks
                )
            )

        MASKER.spans("warm up")
        with patch.object(Scanner, "scan", return_value=[]):
            legacy = [_covered(MASKER.spans(text)) for text in corpus]
        for i, (text, expected) in enumerate(zip(corpus, legacy, strict=True)):
            with self.subTest(case=i):
                self.assertTrue(expected <= _covered(MASKER.spans(text)))

    def test_non_visible_findings_are_masked(self):
        secret = "p0o9I8u7Y6t5R4e3W2q1" + "+ZxCvBnMmNbVcXz/a1Q2"
        text = f"AWS key {_AWS_ID} secret: {secret}"
        scanner = MASKER._scanner._start()
        self.assertEqual(
            {finding.visible for finding in scanner.scan(text)}, {False, True}
        )
        self.assertEqual(
            MASKER.tokens(text),
            (
                "AWS key [masked:token] secret: [masked:scanner]",
                {"token": 1, "scanner": 1},
            ),
        )

    def test_text_with_nul_bytes_does_not_leave(self):
        # The scanner would decode this as UTF-16 and report positions in its copy.
        text = "\x00".join("before " + github_token() + " after") + "\x00"
        with patch.object(Scanner, "scan") as scan:
            self.assertEqual(
                MASKER.text(text), ("[masked:unscanned]", {"unscanned": 1})
            )
            scan.assert_not_called()

    def test_union_labels_and_marker_counts_for_overlaps_and_bridges(self):
        token = github_token()
        assignment = "Ab12cd34ef56"
        text = f"prefix POSTGRES_PASSWORD={assignment} {token} suffix"
        first, second = text.index(assignment), text.index(token)
        for end in (first + 6, second, second + 6):
            with self.subTest(end=end):
                finding = SimpleNamespace(
                    location={"start_offset": first - 2, "end_offset": end}
                )
                with patch.object(Scanner, "scan", return_value=[finding]) as scan:
                    masked, counts = MASKER.text(text)
                    scan.assert_called_once_with(text, timeout=5)
                self.assertEqual(
                    counts, dict(Counter(re.findall(r"\[masked:(\w+)\]", masked)))
                )
                self.assertEqual(
                    counts, {"assignment": 1, **({"token": 1} if end < second else {})}
                )

    def test_builtin_ids_do_not_use_custom_namespace(self):
        rules = MASKER._scanner._start().rules
        self.assertFalse(
            any(rule["id"].startswith("custom.") for rule in rules.metadata())
        )
