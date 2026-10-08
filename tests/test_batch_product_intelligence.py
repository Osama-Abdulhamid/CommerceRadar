import unittest

from services.batch.product_intelligence import key_tokens, normalize_text, token_jaccard


class ProductIntelligenceFunctionTests(unittest.TestCase):
    def test_normalize_brand_variants(self):
        self.assertEqual(normalize_text("Samsung"), "samsung")
        self.assertEqual(normalize_text("samsung"), "samsung")
        self.assertEqual(normalize_text("SAMSUNG"), "samsung")

    def test_normalize_title_removes_language_tags_and_punctuation(self):
        self.assertEqual(
            normalize_text('"Samsung Galaxy S24 128GB - Black"@en'),
            "samsung galaxy s24 128gb black",
        )

    def test_key_tokens_prefers_selective_tokens(self):
        self.assertEqual(
            key_tokens("samsung galaxy s24 128gb black", limit=3),
            ["samsung", "galaxy", "128gb"],
        )

    def test_equivalent_titles_have_high_similarity(self):
        left = normalize_text("Samsung Galaxy S24 128GB Black")
        right = normalize_text("Galaxy S24 128 GB Samsung Black")
        self.assertGreaterEqual(token_jaccard(left, right), 0.55)

    def test_unrelated_titles_have_low_similarity(self):
        left = normalize_text("Samsung Galaxy S24 128GB Black")
        right = normalize_text("Krowne Royal Wall Mount Faucet")
        self.assertLess(token_jaccard(left, right), 0.20)


if __name__ == "__main__":
    unittest.main()
