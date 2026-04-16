package dev.cachly

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

// ── CosineSimilarity ──────────────────────────────────────────────────────────

class CosineSimilarityTest {

    private val cache = SemanticCache(jedis = org.mockito.kotlin.mock())

    @Test fun `identical vectors returns 1`() {
        val v = doubleArrayOf(1.0, 2.0, 3.0)
        assertEquals(1.0, cache.cosineSimilarity(v, v), 1e-9)
    }

    @Test fun `orthogonal vectors returns 0`() {
        assertEquals(0.0,
            cache.cosineSimilarity(doubleArrayOf(1.0, 0.0), doubleArrayOf(0.0, 1.0)), 1e-9)
    }

    @Test fun `zero vector returns 0`() {
        assertEquals(0.0,
            cache.cosineSimilarity(doubleArrayOf(0.0, 0.0), doubleArrayOf(1.0, 2.0)))
    }

    @Test fun `opposite vectors returns -1`() {
        assertEquals(-1.0,
            cache.cosineSimilarity(doubleArrayOf(1.0, 0.0), doubleArrayOf(-1.0, 0.0)), 1e-9)
    }

    @Test fun `similar vectors high score`() {
        val sim = cache.cosineSimilarity(
            doubleArrayOf(0.9, 0.1, 0.0), doubleArrayOf(0.85, 0.15, 0.0))
        assertTrue(sim > 0.99, "Expected > 0.99 but got $sim")
    }
}

// ── SemanticResult ────────────────────────────────────────────────────────────

class SemanticResultTest {

    @Test fun `hit result contains similarity`() {
        val r = SemanticResult("answer", hit = true, similarity = 0.97)
        assertTrue(r.hit)
        assertEquals(0.97, r.similarity)
    }

    @Test fun `miss result has null similarity`() {
        val r = SemanticResult("answer", hit = false)
        assertFalse(r.hit)
        assertNull(r.similarity)
    }

    @Test fun `generic type - integer`() {
        val r = SemanticResult(42, hit = true, similarity = 0.91)
        assertTrue(r.hit)
        assertEquals(42, r.value)
    }
}

// ── SemanticOptions ───────────────────────────────────────────────────────────

class SemanticOptionsTest {

    @Test fun `defaults`() {
        val opts = SemanticOptions()
        assertEquals(0.85, opts.similarityThreshold, 1e-9)
        assertEquals("cachly:sem", opts.namespace)
        assertEquals(0L, opts.ttlSeconds)
        assertFalse(opts.autoNamespace)
        assertFalse(opts.useHybrid)
        assertTrue(opts.normalizePrompt)
    }

    @Test fun `useHybrid defaults to false`() {
        assertFalse(SemanticOptions().useHybrid)
    }

    @Test fun `useHybrid can be set to true`() {
        assertTrue(SemanticOptions(useHybrid = true).useHybrid)
    }

    @Test fun `copy preserves useHybrid`() {
        val base = SemanticOptions(useHybrid = true, namespace = "ns:test")
        val copied = base.copy(similarityThreshold = 0.9)
        assertTrue(copied.useHybrid, "copy() must preserve useHybrid=true")
        assertEquals("ns:test", copied.namespace)
    }

    @Test fun `SemanticOptions equality considers useHybrid`() {
        val a = SemanticOptions(useHybrid = true)
        val b = SemanticOptions(useHybrid = false)
        assertNotEquals(a, b)
    }
}

// ── confidenceBand ────────────────────────────────────────────────────────────

class ConfidenceBandTest {

    @Test fun `high confidence when above high threshold`() {
        assertEquals(SemanticConfidence.HIGH,
            SemanticCache.confidenceBand(0.98, 0.85, 0.97))
    }

    @Test fun `medium confidence between thresholds`() {
        assertEquals(SemanticConfidence.MEDIUM,
            SemanticCache.confidenceBand(0.90, 0.85, 0.97))
    }

    @Test fun `uncertain when below threshold`() {
        assertEquals(SemanticConfidence.UNCERTAIN,
            SemanticCache.confidenceBand(0.80, 0.85, 0.97))
    }

    @Test fun `exactly at high threshold is high`() {
        assertEquals(SemanticConfidence.HIGH,
            SemanticCache.confidenceBand(0.97, 0.85, 0.97))
    }

    @Test fun `exactly at threshold is medium`() {
        assertEquals(SemanticConfidence.MEDIUM,
            SemanticCache.confidenceBand(0.85, 0.85, 0.97))
    }
}

// ── §4 detectNamespace ────────────────────────────────────────────────────────

class DetectNamespaceTest {

    @Test fun `detects code namespace`() {
        val codePrompts = listOf(
            "def process(data): return data * 2",
            "const x = () => 42",
            "class MyService { constructor() {} }",
            "import React from 'react'",
            "#!/usr/bin/env python3",
            "func main() { fmt.Println(\"hi\") }",
            "#include <iostream> int main() {}",
            "interface { bar(): void }",
            "struct { host string }",
            "transform = lambda x: x * 2",
        )
        codePrompts.forEach { prompt ->
            assertEquals("cachly:sem:code", SemanticCache.detectNamespace(prompt),
                "Expected code namespace for: $prompt")
        }
    }

    @Test fun `detects translation namespace`() {
        val translationPrompts = listOf(
            "translate this text to english",
            "übersetze diesen Text auf englisch",
            "bitte auf deutsch schreiben",
            "traduis ce texte en anglais",
            "vertaal naar het Frans",
        )
        translationPrompts.forEach { prompt ->
            assertEquals("cachly:sem:translation", SemanticCache.detectNamespace(prompt),
                "Expected translation namespace for: $prompt")
        }
    }

    @Test fun `detects summary namespace`() {
        val summaryPrompts = listOf(
            "summarize this article for me",
            "summarise the following text",
            "fasse zusammen was in dem Text steht",
            "give me the key points of this paper",
            "explain in a nutshell",
        )
        summaryPrompts.forEach { prompt ->
            assertEquals("cachly:sem:summary", SemanticCache.detectNamespace(prompt),
                "Expected summary namespace for: $prompt")
        }
    }

    @Test fun `detects qa namespace`() {
        val qaPrompts = listOf(
            "what is the capital of France?",
            "how does photosynthesis work?",
            "wer ist der aktuelle Bundeskanzler?",
            "wie funktioniert ein JWT?",
        )
        qaPrompts.forEach { prompt ->
            assertEquals("cachly:sem:qa", SemanticCache.detectNamespace(prompt),
                "Expected qa namespace for: $prompt")
        }
    }

    @Test fun `trailing question mark is qa`() {
        assertEquals("cachly:sem:qa",
            SemanticCache.detectNamespace("Is Redis faster than Memcached?"))
    }

    @Test fun `detects creative namespace`() {
        val creativePrompts = listOf(
            "Write a poem about autumn",
            "Tell me a story about dragons",
            "Generate a product description for running shoes",
        )
        creativePrompts.forEach { prompt ->
            assertEquals("cachly:sem:creative", SemanticCache.detectNamespace(prompt),
                "Expected creative namespace for: $prompt")
        }
    }

    @Test fun `code takes priority over translation`() {
        assertEquals("cachly:sem:code",
            SemanticCache.detectNamespace("translate this function def foo(): pass"))
    }

    @Test fun `is case-insensitive`() {
        assertEquals("cachly:sem:code",        SemanticCache.detectNamespace("CONST X = 1"))
        assertEquals("cachly:sem:translation", SemanticCache.detectNamespace("TRANSLATE TO GERMAN"))
        assertEquals("cachly:sem:summary",     SemanticCache.detectNamespace("SUMMARIZE THIS"))
        assertEquals("cachly:sem:qa",          SemanticCache.detectNamespace("WHAT IS GRAVITY?"))
    }

    @Test fun `trims whitespace`() {
        assertEquals("cachly:sem:qa",   SemanticCache.detectNamespace("   what is dark matter?   "))
        assertEquals("cachly:sem:code", SemanticCache.detectNamespace("  const x = 1  "))
    }
}

// ── normalizePrompt ───────────────────────────────────────────────────────────

class NormalizePromptTest {

    @Test fun `removes default filler word please`() {
        val result = SemanticCache.normalizePrompt("please tell me about Redis")
        assertFalse(result.contains("please"), "Filler 'please' should be removed, got: $result")
    }

    @Test fun `removes default filler word bitte`() {
        val result = SemanticCache.normalizePrompt("bitte erkläre mir Redis")
        assertFalse(result.contains("bitte"), "Filler 'bitte' should be removed, got: $result")
    }

    @Test fun `lowercases text`() {
        assertEquals("what is redis?", SemanticCache.normalizePrompt("WHAT IS REDIS?"))
    }

    @Test fun `collapses extra whitespace`() {
        val result = SemanticCache.normalizePrompt("what   is   redis?")
        assertFalse(result.contains("  "), "Extra spaces should be collapsed, got: $result")
    }

    @Test fun `normalizes trailing exclamation to question mark`() {
        val result = SemanticCache.normalizePrompt("what is redis!")
        assertTrue(result.endsWith("?"), "Trailing ! should become ?, got: $result")
    }

    @Test fun `empty string returns empty`() {
        assertEquals("", SemanticCache.normalizePrompt(""))
    }

    @Test fun `accepts custom filler word list`() {
        val result = SemanticCache.normalizePrompt("yo what is redis?", listOf("yo"))
        assertFalse(result.startsWith("yo"), "Custom filler 'yo' must be removed, got: $result")
    }
}

// ── §7 quantizeEmbedding ──────────────────────────────────────────────────────

class QuantizeEmbeddingTest {

    @Test fun `empty vector returns empty result`() {
        val q = SemanticCache.quantizeEmbedding(doubleArrayOf())
        @Suppress("UNCHECKED_CAST")
        val values = q["values"] as List<Int>
        assertTrue(values.isEmpty())
        assertEquals(0.0, q["min"] as Double, 1e-9)
        assertEquals(0.0, q["max"] as Double, 1e-9)
    }

    @Test fun `constant vector maps all values to 0`() {
        val q = SemanticCache.quantizeEmbedding(doubleArrayOf(0.5, 0.5, 0.5))
        @Suppress("UNCHECKED_CAST")
        val values = q["values"] as List<Int>
        values.forEach { assertEquals(0, it) }
    }

    @Test fun `min maps to -128 and max maps to 127`() {
        val q = SemanticCache.quantizeEmbedding(doubleArrayOf(0.0, 1.0))
        @Suppress("UNCHECKED_CAST")
        val values = q["values"] as List<Int>
        assertEquals(-128, values[0])
        assertEquals(127,  values[1])
    }

    @Test fun `all values are in int8 range`() {
        val vec = DoubleArray(16) { Math.sin(it.toDouble()) }
        val q = SemanticCache.quantizeEmbedding(vec)
        @Suppress("UNCHECKED_CAST")
        val values = q["values"] as List<Int>
        values.forEach { v ->
            assertTrue(v >= -128 && v <= 127, "Value $v is outside int8 range")
        }
    }

    @Test fun `preserves min and max fields`() {
        val q = SemanticCache.quantizeEmbedding(doubleArrayOf(0.1, 0.4, 0.9))
        assertEquals(0.1, q["min"] as Double, 1e-9)
        assertEquals(0.9, q["max"] as Double, 1e-9)
    }
}


