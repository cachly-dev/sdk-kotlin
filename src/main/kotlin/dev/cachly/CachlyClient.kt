package dev.cachly

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import redis.clients.jedis.UnifiedJedis
import redis.clients.jedis.params.ScanParams
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.math.sqrt

// ── Types ─────────────────────────────────────────────────────────────────────

enum class SemanticConfidence { HIGH, MEDIUM, UNCERTAIN }

data class SemanticResult<T>(
    val value: T,
    val hit: Boolean,
    val similarity: Double? = null,
    val confidence: SemanticConfidence? = null,
)

data class SemanticOptions(
    val similarityThreshold: Double = 0.85,
    val ttlSeconds: Long = 0,
    val namespace: String = "cachly:sem",
    val highConfidenceThreshold: Double = 0.97,
    val normalizePrompt: Boolean = true,
    val autoNamespace: Boolean = false,
    val useAdaptiveThreshold: Boolean = false,
    val quantize: String = "",
    val useHybrid: Boolean = false,
)

// ── New Result Types ───────────────────────────────────────────────────────────

data class CacheStats(
    val hits: Long, val misses: Long, val hitRate: Double, val total: Long,
    val namespaces: List<Map<String, Any>> = emptyList(),
)

data class BatchIndexEntry(
    val id: String, val prompt: String, val embedding: List<Double>,
    val namespace: String, val expiresAt: String? = null,
)

data class BatchIndexResult(val indexed: Int, val skipped: Int)
data class GuardrailViolation(val type: String, val pattern: String, val action: String)
data class GuardrailCheckResult(val safe: Boolean, val violations: List<GuardrailViolation>)
data class TagsResult(val key: String, val tags: List<String>, val ok: Boolean = true)

data class InvalidateTagResult(
    val tag: String, val keysDeleted: Int, val keys: List<String>, val durationMs: Long,
)

data class SwrEntry(
    val key: String, val fetcherHint: String?, val staleFor: String?, val refreshAt: String?,
)

data class SwrCheckResult(val staleKeys: List<SwrEntry>, val count: Int, val checkedAt: String)
data class BulkWarmupResult(val warmed: Int, val skipped: Int, val durationMs: Long)
data class SnapshotWarmupResult(val warmed: Int, val durationMs: Long)

data class LlmProxyStatsResult(
    val totalRequests: Long, val cacheHits: Long, val cacheMisses: Long,
    val estimatedSavedUsd: Double, val avgLatencyMsCached: Long, val avgLatencyMsUncached: Long,
)

data class PubSubMessage(val channel: String, val message: String, val at: String)

data class WorkflowCheckpoint(
    val id: String, val runId: String, val stepIndex: Int, val stepName: String,
    val agentName: String, val status: String, val state: String?, val output: String?,
    val durationMs: Long?, val createdAt: String?,
)

data class WorkflowRun(
    val runId: String, val steps: Int, val latestStatus: String,
    val checkpoints: List<WorkflowCheckpoint> = emptyList(),
)

// ── Edge Cache Types (Feature #5) ─────────────────────────────────────────────

data class EdgeCacheConfig(
    val id: String,
    val instanceId: String,
    val enabled: Boolean,
    val edgeTtl: Int,
    val workerUrl: String,
    val cloudflareZoneId: String,
    val purgeOnWrite: Boolean,
    val cacheSearchResults: Boolean,
    val totalHits: Long,
    val totalMisses: Long,
    val hitRate: Double,
)

data class EdgeCacheConfigUpdate(
    val enabled: Boolean? = null,
    val edgeTtl: Int? = null,
    val workerUrl: String? = null,
    val cloudflareZoneId: String? = null,
    val purgeOnWrite: Boolean? = null,
    val cacheSearchResults: Boolean? = null,
)

data class EdgePurgeOptions(
    val namespaces: List<String>? = null,
    val urls: List<String>? = null,
)

data class EdgePurgeResult(
    val purged: Int,
    val urls: List<String>,
)

data class EdgeCacheStats(
    val enabled: Boolean,
    val workerUrl: String,
    val edgeTtl: Int,
    val totalHits: Long,
    val totalMisses: Long,
    val hitRate: Double,
)

// ── SemanticCache ─────────────────────────────────────────────────────────────

/**
 * Cache LLM responses by *meaning*, not just exact key.
 *
 * When [vectorUrl] is set → uses cachly pgvector API (HNSW O(log n)) with
 * graceful SCAN fallback if the API is unreachable.
 *
 * **Storage layout:**
 * - `{ns}:emb:{uuid}` – embedding + prompt (SCAN mode only, in Valkey)
 * - `{ns}:val:{uuid}` – actual cached value (always in Valkey)
 */
class SemanticCache internal constructor(
    private val jedis: UnifiedJedis,
    private val vectorUrl: String? = null,
) {
    private val mapper = jacksonObjectMapper()

    // Reuse a single HttpClient (thread-safe).
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()

    companion object {
        private val DEFAULT_FILLER_WORDS = listOf(
            // EN
            "please", "hey", "hi", "hello",
            "could you", "can you", "would you", "will you",
            "just", "quickly", "briefly", "simply",
            "tell me", "show me", "give me", "help me", "assist me",
            "explain to me", "describe to me",
            "i need", "i want", "i would like", "i'd like", "i'm looking for",
            // DE
            "bitte", "mal eben", "schnell", "kurz", "einfach",
            "kannst du", "könntest du", "könnten sie", "würden sie", "würdest du",
            "hallo", "hi", "hey",
            "sag mir", "zeig mir", "gib mir", "hilf mir", "erkläre mir", "erklär mir",
            "ich brauche", "ich möchte", "ich hätte gerne", "ich suche",
            // FR
            "s'il vous plaît", "svp", "stp", "bonjour", "salut", "allô",
            "pouvez-vous", "pourriez-vous", "peux-tu", "pourrais-tu",
            "dis-moi", "dites-moi", "montre-moi", "montrez-moi",
            "j'ai besoin de", "je voudrais", "je cherche", "je souhaite",
            "expliquez-moi", "explique-moi", "aidez-moi", "aide-moi",
            // ES
            "por favor", "hola", "oye",
            "puedes", "podrías", "podría usted", "me puedes", "me podrías",
            "dime", "dígame", "muéstrame", "muéstreme", "dame", "deme",
            "necesito", "quisiera", "me gustaría", "quiero saber",
            "ayúdame", "ayúdeme", "explícame", "explíqueme",
            // IT
            "per favore", "perfavore", "ciao", "salve", "ehi",
            "potresti", "mi potresti", "potrebbe", "mi potrebbe",
            "dimmi", "mi dica", "mostrami", "dammi", "mi dia",
            "ho bisogno di", "vorrei", "mi piacerebbe",
            "aiutami", "mi aiuti", "spiegami", "mi spieghi",
            // PT
            "por favor", "olá", "oi", "ei",
            "pode", "poderia", "você poderia", "você pode", "podes",
            "me diga", "diga-me", "me mostre", "mostre-me", "me dê", "dê-me",
            "preciso de", "gostaria de", "quero saber", "estou procurando",
            "me ajude", "ajude-me", "explique-me", "me explique",
        )

        /** Strip filler words, lowercase, collapse whitespace. +8–12% hit-rate uplift. */
        fun normalizePrompt(text: String, fillerWords: List<String> = DEFAULT_FILLER_WORDS): String {
            var s = text.trim().lowercase()
            for (fw in fillerWords) s = s.replace(Regex("\\b${Regex.escape(fw)}\\b", RegexOption.IGNORE_CASE), "")
            s = s.replace(Regex("\\s+"), " ").trim()
            s = s.replace(Regex("[?!]+$"), "?")
            return s
        }

        /** Returns the confidence band for a similarity score. */
        fun confidenceBand(sim: Double, threshold: Double, highThreshold: Double): SemanticConfidence = when {
            sim >= highThreshold -> SemanticConfidence.HIGH
            sim >= threshold     -> SemanticConfidence.MEDIUM
            else                 -> SemanticConfidence.UNCERTAIN
        }

        // ── §4 Namespace Auto-Detection ───────────────────────────────────────

        private val CODE_KW        = listOf("function ", "def ", "class ", "import ", "const ", "let ", "var ",
            "return ", " => ", "void ", "public class", "func ", "#include", "package ",
            "struct {", "interface {", "async def", "lambda ", "#!/")
        private val TRANSLATION_KW = listOf("translate", "übersetze", "auf deutsch", "auf englisch",
            "in english", "in german", "ins deutsche", "ins englische", "übersetz", "traduce", "traduis", "vertaal")
        private val SUMMARY_KW     = listOf("summarize", "summarise", "summary", "zusammenfass", "tl;dr", "tldr",
            "key points", "stichpunkte", "fasse zusammen", "give me a brief", "kurze zusammenfassung", "in a nutshell")
        private val QA_PREFIXES    = listOf("what ", "who ", "where ", "when ", "why ", "how ", "which ",
            "is ", "are ", "was ", "were ", "does ", "do ", "did ",
            "can ", "could ", "would ", "should ", "will ",
            "wer ", "wie ", "wo ", "wann ", "warum ", "welche", "wieso ")

        /**
         * §4 – Classify a prompt into one of 5 semantic namespaces using text heuristics.
         *
         * Overhead: < 0.1 ms. No embedding required.
         *
         * Returns one of: `cachly:sem:code`, `:translation`, `:summary`, `:qa`, `:creative`
         */
        fun detectNamespace(prompt: String): String {
            val s = prompt.trim().lowercase()
            if (CODE_KW.any        { s.contains(it) })    return "cachly:sem:code"
            if (TRANSLATION_KW.any { s.contains(it) })    return "cachly:sem:translation"
            if (SUMMARY_KW.any     { s.contains(it) })    return "cachly:sem:summary"
            if (QA_PREFIXES.any    { s.startsWith(it) })  return "cachly:sem:qa"
            if (s.trimEnd().endsWith("?"))                 return "cachly:sem:qa"
            return "cachly:sem:creative"
        }

        /**
         * §7 – Scalar-quantize a float64 embedding to int8 range [-128, 127].
         * Reduces API JSON payload ~8x (1536-dim: 12 KB → 1.5 KB) with <1% quality loss.
         */
        fun quantizeEmbedding(vec: DoubleArray): Map<String, Any> {
            if (vec.isEmpty()) return mapOf("values" to emptyList<Int>(), "min" to 0.0, "max" to 0.0)
            var minVal = vec[0]; var maxVal = vec[0]
            for (v in vec) { if (v < minVal) minVal = v; if (v > maxVal) maxVal = v }
            val rng = maxVal - minVal
            val values = if (rng > 0) {
                val scale = 255.0 / rng
                vec.map { v -> maxOf(-128, minOf(127, Math.round(scale * (v - minVal)).toInt() - 128)) }
            } else { List(vec.size) { 0 } }
            return mapOf("values" to values, "min" to minVal, "max" to maxVal)
        }
    }

    // ── HTTP helper ────────────────────────────────────────────────────────────

    /**
     * Perform an HTTP request against the cachly vector API.
     * Runs on [Dispatchers.IO] via the caller's coroutine context.
     */
    private fun httpRequestSync(method: String, url: String, body: Any? = null): JsonNode {
        val builder = HttpRequest.newBuilder()
            .uri(URI.create(url)).timeout(Duration.ofSeconds(10))
            .header("Accept", "application/json")

        if (body != null) {
            val json = mapper.writeValueAsString(body)
            builder.header("Content-Type", "application/json")
                   .method(method, HttpRequest.BodyPublishers.ofString(json))
        } else when (method) {
            "DELETE" -> builder.DELETE()
            else     -> builder.method(method, HttpRequest.BodyPublishers.noBody())
        }

        val resp = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() >= 400) {
            error("cachly API $method $url returned ${resp.statusCode()}")
        }
        val responseBody = resp.body()
        return if (responseBody.isNullOrEmpty()) mapper.createObjectNode()
               else mapper.readTree(responseBody)
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /** Derive the val key from an emb key. `{ns}:emb:{uuid}` → `{ns}:val:{uuid}` */
    internal fun valKey(embKey: String): String {
        val lastColon = embKey.lastIndexOf(':')
        val uuidPart = embKey.substring(lastColon) // ":uuid-str"
        val nsType = embKey.substring(0, lastColon) // "{ns}:emb"
        val secondLastColon = nsType.lastIndexOf(':')
        val ns = nsType.substring(0, secondLastColon) // "{ns}"
        return "$ns:val$uuidPart"
    }

    /** Extract the UUID (last segment) from any key. */
    private fun extractId(key: String): String =
        key.substringAfterLast(':', key)

    /** Extract the namespace from an emb/val key: `{ns}:emb:{uuid}` → `{ns}`. */
    private fun extractNamespace(key: String): String {
        val last = key.lastIndexOf(':')
        if (last < 0) return "cachly:sem"
        val nsType = key.substring(0, last)
        val second = nsType.lastIndexOf(':')
        return if (second >= 0) nsType.substring(0, second) else "cachly:sem"
    }

    /** Cursor-based SCAN; collects all matching keys without blocking the server. */
    private fun scanAll(pattern: String): List<String> {
        val keys = mutableListOf<String>()
        val params = ScanParams().match(pattern).count(100)
        var cursor = ScanParams.SCAN_POINTER_START
        do {
            val res = jedis.scan(cursor, params)
            cursor = res.cursor; keys.addAll(res.result)
        } while (cursor != ScanParams.SCAN_POINTER_START)
        return keys
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * §1 – Records whether a cache hit was accepted as correct.
     * Sends feedback to the cachly adaptive-threshold calibrator.
     * No-op when [vectorUrl] is null.
     *
     * @param hitId      Entry UUID returned when [getOrSet] returns a hit
     * @param accepted   `true` if the cached answer was correct
     * @param similarity Cosine similarity of the hit
     * @param namespace  Key namespace
     */
    suspend fun feedback(
        hitId: String,
        accepted: Boolean,
        similarity: Double,
        namespace: String = "cachly:sem",
    ) = withContext(Dispatchers.IO) {
        if (vectorUrl == null || hitId.isBlank()) return@withContext
        runCatching {
            httpRequestSync("POST", "$vectorUrl/feedback", mapOf(
                "hit_id" to hitId, "accepted" to accepted,
                "similarity" to similarity, "namespace" to namespace,
            ))
        }
    }

    /**
     * §1 – Returns the server-side F1-calibrated threshold for a namespace.
     * Falls back to 0.85 when no calibration data exists or vectorUrl is null.
     */
    suspend fun adaptiveThreshold(namespace: String = "cachly:sem"): Double =
        withContext(Dispatchers.IO) {
            if (vectorUrl == null) return@withContext 0.85
            runCatching {
                httpRequestSync(
                    "GET",
                    "$vectorUrl/threshold?namespace=${URLEncoder.encode(namespace, "UTF-8")}"
                )
                .path("threshold").asDouble(0.85)
            }.getOrDefault(0.85)
        }

    /** Set the similarity threshold for a namespace. `POST /v1/sem/:token/threshold` */
    suspend fun setThreshold(namespace: String = "cachly:sem", threshold: Double): JsonNode =
        withContext(Dispatchers.IO) {
            require(vectorUrl != null) { "vectorUrl is required for setThreshold" }
            httpRequestSync("POST", "$vectorUrl/threshold",
                mapOf("namespace" to namespace, "threshold" to threshold))
        }

    /** Return cache statistics. `GET /v1/sem/:token/stats` */
    suspend fun stats(namespace: String = "cachly:sem"): CacheStats = withContext(Dispatchers.IO) {
        require(vectorUrl != null) { "vectorUrl is required for stats" }
        val r = httpRequestSync("GET", "$vectorUrl/stats?namespace=${URLEncoder.encode(namespace, "UTF-8")}")
        CacheStats(
            hits = r.path("hits").asLong(0), misses = r.path("misses").asLong(0),
            hitRate = r.path("hit_rate").asDouble(0.0), total = r.path("total").asLong(0),
            namespaces = r.path("namespaces").map { ns ->
                mapOf("name" to ns.path("name").asText(""), "hits" to ns.path("hits").asLong(0),
                      "misses" to ns.path("misses").asLong(0))
            },
        )
    }

    /**
     * SSE-streaming semantic search. `POST /v1/sem/:token/search/stream`
     * Returns a [Flow] of text chunks from the cached response.
     */
    fun streamSearch(
        prompt: String,
        embedFn: suspend (String) -> DoubleArray,
        namespace: String = "cachly:sem",
        threshold: Double = 0.85,
    ): Flow<String> = flow {
        if (vectorUrl == null) return@flow
        val textForEmbed = normalizePrompt(prompt)
        val embedding = embedFn(textForEmbed)
        val bodyJson = mapper.writeValueAsString(mapOf(
            "embedding" to embedding.toList(), "namespace" to namespace,
            "threshold" to threshold, "prompt" to prompt,
        ))
        val req = HttpRequest.newBuilder()
            .uri(URI.create("$vectorUrl/search/stream"))
            .header("Content-Type", "application/json").header("Accept", "text/event-stream")
            .POST(HttpRequest.BodyPublishers.ofString(bodyJson))
            .timeout(Duration.ofSeconds(60)).build()
        val resp = httpClient.send(req, HttpResponse.BodyHandlers.ofLines())
        resp.body().use { lines ->
            for (line in lines.iterator()) {
                if (!line.startsWith("data:")) continue
                val data = line.removePrefix("data:").trim()
                if (data.isEmpty() || data == "{}") continue
                val node = runCatching { mapper.readTree(data) }.getOrNull() ?: continue
                val text = node.path("text").asText("")
                if (text.isNotEmpty()) emit(text)
            }
        }
    }.flowOn(Dispatchers.IO)

    /** Bulk-index up to 500 entries. `POST /v1/sem/:token/entries/batch` */
    suspend fun batchIndex(entries: List<BatchIndexEntry>): BatchIndexResult = withContext(Dispatchers.IO) {
        require(vectorUrl != null) { "vectorUrl is required for batchIndex" }
        require(entries.size <= 500) { "batchIndex: max 500 entries per request" }
        val r = httpRequestSync("POST", "$vectorUrl/entries/batch",
            mapOf("entries" to entries.map { e ->
                buildMap<String, Any?> {
                    put("id", e.id); put("prompt", e.prompt); put("embedding", e.embedding)
                    put("namespace", e.namespace)
                    if (e.expiresAt != null) put("expires_at", e.expiresAt)
                }
            }))
        BatchIndexResult(indexed = r.path("indexed").asInt(0), skipped = r.path("skipped").asInt(0))
    }

    /** Create a new vector index. `POST /v1/sem/:token/indexes` */
    suspend fun createIndex(
        namespace: String, dimensions: Int = 1536, model: String = "text-embedding-3-small",
        metric: String = "cosine", hybridEnabled: Boolean = false,
    ): JsonNode = withContext(Dispatchers.IO) {
        require(vectorUrl != null) { "vectorUrl is required for createIndex" }
        httpRequestSync("POST", "$vectorUrl/indexes", mapOf(
            "namespace" to namespace, "dimensions" to dimensions,
            "model" to model, "metric" to metric, "hybrid_enabled" to hybridEnabled,
        ))
    }

    /** Delete an index. `DELETE /v1/sem/:token/indexes/:namespace` */
    suspend fun deleteIndex(namespace: String): JsonNode = withContext(Dispatchers.IO) {
        require(vectorUrl != null) { "vectorUrl is required for deleteIndex" }
        httpRequestSync("DELETE", "$vectorUrl/indexes/${URLEncoder.encode(namespace, "UTF-8")}")
    }

    /** Attach JSONB metadata to an entry. `POST /v1/sem/:token/metadata` */
    suspend fun setMetadata(entryId: String, metadata: Map<String, Any>): JsonNode =
        withContext(Dispatchers.IO) {
            require(vectorUrl != null) { "vectorUrl is required for setMetadata" }
            httpRequestSync("POST", "$vectorUrl/metadata",
                mapOf("entry_id" to entryId, "metadata" to metadata))
        }

    /** Semantic search filtered by metadata. `POST /v1/sem/:token/search/filtered` */
    suspend fun filteredSearch(
        prompt: String, embedFn: suspend (String) -> DoubleArray,
        namespace: String = "cachly:sem", threshold: Double = 0.85,
        filter: Map<String, Any> = emptyMap(), limit: Int = 5,
    ): JsonNode = withContext(Dispatchers.IO) {
        require(vectorUrl != null) { "vectorUrl is required for filteredSearch" }
        val embedding = embedFn(normalizePrompt(prompt))
        httpRequestSync("POST", "$vectorUrl/search/filtered", mapOf(
            "prompt" to prompt, "embedding" to embedding.toList(),
            "namespace" to namespace, "threshold" to threshold,
            "filter" to filter, "limit" to limit,
        ))
    }

    /** Configure content-safety guardrails. `POST /v1/sem/:token/guardrails` */
    suspend fun setGuardrail(
        namespace: String = "cachly:sem", piiAction: String = "block",
        toxicAction: String = "flag", toxicThreshold: Double = 0.8,
    ): JsonNode = withContext(Dispatchers.IO) {
        require(vectorUrl != null) { "vectorUrl is required for setGuardrail" }
        httpRequestSync("POST", "$vectorUrl/guardrails", mapOf(
            "namespace" to namespace, "pii_action" to piiAction,
            "toxic_action" to toxicAction, "toxic_threshold" to toxicThreshold,
        ))
    }

    /** Remove guardrail configuration. `DELETE /v1/sem/:token/guardrails/:namespace` */
    suspend fun deleteGuardrail(namespace: String): JsonNode = withContext(Dispatchers.IO) {
        require(vectorUrl != null) { "vectorUrl is required for deleteGuardrail" }
        httpRequestSync("DELETE", "$vectorUrl/guardrails/${URLEncoder.encode(namespace, "UTF-8")}")
    }

    /** Check text against guardrails. `POST /v1/sem/:token/guardrails/check` */
    suspend fun checkGuardrail(text: String, namespace: String = "cachly:sem"): GuardrailCheckResult =
        withContext(Dispatchers.IO) {
            require(vectorUrl != null) { "vectorUrl is required for checkGuardrail" }
            val r = httpRequestSync("POST", "$vectorUrl/guardrails/check",
                mapOf("text" to text, "namespace" to namespace))
            GuardrailCheckResult(
                safe = r.path("safe").asBoolean(true),
                violations = r.path("violations").map { v ->
                    GuardrailViolation(
                        type = v.path("type").asText(""), pattern = v.path("pattern").asText(""),
                        action = v.path("action").asText(""),
                    )
                },
            )
        }

    /** Re-warm from existing entries. `POST /v1/sem/:token/warmup/snapshot` */
    suspend fun snapshotWarmup(namespace: String = "cachly:sem", limit: Int = 100): SnapshotWarmupResult =
        withContext(Dispatchers.IO) {
            require(vectorUrl != null) { "vectorUrl is required for snapshotWarmup" }
            val r = httpRequestSync("POST", "$vectorUrl/warmup/snapshot",
                mapOf("namespace" to namespace, "limit" to limit))
            SnapshotWarmupResult(warmed = r.path("warmed").asInt(0), durationMs = r.path("duration_ms").asLong(0))
        }

    suspend inline fun <reified T : Any> getOrSet(
        prompt: String,
        noinline fn: suspend () -> T,
        noinline embedFn: suspend (String) -> DoubleArray,
        options: SemanticOptions = SemanticOptions(),
    ): SemanticResult<T> = getOrSetImpl(T::class.java, prompt, fn, embedFn, options)

    @PublishedApi
    internal suspend fun <T : Any> getOrSetImpl(
        type: Class<T>, prompt: String, fn: suspend () -> T,
        embedFn: suspend (String) -> DoubleArray, options: SemanticOptions,
    ): SemanticResult<T> {
        val textForEmbed = if (options.normalizePrompt) normalizePrompt(prompt) else prompt
        val resolvedNamespace = if (options.autoNamespace && options.namespace == "cachly:sem")
            detectNamespace(prompt) else options.namespace
        val effectiveOptions = if (options.useAdaptiveThreshold && vectorUrl != null) {
            options.copy(similarityThreshold = adaptiveThreshold(resolvedNamespace), namespace = resolvedNamespace)
        } else options.copy(namespace = resolvedNamespace)

        if (vectorUrl != null) {
            runCatching { return getOrSetViaApi(type, textForEmbed, fn, embedFn, effectiveOptions) }
        }
        return getOrSetViaScan(type, textForEmbed, prompt, fn, embedFn, effectiveOptions)
    }

    // ── §8 Cache-Warming ──────────────────────────────────────────────────────

    data class WarmupEntry<T>(val prompt: String, val fn: suspend () -> T, val namespace: String? = null)
    data class WarmupResult(val warmed: Int, val skipped: Int)

    suspend inline fun <reified T : Any> warmup(
        entries: List<WarmupEntry<T>>,
        noinline embedFn: suspend (String) -> DoubleArray,
        options: SemanticOptions = SemanticOptions(similarityThreshold = 0.98),
    ): WarmupResult {
        var warmed = 0; var skipped = 0
        val warmOpts = if (options.similarityThreshold <= 0) options.copy(similarityThreshold = 0.98) else options
        for (entry in entries) {
            val entryOpts = warmOpts.copy(namespace = entry.namespace ?: warmOpts.namespace)
            runCatching {
                val result = getOrSet(entry.prompt, entry.fn, embedFn, entryOpts)
                if (result.hit) skipped++ else warmed++
            }.onFailure { skipped++ }
        }
        return WarmupResult(warmed, skipped)
    }

    suspend inline fun <reified T : Any> importFromLog(
        filePath: String,
        noinline responseFn: suspend (String) -> T,
        noinline embedFn: suspend (String) -> DoubleArray,
        promptField: String = "prompt",
        batchSize: Int = 50,
        options: SemanticOptions = SemanticOptions(similarityThreshold = 0.98),
    ): WarmupResult {
        val lines = withContext(Dispatchers.IO) {
            java.io.File(filePath).readLines(Charsets.UTF_8)
        }.filter { it.isNotBlank() }
        val localMapper = jacksonObjectMapper()
        var totalWarmed = 0; var totalSkipped = 0
        for (i in lines.indices step batchSize) {
            val batch = lines.subList(i, minOf(i + batchSize, lines.size))
            val entries = batch.mapNotNull { line ->
                try {
                    val obj = localMapper.readTree(line)
                    val prompt = obj.path(promptField).asText("")
                    if (prompt.isEmpty()) null else {
                        val p = prompt
                        WarmupEntry(prompt = p, fn = { responseFn(p) })
                    }
                } catch (_: Exception) { null }
            }
            val result = warmup(entries, embedFn, options)
            totalWarmed += result.warmed; totalSkipped += result.skipped
        }
        return WarmupResult(totalWarmed, totalSkipped)
    }

    // ── Private implementation ─────────────────────────────────────────────────

    private suspend fun <T : Any> getOrSetViaApi(
        type: Class<T>, prompt: String, fn: suspend () -> T,
        embedFn: suspend (String) -> DoubleArray, options: SemanticOptions,
    ): SemanticResult<T> = withContext(Dispatchers.IO) {
        val queryEmbed = embedFn(prompt)

        // §7 – build search body with optional int8 quantization.
        val searchBody: MutableMap<String, Any> = if (options.quantize.equals("int8", ignoreCase = true)) {
            mutableMapOf("embedding_q8" to quantizeEmbedding(queryEmbed),
                "namespace" to options.namespace, "threshold" to options.similarityThreshold)
        } else {
            mutableMapOf("embedding" to queryEmbed.toList(),
                "namespace" to options.namespace, "threshold" to options.similarityThreshold)
        }
        // §3 – hybrid BM25+Vector RRF: include prompt and set hybrid=true.
        if (options.useHybrid && prompt.isNotEmpty()) {
            searchBody["hybrid"] = true; searchBody["prompt"] = prompt
        }
        val searchResp = httpRequestSync("POST", "$vectorUrl/search", searchBody)
        if (searchResp.path("found").asBoolean(false)) {
            val id = searchResp.path("id").asText("")
            val sim = searchResp.path("similarity").asDouble(0.0)
            if (id.isNotEmpty()) {
                jedis.get("${options.namespace}:val:$id")?.let { valRaw ->
                    runCatching {
                        return@withContext SemanticResult(
                            mapper.readValue(valRaw, type), hit = true, similarity = sim,
                            confidence = confidenceBand(sim, options.similarityThreshold, options.highConfidenceThreshold),
                        )
                    }
                }
            }
        }
        // Cache miss.
        val value = fn()
        val id = UUID.randomUUID().toString()
        val vKey = "${options.namespace}:val:$id"
        val valJson = mapper.writeValueAsString(value)
        if (options.ttlSeconds > 0) jedis.setex(vKey, options.ttlSeconds, valJson) else jedis.set(vKey, valJson)

        // §7 – index with quantized or full embedding.
        val indexBody: Map<String, Any> = buildMap {
            put("id", id); put("prompt", prompt); put("namespace", options.namespace)
            if (options.quantize.equals("int8", ignoreCase = true)) put("embedding_q8", quantizeEmbedding(queryEmbed))
            else put("embedding", queryEmbed.toList())
            if (options.ttlSeconds > 0) put("expires_at", Instant.now().plusSeconds(options.ttlSeconds).toString())
        }
        runCatching { httpRequestSync("POST", "$vectorUrl/entries", indexBody) }
        SemanticResult(value, hit = false)
    }

    private suspend fun <T : Any> getOrSetViaScan(
        type: Class<T>, prompt: String, originalPrompt: String, fn: suspend () -> T,
        embedFn: suspend (String) -> DoubleArray, options: SemanticOptions,
    ): SemanticResult<T> = withContext(Dispatchers.IO) {
        val queryEmbed = embedFn(prompt)
        var bestSim = Double.NEGATIVE_INFINITY; var bestValKey: String? = null
        for (embKey in scanAll("${options.namespace}:emb:*")) {
            val raw = jedis.get(embKey) ?: continue
            runCatching {
                val stored = mapper.readTree(raw)["embedding"]?.map { it.asDouble() }?.toDoubleArray() ?: return@runCatching
                val sim = cosineSimilarity(queryEmbed, stored)
                if (sim > bestSim) { bestSim = sim; bestValKey = valKey(embKey) }
            }
        }
        if (bestValKey != null && bestSim >= options.similarityThreshold) {
            jedis.get(bestValKey)?.let { valRaw ->
                runCatching {
                    return@withContext SemanticResult(
                        mapper.readValue(valRaw, type), hit = true, similarity = bestSim,
                        confidence = confidenceBand(bestSim, options.similarityThreshold, options.highConfidenceThreshold),
                    )
                }
            }
        }
        // Cache miss – write val first, then emb.
        val value = fn()
        val id = UUID.randomUUID().toString()
        val embKey = "${options.namespace}:emb:$id"; val vKey = "${options.namespace}:val:$id"
        val embJson = mapper.writeValueAsString(mapOf(
            "embedding" to queryEmbed.toList(), "prompt" to prompt,
            "original_prompt" to originalPrompt.ifEmpty { prompt },
        ))
        val valJson = mapper.writeValueAsString(value)
        if (options.ttlSeconds > 0) {
            jedis.setex(vKey, options.ttlSeconds, valJson); jedis.setex(embKey, options.ttlSeconds, embJson)
        } else { jedis.set(vKey, valJson); jedis.set(embKey, embJson) }
        SemanticResult(value, hit = false)
    }

    /**
     * Remove a single semantic cache entry by its emb key.
     *
     * In API mode: deletes val from Valkey + calls DELETE on the pgvector API.
     * In SCAN mode: deletes both emb and val from Valkey.
     *
     * @return `true` if the entry existed and was deleted
     */
    suspend fun invalidate(key: String): Boolean = withContext(Dispatchers.IO) {
        val id = extractId(key); val ns = extractNamespace(key); val vKey = "$ns:val:$id"
        if (vectorUrl != null) {
            jedis.del(vKey)
            return@withContext runCatching { httpRequestSync("DELETE", "$vectorUrl/entries/$id"); true }.getOrDefault(false)
        }
        jedis.del(key, vKey) > 0
    }

    /**
     * List every cached prompt together with its emb Redis key.
     *
     * In API mode: queries pgvector API, keys have the form `{ns}:emb:{uuid}`.
     * In SCAN mode: scans Valkey emb keys.
     *
     * @return list of `Pair(embKey, prompt)`
     */
    suspend fun entries(namespace: String = "cachly:sem"): List<Pair<String, String>> =
        withContext(Dispatchers.IO) {
            if (vectorUrl != null) {
                runCatching {
                    val resp = httpRequestSync("GET", "$vectorUrl/entries?namespace=${URLEncoder.encode(namespace, "UTF-8")}")
                    resp.get("data")?.mapNotNull { item ->
                        val id = item.path("id").asText(""); val prompt = item.path("prompt").asText("")
                        if (id.isNotEmpty()) "$namespace:emb:$id" to prompt else null
                    } ?: emptyList()
                }.getOrNull()?.let { return@withContext it }
            }
            scanAll("$namespace:emb:*").mapNotNull { embKey ->
                val raw = jedis.get(embKey) ?: return@mapNotNull null
                runCatching {
                    val entry = mapper.readTree(raw)
                    // Prefer original_prompt (un-normalised, for display); fall back to prompt.
                    val prompt = entry["original_prompt"]?.asText("")?.takeIf { it.isNotEmpty() }
                        ?: entry["prompt"]?.asText("") ?: ""
                    embKey to prompt
                }.getOrNull()
            }
        }

    /**
     * Delete all entries in a namespace.
     *
     * In API mode: calls flush endpoint + purges val keys from Valkey.
     * In SCAN mode: deletes all emb and val keys from Valkey.
     *
     * Returns count of logical entries deleted.
     */
    suspend fun flush(namespace: String = "cachly:sem"): Long = withContext(Dispatchers.IO) {
        if (vectorUrl != null) {
            val deleted = runCatching {
                httpRequestSync("DELETE", "$vectorUrl/flush?namespace=${URLEncoder.encode(namespace, "UTF-8")}")
                    .path("deleted").asLong(0)
            }.getOrDefault(0L)
            val valKeys = scanAll("$namespace:val:*").toTypedArray()
            if (valKeys.isNotEmpty()) jedis.del(*valKeys)
            return@withContext deleted
        }
        val embKeys = scanAll("$namespace:emb:*"); val valKeys = scanAll("$namespace:val:*")
        val all = (embKeys + valKeys).toTypedArray()
        if (all.isNotEmpty()) jedis.del(*all)
        embKeys.size.toLong()
    }

    /**
     * Return the number of entries in a namespace.
     *
     * In API mode: queries pgvector API.
     * In SCAN mode: scans Valkey emb keys.
     */
    suspend fun size(namespace: String = "cachly:sem"): Int = withContext(Dispatchers.IO) {
        if (vectorUrl != null) {
            runCatching {
                httpRequestSync("GET", "$vectorUrl/size?namespace=${URLEncoder.encode(namespace, "UTF-8")}")
                    .path("size").asInt(0)
            }.getOrNull()?.let { return@withContext it }
        }
        scanAll("$namespace:emb:*").size
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    internal fun cosineSimilarity(a: DoubleArray, b: DoubleArray): Double {
        val len = minOf(a.size, b.size); var dot = 0.0; var normA = 0.0; var normB = 0.0
        for (i in 0 until len) { dot += a[i] * b[i]; normA += a[i] * a[i]; normB += b[i] * b[i] }
        val denom = sqrt(normA) * sqrt(normB)
        return if (denom == 0.0) 0.0 else dot / denom
    }
}

// ── CachlyClient ──────────────────────────────────────────────────────────────

/**
 * Official Kotlin client for cachly.dev managed Valkey/Redis instances.
 */
class CachlyClient private constructor(
    private val jedis: UnifiedJedis,
    vectorUrl: String? = null,
    private val batchUrl: String? = null,
    private val pubsubUrl: String? = null,
    private val workflowUrl: String? = null,
    private val llmProxyUrl: String? = null,
    private val edgeUrl: String? = null,
    private val edgeApiUrl: String? = null,
) : AutoCloseable {

    private val mapper = jacksonObjectMapper()
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()

    /** Semantic cache helper for AI/LLM workloads (Speed/Business tiers). */
    val semantic = SemanticCache(jedis, vectorUrl)

    /** Pub/Sub client. Obtain via [CachlyClient.pubSub]. */
    val pubSub: PubSubClient? = pubsubUrl?.let { PubSubClient(it, httpClient, mapper) }

    /** Workflow client. Obtain via [CachlyClient.workflow]. */
    val workflow: WorkflowClient? = workflowUrl?.let { WorkflowClient(it, httpClient, mapper) }

    /** Edge Cache client for managing Cloudflare Edge Cache (Feature #5). */
    val edge: EdgeCacheClient? = edgeApiUrl?.let { EdgeCacheClient(it, httpClient, mapper) }

    // ── HTTP helper ───────────────────────────────────────────────────────────

    private fun httpRequestSync(method: String, url: String, body: Any? = null): JsonNode {
        val builder = HttpRequest.newBuilder()
            .uri(URI.create(url)).timeout(Duration.ofSeconds(10))
            .header("Accept", "application/json")

        if (body != null) {
            val json = mapper.writeValueAsString(body)
            builder.header("Content-Type", "application/json")
                   .method(method, HttpRequest.BodyPublishers.ofString(json))
        } else when (method) {
            "DELETE" -> builder.DELETE()
            else     -> builder.method(method, HttpRequest.BodyPublishers.noBody())
        }

        val resp = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() >= 400) {
            error("cachly API $method $url returned ${resp.statusCode()}")
        }
        val responseBody = resp.body()
        return if (responseBody.isNullOrEmpty()) mapper.createObjectNode()
               else mapper.readTree(responseBody)
    }

    // ── Basic operations ──────────────────────────────────────────────────────

    /** Get a value by key. Returns `null` when not found. */
    suspend inline fun <reified T : Any> get(key: String): T? = getImpl(key, T::class.java)

    @PublishedApi
    internal suspend fun <T : Any> getImpl(key: String, type: Class<T>): T? =
        withContext(Dispatchers.IO) { jedis.get(key)?.let { mapper.readValue(it, type) } }

    /**
     * Set a value as JSON.
     * @param ttlSeconds pass > 0 for automatic expiry
     */
    suspend fun set(key: String, value: Any, ttlSeconds: Long = 0) = withContext(Dispatchers.IO) {
        val json = mapper.writeValueAsString(value)
        if (ttlSeconds > 0) jedis.setex(key, ttlSeconds, json) else jedis.set(key, json)
    }

    /** Delete one or more keys. Returns number of keys deleted. */
    suspend fun del(vararg keys: String): Long = withContext(Dispatchers.IO) { jedis.del(*keys) }

    /** Check whether a key exists. */
    suspend fun exists(key: String): Boolean = withContext(Dispatchers.IO) { jedis.exists(key) }

    /** Update TTL for an existing key. */
    suspend fun expire(key: String, seconds: Long) = withContext(Dispatchers.IO) { jedis.expire(key, seconds) }

    /** Return the remaining TTL for a key in seconds. Returns `-1` if no expiry, `-2` if key not found. */
    suspend fun ttl(key: String): Long = withContext(Dispatchers.IO) { jedis.ttl(key) }

    /** Check connectivity to the cache server. Returns `true` if server is reachable. */
    suspend fun ping(): Boolean = withContext(Dispatchers.IO) {
        try { "PONG".equals(jedis.ping(), ignoreCase = true) } catch (_: Exception) { false }
    }

    /** Atomic counter increment. Returns the new value. */
    suspend fun incr(key: String): Long = withContext(Dispatchers.IO) { jedis.incr(key) }

    /** Get-or-set: return cached value or call [fn], cache and return result. */
    suspend inline fun <reified T : Any> getOrSet(
        key: String,
        ttlSeconds: Long = 0,
        noinline fn: suspend () -> T,
    ): T {
        getImpl(key, T::class.java)?.let { return it }
        val value = fn(); set(key, value, ttlSeconds); return value
    }

    // ── Bulk operations ───────────────────────────────────────────────────────

    /** One item in a bulk [mset] call. */
    data class MSetItem(val key: String, val value: Any, val ttlSeconds: Long = 0)

    /**
     * Set multiple key-value pairs in a single pipeline round-trip.
     * Supports per-key TTL – unlike native MSET which has no expiry option.
     *
     * ```kotlin
     * cache.mset(listOf(
     *     CachlyClient.MSetItem("user:1", User("Alice"), 300),
     *     CachlyClient.MSetItem("user:2", User("Bob")),
     * ))
     * ```
     */
    suspend fun mset(items: List<MSetItem>) = withContext(Dispatchers.IO) {
        if (items.isEmpty()) return@withContext
        jedis.pipelined().use { pipe ->
            for (item in items) {
                val json = mapper.writeValueAsString(item.value)
                if (item.ttlSeconds > 0) pipe.setex(item.key, item.ttlSeconds, json) else pipe.set(item.key, json)
            }
            pipe.sync()
        }
    }

    /**
     * Retrieve multiple keys in one round-trip (native MGET).
     * Returns a list in the same order as [keys]; missing keys return null.
     *
     * ```kotlin
     * val (alice, bob) = cache.mget<User>(listOf("user:1", "user:2"))
     * ```
     */
    suspend inline fun <reified T : Any> mget(keys: List<String>): List<T?> = mgetImpl(keys, T::class.java)

    @PublishedApi
    internal suspend fun <T : Any> mgetImpl(keys: List<String>, type: Class<T>): List<T?> = withContext(Dispatchers.IO) {
        if (keys.isEmpty()) return@withContext emptyList()
        jedis.mget(*keys.toTypedArray()).map { raw ->
            if (raw == null) null else mapper.readValue(raw, type)
        }
    }

    // ── Tag-based invalidation ────────────────────────────────────────────────

    /** Associate a cache key with tags. `POST /v1/cache/:token/tags` */
    suspend fun setTags(key: String, tags: List<String>): TagsResult = withContext(Dispatchers.IO) {
        require(batchUrl != null) { "batchUrl is required for setTags" }
        val r = httpRequestSync("POST", "$batchUrl/tags", mapOf("key" to key, "tags" to tags))
        TagsResult(key = r.path("key").asText(key), tags = r.path("tags").map { it.asText() },
            ok = r.path("ok").asBoolean(true))
    }

    /** Delete all keys with given tag. `POST /v1/cache/:token/invalidate` */
    suspend fun invalidateTag(tag: String): InvalidateTagResult = withContext(Dispatchers.IO) {
        require(batchUrl != null) { "batchUrl is required for invalidateTag" }
        val r = httpRequestSync("POST", "$batchUrl/invalidate", mapOf("tag" to tag))
        InvalidateTagResult(tag = r.path("tag").asText(tag), keysDeleted = r.path("keys_deleted").asInt(0),
            keys = r.path("keys").map { it.asText() }, durationMs = r.path("duration_ms").asLong(0))
    }

    /** Get tags for a key. `GET /v1/cache/:token/tags/:key` */
    suspend fun getTags(key: String): TagsResult = withContext(Dispatchers.IO) {
        require(batchUrl != null) { "batchUrl is required for getTags" }
        val r = httpRequestSync("GET", "$batchUrl/tags/${URLEncoder.encode(key, "UTF-8")}")
        TagsResult(key = r.path("key").asText(key), tags = r.path("tags").map { it.asText() })
    }

    /** Remove all tag associations for a key. `DELETE /v1/cache/:token/tags/:key` */
    suspend fun deleteTags(key: String): JsonNode = withContext(Dispatchers.IO) {
        require(batchUrl != null) { "batchUrl is required for deleteTags" }
        httpRequestSync("DELETE", "$batchUrl/tags/${URLEncoder.encode(key, "UTF-8")}")
    }

    // ── Stale-While-Revalidate (SWR) ─────────────────────────────────────────

    /** Register a key for SWR. `POST /v1/cache/:token/swr/register` */
    suspend fun swrRegister(
        key: String, ttlSeconds: Long, staleWindowSeconds: Long, fetcherHint: String? = null,
    ): JsonNode = withContext(Dispatchers.IO) {
        require(batchUrl != null) { "batchUrl is required for swrRegister" }
        httpRequestSync("POST", "$batchUrl/swr/register", buildMap {
            put("key", key); put("ttl_seconds", ttlSeconds); put("stale_window_seconds", staleWindowSeconds)
            if (fetcherHint != null) put("fetcher_hint", fetcherHint)
        })
    }

    /** Query stale keys. `POST /v1/cache/:token/swr/check` */
    suspend fun swrCheck(): SwrCheckResult = withContext(Dispatchers.IO) {
        require(batchUrl != null) { "batchUrl is required for swrCheck" }
        val r = httpRequestSync("POST", "$batchUrl/swr/check", emptyMap<String, Any>())
        val staleKeys = r.path("stale_keys").map { k ->
            SwrEntry(
                key = k.path("key").asText(""),
                fetcherHint = k.path("fetcher_hint").asText("").takeIf { it.isNotEmpty() },
                staleFor = k.path("stale_for").asText("").takeIf { it.isNotEmpty() },
                refreshAt = k.path("refresh_at").asText("").takeIf { it.isNotEmpty() },
            )
        }
        SwrCheckResult(staleKeys = staleKeys, count = r.path("count").asInt(staleKeys.size),
            checkedAt = r.path("checked_at").asText(""))
    }

    /** Remove a key from the SWR registry. `DELETE /v1/cache/:token/swr/:key` */
    suspend fun swrRemove(key: String): JsonNode = withContext(Dispatchers.IO) {
        require(batchUrl != null) { "batchUrl is required for swrRemove" }
        httpRequestSync("DELETE", "$batchUrl/swr/${URLEncoder.encode(key, "UTF-8")}")
    }

    // ── Bulk Warmup ───────────────────────────────────────────────────────────

    data class BulkWarmupEntry(val key: String, val value: String, val ttl: Long = 0)

    /** Bulk-warm the KV cache. `POST /v1/cache/:token/warm` */
    suspend fun bulkWarmup(entries: List<BulkWarmupEntry>): BulkWarmupResult = withContext(Dispatchers.IO) {
        require(batchUrl != null) { "batchUrl is required for bulkWarmup" }
        val r = httpRequestSync("POST", "$batchUrl/warm",
            mapOf("entries" to entries.map { e ->
                buildMap<String, Any> {
                    put("key", e.key); put("value", e.value)
                    if (e.ttl > 0) put("ttl", e.ttl)
                }
            }))
        BulkWarmupResult(warmed = r.path("warmed").asInt(0), skipped = r.path("skipped").asInt(0),
            durationMs = r.path("duration_ms").asLong(0))
    }

    // ── LLM Proxy Stats ───────────────────────────────────────────────────────

    /** `GET /v1/llm-proxy/:token/stats` */
    suspend fun llmProxyStats(): LlmProxyStatsResult = withContext(Dispatchers.IO) {
        require(llmProxyUrl != null) { "llmProxyUrl is required for llmProxyStats" }
        val r = httpRequestSync("GET", "$llmProxyUrl/stats")
        LlmProxyStatsResult(
            totalRequests = r.path("total_requests").asLong(0),
            cacheHits = r.path("cache_hits").asLong(0),
            cacheMisses = r.path("cache_misses").asLong(0),
            estimatedSavedUsd = r.path("estimated_saved_usd").asDouble(0.0),
            avgLatencyMsCached = r.path("avg_latency_ms_cached").asLong(0),
            avgLatencyMsUncached = r.path("avg_latency_ms_uncached").asLong(0),
        )
    }

    // ── Distributed lock ──────────────────────────────────────────────────────

    /**
     * Handle returned by a successful [lock] call.
     * Call [release] in a try/finally block to free the lock early.
     */
    inner class LockHandle internal constructor(
        private val lockKey: String,
        val token: String,
    ) : AutoCloseable {
        private val releaseScript = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
        """.trimIndent()

        /** Release the lock atomically. No-op if already expired or released. */
        suspend fun release() = withContext(Dispatchers.IO) {
            jedis.eval(releaseScript, listOf(lockKey), listOf(token))
        }

        override fun close() {
            runBlocking { release() }
        }
    }

    /**
     * Acquire a distributed lock using Redis SET NX PX (Redlock-lite pattern).
     *
     * Returns a [LockHandle] on success, or null when all attempts are exhausted.
     * The lock auto-expires after [ttlMs] to prevent deadlocks.
     *
     * ```kotlin
     * val lock = cache.lock("job:invoice:42", ttlMs = 5000, retries = 5)
     *     ?: throw IllegalStateException("Resource busy")
     * lock.use { processInvoice() }
     * ```
     */
    suspend fun lock(
        key: String,
        ttlMs: Long,
        retries: Int = 3,
        retryDelayMs: Long = 50,
    ): LockHandle? = withContext(Dispatchers.IO) {
        val lockKey = "cachly:lock:$key"; val token = UUID.randomUUID().toString()
        repeat(retries + 1) { attempt ->
            val result = jedis.set(lockKey, token,
                redis.clients.jedis.params.SetParams.setParams().nx().px(ttlMs))
            if (result == "OK") return@withContext LockHandle(lockKey, token)
            if (attempt < retries) Thread.sleep(retryDelayMs)
        }
        null
    }

    // ── Batch API ─────────────────────────────────────────────────────────────

    /** A single operation in a [batch] call. */
    data class BatchOp(val op: String, val key: String, val value: String? = null, val ttl: Long? = null) {
        companion object {
            fun get(key: String) = BatchOp("get", key)
            fun set(key: String, value: String, ttl: Long? = null) = BatchOp("set", key, value, ttl)
            fun del(key: String) = BatchOp("del", key)
            fun exists(key: String) = BatchOp("exists", key)
            fun ttl(key: String) = BatchOp("ttl", key)
        }
    }

    /** Result of a single operation in a [batch] call. */
    data class BatchOpResult(
        /** Value for "get" ops. `null` = key not found. */
        val value: String? = null,
        /** `true` if a "get" key existed. */
        val found: Boolean = false,
        /** `true` for successful "set" and "del" (≥1 key deleted). */
        val ok: Boolean = false,
        /** Result of "exists" ops. */
        val exists: Boolean = false,
        /** Remaining TTL in seconds for "ttl" ops. -1 = no expiry, -2 = not found. */
        val ttlSeconds: Long = -2,
        /** Per-op error message returned by the server (non-fatal). */
        val error: String? = null,
    )

    /**
     * Execute multiple cache operations in a **single round-trip**.
     *
     * When `batchUrl` is configured (pass to [connect]), all ops are sent via
     * `POST {batchUrl}/batch` (one HTTP request).
     * Otherwise they fall back to a Jedis pipeline.
     *
     * ```kotlin
     * val results = cache.batch(listOf(
     *     CachlyClient.BatchOp.get("user:1"),
     *     CachlyClient.BatchOp.get("config:app"),
     *     CachlyClient.BatchOp.set("visits", System.currentTimeMillis().toString(), ttl = 86400),
     * ))
     * val user   = results[0].value    // String?
     * val config = results[1].value    // String?
     * val ok     = results[2].ok       // Boolean
     * ```
     */
    suspend fun batch(ops: List<BatchOp>): List<BatchOpResult> {
        if (ops.isEmpty()) return emptyList()
        return if (batchUrl != null) batchViaHttp(ops, batchUrl) else batchViaPipeline(ops)
    }

    private suspend fun batchViaHttp(ops: List<BatchOp>, baseUrl: String): List<BatchOpResult> =
        withContext(Dispatchers.IO) {
            val resp = httpRequestSync("POST", "${baseUrl.trimEnd('/')}/batch",
                mapOf("ops" to ops.map { o ->
                    buildMap { put("op", o.op); put("key", o.key)
                        if (o.value != null) put("value", o.value)
                        if (o.ttl != null) put("ttl", o.ttl) }
                }))
            val results = resp["results"] ?: return@withContext ops.map { BatchOpResult() }
            ops.mapIndexed { i, op ->
                val r = results[i] ?: return@mapIndexed BatchOpResult()
                val errMsg = r["error"]?.asText()?.takeIf { it.isNotEmpty() }
                if (errMsg != null) return@mapIndexed BatchOpResult(error = errMsg)
                when (op.op) {
                    "get"    -> BatchOpResult(value = r["value"]?.asText(), found = r["found"]?.asBoolean() ?: false)
                    "set"    -> BatchOpResult(ok = r["ok"]?.asBoolean() ?: false)
                    "del"    -> BatchOpResult(ok = (r["deleted"]?.asLong() ?: 0L) > 0)
                    "exists" -> BatchOpResult(exists = r["exists"]?.asBoolean() ?: false)
                    "ttl"    -> BatchOpResult(ttlSeconds = r["ttl_seconds"]?.asLong() ?: -2L)
                    else     -> BatchOpResult()
                }
            }
        }

    private suspend fun batchViaPipeline(ops: List<BatchOp>): List<BatchOpResult> =
        withContext(Dispatchers.IO) {
            val responseList = mutableListOf<redis.clients.jedis.Response<*>>()
            jedis.pipelined().use { pipe ->
                for (op in ops) {
                    val resp: redis.clients.jedis.Response<*>? = when (op.op) {
                        "get"    -> pipe.get(op.key)
                        "set"    -> if (op.ttl != null && op.ttl > 0) pipe.setex(op.key, op.ttl, op.value ?: "")
                                    else pipe.set(op.key, op.value ?: "")
                        "del"    -> pipe.del(op.key)
                        "exists" -> pipe.exists(op.key)
                        "ttl"    -> pipe.ttl(op.key)
                        else     -> null
                    }
                    if (resp != null) responseList.add(resp)
                }
                pipe.sync()
            }
            ops.mapIndexed { i, op ->
                val resp = responseList.getOrNull(i)
                val v = resp?.get()
                when (op.op) {
                    "get"    -> { val s = v as? String; BatchOpResult(value = s, found = s != null) }
                    "set"    -> BatchOpResult(ok = v == "OK")
                    "del"    -> BatchOpResult(ok = (v as? Long ?: 0L) > 0)
                    "exists" -> BatchOpResult(exists = (v as? Boolean) ?: ((v as? Long ?: 0L) > 0))
                    "ttl"    -> BatchOpResult(ttlSeconds = v as? Long ?: -2L)
                    else     -> BatchOpResult()
                }
            }
        }

    // ── Streaming cache ───────────────────────────────────────────────────────

    /**
     * Cache a streaming response chunk-by-chunk via Redis RPUSH.
     * Replay with [streamGet].
     *
     * ```kotlin
     * cache.streamSet("chat:42", tokenChunks, ttlSeconds = 3600)
     * ```
     */
    suspend fun streamSet(key: String, chunks: Iterable<String>, ttlSeconds: Long = 0) =
        withContext(Dispatchers.IO) {
            val listKey = "cachly:stream:$key"; jedis.del(listKey)
            for (chunk in chunks) jedis.rpush(listKey, chunk)
            if (ttlSeconds > 0) jedis.expire(listKey, ttlSeconds)
        }

    /**
     * Retrieve a cached stream as a [List][String].
     * Returns null on cache miss (key absent or empty list).
     *
     * ```kotlin
     * val cached = cache.streamGet("chat:42")
     * if (cached != null) cached.forEach { print(it) }
     * ```
     */
    suspend fun streamGet(key: String): List<String>? = withContext(Dispatchers.IO) {
        val listKey = "cachly:stream:$key"; val len = jedis.llen(listKey)
        if (len == 0L) null else jedis.lrange(listKey, 0, -1)
    }

    /** Direct access to the underlying Jedis client. */
    fun raw(): UnifiedJedis = jedis

    override fun close() = jedis.close()

    companion object {
        /**
         * Connect to a cachly instance.
         *
         * @param url       Redis URL: `redis://:password@host:port`
         * @param vectorUrl Optional pgvector API URL: `https://api.cachly.dev/v1/sem/{token}`
         *                  When set, [semantic] uses HNSW O(log n) with graceful SCAN fallback.
         * @param batchUrl  Optional KV Batch API URL: `https://api.cachly.dev/v1/cache/{token}`
         *                  When set, [batch] sends all ops in one HTTP request.
         * @param edgeUrl   Optional Edge Worker URL for semantic search reads (sub-1ms latency)
         * @param edgeApiUrl Optional Edge Cache Management API URL for config, purge, stats
         */
        fun connect(
            url: String,
            vectorUrl: String? = null,
            batchUrl: String? = null,
            pubsubUrl: String? = null,
            workflowUrl: String? = null,
            llmProxyUrl: String? = null,
            edgeUrl: String? = null,
            edgeApiUrl: String? = null,
        ): CachlyClient = CachlyClient(
            jedis = redis.clients.jedis.JedisPooled(java.net.URI.create(url)),
            vectorUrl = vectorUrl, batchUrl = batchUrl, pubsubUrl = pubsubUrl,
            workflowUrl = workflowUrl, llmProxyUrl = llmProxyUrl,
            edgeUrl = edgeUrl, edgeApiUrl = edgeApiUrl,
        )
    }
}

// ── PubSubClient ──────────────────────────────────────────────────────────────

/** Pub/Sub client. Obtain via [CachlyClient.pubSub]. */
class PubSubClient internal constructor(
    private val pubsubUrl: String,
    private val httpClient: HttpClient,
    private val mapper: com.fasterxml.jackson.databind.ObjectMapper,
) {
    private fun httpSync(method: String, url: String, body: Any? = null): JsonNode {
        val builder = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10))
            .header("Accept", "application/json")
        if (body != null) {
            builder.header("Content-Type", "application/json")
                   .method(method, HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
        } else { builder.method(method, HttpRequest.BodyPublishers.noBody()) }
        val resp = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() >= 400) error("cachly pubsub $method $url → ${resp.statusCode()}")
        val rb = resp.body()
        return if (rb.isNullOrEmpty()) mapper.createObjectNode() else mapper.readTree(rb)
    }

    /** Publish a message. `POST /v1/pubsub/:token/publish` */
    suspend fun publish(channel: String, message: String): JsonNode = withContext(Dispatchers.IO) {
        httpSync("POST", "$pubsubUrl/publish", mapOf("channel" to channel, "message" to message))
    }

    /** Subscribe via SSE. `POST /v1/pubsub/:token/subscribe` */
    fun subscribe(channels: List<String>): Flow<PubSubMessage> = flow {
        val bodyJson = mapper.writeValueAsString(mapOf("channels" to channels))
        val req = HttpRequest.newBuilder().uri(URI.create("$pubsubUrl/subscribe"))
            .header("Content-Type", "application/json").header("Accept", "text/event-stream")
            .POST(HttpRequest.BodyPublishers.ofString(bodyJson)).timeout(Duration.ofSeconds(300)).build()
        httpClient.send(req, HttpResponse.BodyHandlers.ofLines()).body().use { lines ->
            for (line in lines.iterator()) {
                if (!line.startsWith("data:")) continue
                val data = line.removePrefix("data:").trim()
                if (data.isEmpty() || data == "{}") continue
                val node = runCatching { mapper.readTree(data) }.getOrNull() ?: continue
                emit(PubSubMessage(channel = node.path("channel").asText(""),
                    message = node.path("message").asText(""), at = node.path("at").asText("")))
            }
        }
    }.flowOn(Dispatchers.IO)

    /** List active channels. `GET /v1/pubsub/:token/channels` */
    suspend fun channels(): JsonNode = withContext(Dispatchers.IO) { httpSync("GET", "$pubsubUrl/channels") }

    /** Pub/Sub statistics. `GET /v1/pubsub/:token/stats` */
    suspend fun stats(): JsonNode = withContext(Dispatchers.IO) { httpSync("GET", "$pubsubUrl/stats") }
}

// ── WorkflowClient ────────────────────────────────────────────────────────────

/** Workflow checkpoint client. Obtain via [CachlyClient.workflow]. */
class WorkflowClient internal constructor(
    private val workflowUrl: String,
    private val httpClient: HttpClient,
    private val mapper: com.fasterxml.jackson.databind.ObjectMapper,
) {
    private fun httpSync(method: String, url: String, body: Any? = null): JsonNode {
        val builder = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10))
            .header("Accept", "application/json")
        if (body != null) {
            builder.header("Content-Type", "application/json")
                   .method(method, HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
        } else when (method) {
            "DELETE" -> builder.DELETE()
            else     -> builder.method(method, HttpRequest.BodyPublishers.noBody())
        }
        val resp = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() >= 400) error("cachly workflow $method $url → ${resp.statusCode()}")
        val rb = resp.body()
        return if (rb.isNullOrEmpty()) mapper.createObjectNode() else mapper.readTree(rb)
    }

    private fun JsonNode.toCheckpoint() = WorkflowCheckpoint(
        id = path("id").asText(""), runId = path("run_id").asText(""),
        stepIndex = path("step_index").asInt(0), stepName = path("step_name").asText(""),
        agentName = path("agent_name").asText(""), status = path("status").asText(""),
        state = path("state").asText("").takeIf { it.isNotEmpty() },
        output = path("output").asText("").takeIf { it.isNotEmpty() },
        durationMs = path("duration_ms").asLong(0).takeIf { it > 0 },
        createdAt = path("created_at").asText("").takeIf { it.isNotEmpty() },
    )

    private fun JsonNode.toRun(): WorkflowRun {
        val cps = path("checkpoints").map { it.toCheckpoint() }
        return WorkflowRun(runId = path("run_id").asText(""), steps = path("steps").asInt(cps.size),
            latestStatus = path("latest_status").asText(""), checkpoints = cps)
    }

    /** Save a checkpoint. `POST /v1/workflow/:token/checkpoints` */
    suspend fun saveCheckpoint(
        runId: String, stepIndex: Int, stepName: String, agentName: String, status: String,
        state: String? = null, output: String? = null, durationMs: Long? = null,
    ): WorkflowCheckpoint = withContext(Dispatchers.IO) {
        httpSync("POST", "$workflowUrl/checkpoints", buildMap {
            put("run_id", runId); put("step_index", stepIndex); put("step_name", stepName)
            put("agent_name", agentName); put("status", status)
            if (state != null) put("state", state)
            if (output != null) put("output", output)
            if (durationMs != null) put("duration_ms", durationMs)
        }).toCheckpoint()
    }

    /** List all runs. `GET /v1/workflow/:token/runs` */
    suspend fun listRuns(): List<WorkflowRun> = withContext(Dispatchers.IO) {
        httpSync("GET", "$workflowUrl/runs").path("runs").map { it.toRun() }
    }

    /** Get checkpoints for a run. `GET /v1/workflow/:token/runs/:runId` */
    suspend fun getRun(runId: String): WorkflowRun = withContext(Dispatchers.IO) {
        httpSync("GET", "$workflowUrl/runs/${URLEncoder.encode(runId, "UTF-8")}").toRun()
    }

    /** Get the latest checkpoint. `GET /v1/workflow/:token/runs/:runId/latest` */
    suspend fun latestCheckpoint(runId: String): WorkflowCheckpoint = withContext(Dispatchers.IO) {
        httpSync("GET", "$workflowUrl/runs/${URLEncoder.encode(runId, "UTF-8")}/latest").toCheckpoint()
    }

    /** Delete a run. `DELETE /v1/workflow/:token/runs/:runId` */
    suspend fun deleteRun(runId: String): JsonNode = withContext(Dispatchers.IO) {
        httpSync("DELETE", "$workflowUrl/runs/${URLEncoder.encode(runId, "UTF-8")}")
    }
}

// ── EdgeCacheClient (Feature #5) ──────────────────────────────────────────────

/**
 * Edge Cache client for managing Cloudflare Edge Cache.
 * Obtain via [CachlyClient.edge].
 *
 * Example:
 * ```kotlin
 * val stats = cache.edge?.stats()
 * println("Hit rate: ${stats?.hitRate?.times(100)}%")
 *
 * cache.edge?.setConfig(EdgeCacheConfigUpdate(enabled = true, edgeTtl = 120))
 * cache.edge?.purge(EdgePurgeOptions(namespaces = listOf("cachly:sem:qa")))
 * ```
 */
class EdgeCacheClient internal constructor(
    private val edgeApiUrl: String,
    private val httpClient: HttpClient,
    private val mapper: com.fasterxml.jackson.databind.ObjectMapper,
) {
    private val baseUrl = edgeApiUrl.trimEnd('/')

    private fun httpSync(method: String, url: String, body: Any? = null): JsonNode {
        val builder = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10))
            .header("Accept", "application/json")
        if (body != null) {
            builder.header("Content-Type", "application/json")
                   .method(method, HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
        } else when (method) {
            "DELETE" -> builder.DELETE()
            else     -> builder.method(method, HttpRequest.BodyPublishers.noBody())
        }
        val resp = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() >= 400) error("cachly edge $method $url → ${resp.statusCode()}: ${resp.body()}")
        val rb = resp.body()
        return if (rb.isNullOrEmpty()) mapper.createObjectNode() else mapper.readTree(rb)
    }

    /** Get current edge cache configuration. `GET /config` */
    suspend fun getConfig(): EdgeCacheConfig = withContext(Dispatchers.IO) {
        val r = httpSync("GET", "$baseUrl/config")
        EdgeCacheConfig(
            id = r.path("id").asText(""),
            instanceId = r.path("instance_id").asText(""),
            enabled = r.path("enabled").asBoolean(false),
            edgeTtl = r.path("edge_ttl").asInt(0),
            workerUrl = r.path("worker_url").asText(""),
            cloudflareZoneId = r.path("cloudflare_zone_id").asText(""),
            purgeOnWrite = r.path("purge_on_write").asBoolean(false),
            cacheSearchResults = r.path("cache_search_results").asBoolean(false),
            totalHits = r.path("total_hits").asLong(0),
            totalMisses = r.path("total_misses").asLong(0),
            hitRate = r.path("hit_rate").asDouble(0.0),
        )
    }

    /** Update edge cache configuration. `PUT /config` */
    suspend fun setConfig(update: EdgeCacheConfigUpdate): EdgeCacheConfig = withContext(Dispatchers.IO) {
        val body = buildMap<String, Any?> {
            if (update.enabled != null) put("enabled", update.enabled)
            if (update.edgeTtl != null) put("edge_ttl", update.edgeTtl)
            if (update.workerUrl != null) put("worker_url", update.workerUrl)
            if (update.cloudflareZoneId != null) put("cloudflare_zone_id", update.cloudflareZoneId)
            if (update.purgeOnWrite != null) put("purge_on_write", update.purgeOnWrite)
            if (update.cacheSearchResults != null) put("cache_search_results", update.cacheSearchResults)
        }
        val r = httpSync("PUT", "$baseUrl/config", body)
        EdgeCacheConfig(
            id = r.path("id").asText(""),
            instanceId = r.path("instance_id").asText(""),
            enabled = r.path("enabled").asBoolean(false),
            edgeTtl = r.path("edge_ttl").asInt(0),
            workerUrl = r.path("worker_url").asText(""),
            cloudflareZoneId = r.path("cloudflare_zone_id").asText(""),
            purgeOnWrite = r.path("purge_on_write").asBoolean(false),
            cacheSearchResults = r.path("cache_search_results").asBoolean(false),
            totalHits = r.path("total_hits").asLong(0),
            totalMisses = r.path("total_misses").asLong(0),
            hitRate = r.path("hit_rate").asDouble(0.0),
        )
    }

    /** Delete edge cache configuration (disables edge caching). `DELETE /config` */
    suspend fun deleteConfig(): Unit = withContext(Dispatchers.IO) {
        httpSync("DELETE", "$baseUrl/config")
    }

    /** Purge cached entries from Cloudflare CDN. `POST /purge` */
    suspend fun purge(options: EdgePurgeOptions? = null): EdgePurgeResult = withContext(Dispatchers.IO) {
        val body = buildMap<String, Any?> {
            if (options?.namespaces != null) put("namespaces", options.namespaces)
            if (options?.urls != null) put("urls", options.urls)
        }
        val r = httpSync("POST", "$baseUrl/purge", body)
        EdgePurgeResult(
            purged = r.path("purged").asInt(0),
            urls = r.path("urls").map { it.asText() },
        )
    }

    /** Get edge cache hit/miss statistics. `GET /stats` */
    suspend fun stats(): EdgeCacheStats = withContext(Dispatchers.IO) {
        val r = httpSync("GET", "$baseUrl/stats")
        EdgeCacheStats(
            enabled = r.path("enabled").asBoolean(false),
            workerUrl = r.path("worker_url").asText(""),
            edgeTtl = r.path("edge_ttl").asInt(0),
            totalHits = r.path("total_hits").asLong(0),
            totalMisses = r.path("total_misses").asLong(0),
            hitRate = r.path("hit_rate").asDouble(0.0),
        )
    }
}

