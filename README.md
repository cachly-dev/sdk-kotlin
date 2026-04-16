# cachly Kotlin SDK

Official Kotlin SDK for [cachly.dev](https://cachly.dev) – Managed Valkey/Redis cache.

**DSGVO-compliant · German servers · 30s provisioning**  
**Coroutine-native · Ktor & Spring Boot ready**

## Installation

```kotlin
// build.gradle.kts
implementation("dev.cachly:cachly-kotlin:0.1.0-beta.1")
```

> Requires Kotlin 2.0+ and Java 17+. Uses Jedis 5, Kotlin Coroutines, Jackson.

## Quick Start

```kotlin
import dev.cachly.CachlyClient

val cache = CachlyClient.connect(System.getenv("CACHLY_URL"))

// Set with TTL
cache.set("user:42", User("Alice", "pro"), ttlSeconds = 300)

// Get
val user: User? = cache.get("user:42")

// Exists / delete
cache.exists("user:42")
cache.del("user:42")

// Atomic counter
val count: Long = cache.incr("page:views")

cache.close()
```

## Get-or-Set Pattern

```kotlin
val report: Report = cache.getOrSet("report:monthly", ttlSeconds = 60) {
    db.runExpensiveReport()   // only called on cache miss
}
```

## Semantic AI Cache (Speed / Business tiers)

```kotlin
import dev.cachly.SemanticOptions

val result = cache.semantic.getOrSet<String>(
    prompt = userQuestion,
    fn = { openAi.ask(userQuestion) },
    embedFn = { text -> openAi.embed(text) },
    options = SemanticOptions(
        similarityThreshold = 0.92,
        ttlSeconds = 3600,
    ),
)

println(if (result.hit) "⚡ hit (sim=${result.similarity})" else "🔄 miss")
println(result.value)
```

## Ktor Integration

```kotlin
fun Application.configureCache() {
    val cache = CachlyClient.connect(environment.config.property("cachly.url").getString())
    environment.monitor.subscribe(ApplicationStopped) { cache.close() }
    dependencies { single { cache } }
}

// In a route handler:
val cache: CachlyClient by inject()
val data: MyData = cache.getOrSet("key", ttlSeconds = 60) { service.fetchData() }
```

## Spring Boot Integration (Kotlin)

```kotlin
@Configuration
class CachlyConfig(@Value("\${cachly.url}") private val url: String) {
    @Bean(destroyMethod = "close")
    fun cachlyClient() = CachlyClient.connect(url)
}
```

## API Reference

| Method | Description |
|---|---|
| `connect(url)` | Factory – create client from Redis URL |
| `get<T>(key)` | Suspend – get value (`null` if not found) |
| `set(key, value, ttlSeconds)` | Suspend – set value |
| `del(vararg keys)` | Suspend – delete keys |
| `exists(key): Boolean` | Suspend – check existence |
| `expire(key, seconds)` | Suspend – update TTL |
| `incr(key): Long` | Suspend – atomic increment |
| `getOrSet<T>(key, ttlSeconds, fn)` | Suspend – get-or-set pattern |
| `semantic` | `SemanticCache` helper for AI workloads |
| `raw()` | Direct `UnifiedJedis` access |

## Batch API – mehrere Ops in einem Round-Trip

Bündelt GET/SET/DEL/EXISTS/TTL-Ops in **einem** HTTP-Request oder einer Jedis-Pipeline.

```kotlin
val cache = CachlyClient.builder()
    .url(System.getenv("CACHLY_URL"))
    .batchUrl(System.getenv("CACHLY_BATCH_URL")) // optional
    .build()

val results = cache.batch(listOf(
    BatchOp.get("user:1"),
    BatchOp.get("config:app"),
    BatchOp.set("visits", "42", ttlSeconds = 86400),
    BatchOp.exists("session:xyz"),
    BatchOp.ttl("token:abc"),
))

val user  : String? = results[0].value      // null on miss
val ok    : Boolean = results[2].ok
val found : Boolean = results[3].exists
val secs  : Long    = results[4].ttlSeconds  // -1 = kein TTL, -2 = nicht vorhanden
```

## Environment Variables

```bash
CACHLY_URL=redis://:your-password@my-app.cachly.dev:30101
CACHLY_BATCH_URL=https://api.cachly.dev/v1/cache/YOUR_TOKEN   # optional
# Speed / Business tier – Semantic AI Cache:
CACHLY_VECTOR_URL=https://api.cachly.dev/v1/sem/your-vector-token
```

Find both values in your [cachly.dev dashboard](https://cachly.dev/instances).

## License


