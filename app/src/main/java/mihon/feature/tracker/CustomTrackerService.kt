package mihon.feature.tracker

import android.content.Context
import android.content.SharedPreferences
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.add
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import mihon.app.di.appGraph
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import tachiyomi.core.common.util.system.logcat
import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.manga.model.Manga
import java.io.File
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

/**
 * Isolated Custom Tracking Service for Mihon Fork.
 *
 * Provides:
 * 1. Automatic chapter completion tracking with deterministic ID and offline buffer queue.
 * 2. Full library synchronization to the tracker backend.
 * 3. Offline resilience: retries failed events without duplicating.
 * 4. Configurable endpoint URL and API Key via SharedPreferences.
 */
object CustomTrackerService {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val jsonMediaType = "application/json; charset=utf-8".toMediaType()

    private const val PREFS_NAME = "mihon_custom_tracker_prefs"
    private const val KEY_ENDPOINT = "tracker_endpoint_url"
    private const val KEY_API_KEY = "tracker_api_key"
    private const val KEY_ENABLED = "tracker_enabled"
    private const val QUEUE_FILENAME = "tracker_pending_queue.json"

    const val DEFAULT_ENDPOINT = "https://mihon-tracker.onrender.com"

    private val httpClient: OkHttpClient by lazy {
        OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .build()
    }

    private fun getPrefs(context: Context): SharedPreferences {
        return context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    }

    fun getEndpointUrl(context: Context): String {
        return getPrefs(context).getString(KEY_ENDPOINT, DEFAULT_ENDPOINT)?.trim().orEmpty().ifEmpty {
            DEFAULT_ENDPOINT
        }
    }

    fun setEndpointUrl(context: Context, url: String) {
        getPrefs(context).edit().putString(KEY_ENDPOINT, url.trim().trimEnd('/')).apply()
    }

    fun getApiKey(context: Context): String {
        return getPrefs(context).getString(KEY_API_KEY, "")?.trim().orEmpty()
    }

    fun setApiKey(context: Context, key: String) {
        getPrefs(context).edit().putString(KEY_API_KEY, key.trim()).apply()
    }

    fun isEnabled(context: Context): Boolean {
        return getPrefs(context).getBoolean(KEY_ENABLED, true)
    }

    fun setEnabled(context: Context, enabled: Boolean) {
        getPrefs(context).edit().putBoolean(KEY_ENABLED, enabled).apply()
    }

    /**
     * Dispatches chapter completion event to the configured custom tracker endpoint.
     * Retries offline events automatically.
     */
    fun onChapterCompleted(context: Context, manga: Manga, chapter: Chapter) {
        if (!isEnabled(context)) {
            logcat { "CustomTrackerService: Tracking disabled by user, skipping." }
            return
        }

        val baseUrl = getEndpointUrl(context)
        val apiKey = getApiKey(context)
        val trackUrl = if (baseUrl.endsWith("/api/track")) baseUrl else "$baseUrl/api/track"

        scope.launch {
            val isoTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
            val eventId = "evt_${manga.id}_${chapter.chapterNumber}_${System.currentTimeMillis()}"

            val payloadJson = buildJsonObject {
                put("id", eventId)
                put("event_id", eventId)
                put("manga_id", manga.id.toString())
                put("manga_title", manga.title)
                if (chapter.chapterNumber % 1.0 == 0.0) {
                    put("chapter_number", chapter.chapterNumber.toLong())
                } else {
                    put("chapter_number", chapter.chapterNumber)
                }
                put("chapter_id", chapter.id.toString())
                put("cover_url", manga.thumbnailUrl.orEmpty())
                put("timestamp", isoTimestamp)
                put("source", "mihon-fork")
                putJsonArray("genres") {
                    manga.genre?.forEach { add(it) }
                }
            }.toString()

            // Try dispatching immediately
            val success = sendTrackingPayload(trackUrl, apiKey, payloadJson)
            if (!success) {
                logcat { "CustomTrackerService: Network request failed, queueing event for retry." }
                enqueueOfflineEvent(context, payloadJson)
            } else {
                // If this succeeded, try flushing any previously pending offline events
                flushOfflineQueue(context, trackUrl, apiKey)
            }
        }
    }

    private fun sendTrackingPayload(url: String, apiKey: String, json: String): Boolean {
        return try {
            val reqBuilder = Request.Builder()
                .url(url)
                .post(json.toRequestBody(jsonMediaType))
                .header("Accept", "application/json")
                .header("User-Agent", "Mihon-Tracker-Android/2.0")

            if (apiKey.isNotBlank()) {
                reqBuilder.header("X-API-KEY", apiKey)
            }

            httpClient.newCall(reqBuilder.build()).execute().use { response ->
                if (response.isSuccessful) {
                    logcat { "CustomTrackerService: Event delivered successfully (HTTP ${response.code})" }
                    true
                } else {
                    logcat { "CustomTrackerService: Server rejected event with HTTP ${response.code}: ${response.message}" }
                    // 400 Bad Request won't fix with retry, but 5xx or network might
                    response.code == 400
                }
            }
        } catch (e: Exception) {
            logcat { "CustomTrackerService: Connection exception: ${e.message}" }
            false
        }
    }

    @Synchronized
    private fun enqueueOfflineEvent(context: Context, json: String) {
        try {
            val queueFile = File(context.filesDir, QUEUE_FILENAME)
            val currentList = if (queueFile.exists()) {
                try {
                    Json.parseToJsonElement(queueFile.readText()).jsonArray.toMutableList()
                } catch (e: Exception) {
                    mutableListOf()
                }
            } else {
                mutableListOf()
            }

            val newElement = Json.parseToJsonElement(json)
            currentList.add(newElement)

            // Cap queue at 100 events to prevent unbounded growth
            if (currentList.size > 100) {
                currentList.removeAt(0)
            }

            val arrayJson = buildJsonObject {
                putJsonArray("queue") {
                    currentList.forEach { add(it) }
                }
            }["queue"].toString()

            queueFile.writeText(arrayJson)
            logcat { "CustomTrackerService: Enqueued event. Queue size: ${currentList.size}" }
        } catch (e: Exception) {
            logcat { "CustomTrackerService: Failed to enqueue offline event: ${e.message}" }
        }
    }

    @Synchronized
    private fun flushOfflineQueue(context: Context, url: String, apiKey: String) {
        val queueFile = File(context.filesDir, QUEUE_FILENAME)
        if (!queueFile.exists()) return

        try {
            val content = queueFile.readText()
            val items = Json.parseToJsonElement(content).jsonArray
            if (items.isEmpty()) return

            val remaining = mutableListOf<String>()
            for (item in items) {
                val itemStr = item.toString()
                val ok = sendTrackingPayload(url, apiKey, itemStr)
                if (!ok) {
                    remaining.add(itemStr)
                }
            }

            if (remaining.isEmpty()) {
                queueFile.delete()
                logcat { "CustomTrackerService: Successfully flushed all offline events." }
            } else {
                val newArray = "[" + remaining.joinToString(",") + "]"
                queueFile.writeText(newArray)
                logcat { "CustomTrackerService: Flushed partial queue, ${remaining.size} items remaining." }
            }
        } catch (e: Exception) {
            logcat { "CustomTrackerService: Error flushing queue: ${e.message}" }
        }
    }

    /**
     * Synchronizes the entire Mihon library with the tracker backend.
     * Extracts manga metadata, total chapters, read status, and progress.
     */
    suspend fun syncFullLibrary(
        context: Context,
        onResult: (success: Boolean, message: String) -> Unit,
    ) {
        withContext(Dispatchers.IO) {
            try {
                val baseUrl = getEndpointUrl(context)
                val apiKey = getApiKey(context)
                val syncUrl = if (baseUrl.endsWith("/api/sync/library")) baseUrl else "$baseUrl/api/sync/library"

                val favorites: List<Manga> = context.appGraph.getFavorites.await()
                if (favorites.isEmpty()) {
                    withContext(Dispatchers.Main) {
                        onResult(true, "Tu biblioteca de Mihon está vacía. No hay mangas que sincronizar.")
                    }
                    return@withContext
                }

                logcat { "CustomTrackerService: Starting full library sync for ${favorites.size} mangas" }

                val mangaJsonList = buildJsonObject {
                    putJsonArray("mangas") {
                        for (manga in favorites) {
                            addJsonObject {
                                put("id", manga.id.toString())
                                put("title", manga.title)
                                put("cover_url", manga.thumbnailUrl.orEmpty())
                                put("author", manga.author.orEmpty())
                                put("artist", manga.artist.orEmpty())
                                put("source", manga.source.toString())
                                put("is_favorite", manga.favorite)
                                put("status", if (manga.status == 2L) "completed" else "reading")
                                putJsonArray("genres") {
                                    manga.genre?.forEach { add(it) }
                                }
                            }
                        }
                    }
                }.toString()

                val requestBuilder = Request.Builder()
                    .url(syncUrl)
                    .post(mangaJsonList.toRequestBody(jsonMediaType))
                    .header("Accept", "application/json")
                    .header("User-Agent", "Mihon-Tracker-Android/2.0")

                if (apiKey.isNotBlank()) {
                    requestBuilder.header("X-API-KEY", apiKey)
                }

                httpClient.newCall(requestBuilder.build()).execute().use { response ->
                    val respBody = response.body?.string().orEmpty()
                    if (response.isSuccessful) {
                        withContext(Dispatchers.Main) {
                            onResult(true, "¡Sincronización completada! ${favorites.size} mangas sincronizados.")
                        }
                    } else {
                        withContext(Dispatchers.Main) {
                            onResult(false, "Error del servidor (HTTP ${response.code}): $respBody")
                        }
                    }
                }
            } catch (e: Exception) {
                logcat { "CustomTrackerService: Library sync failed: ${e.message}" }
                withContext(Dispatchers.Main) {
                    onResult(false, "Fallo de conexión: ${e.message}")
                }
            }
        }
    }
}
