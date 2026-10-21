package mihon.feature.tracker

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import tachiyomi.core.common.util.system.logcat
import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.manga.model.Manga
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

/**
 * Isolated Custom Tracking Service for Mihon Fork.
 *
 * Automatically sends webhook/API requests to the custom reading tracker backend
 * whenever a chapter is completed.
 *
 * Example Payload:
 * {
 *   "manga_id": "12345",
 *   "manga_title": "One Piece",
 *   "chapter_number": 1105,
 *   "cover_url": "https://example.com/cover.jpg",
 *   "timestamp": "2026-10-20T15:30:00Z",
 *   "genres": ["Shonen", "Action"]
 * }
 */
object CustomTrackerService {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val jsonMediaType = "application/json; charset=utf-8".toMediaType()

    private val httpClient: OkHttpClient by lazy {
        OkHttpClient.Builder()
            .connectTimeout(5, TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.SECONDS)
            .writeTimeout(5, TimeUnit.SECONDS)
            .build()
    }

    /**
     * Target endpoint URL for the reading tracker backend.
     * Deployed 24/7 on Render cloud platform.
     */
    @Volatile
    var trackerEndpointUrl: String = "https://mihon-tracker.onrender.com/api/track"

    @Volatile
    var isEnabled: Boolean = true

    /**
     * Dispatches chapter completion event to the configured custom tracker endpoint.
     */
    fun onChapterCompleted(manga: Manga, chapter: Chapter) {
        if (!isEnabled) {
            logcat { "CustomTrackerService: Tracking disabled, skipping" }
            return
        }

        scope.launch {
            try {
                val isoTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
                val payloadJson = buildJsonObject {
                    put("manga_id", manga.id.toString())
                    put("manga_title", manga.title)
                    if (chapter.chapterNumber % 1.0 == 0.0) {
                        put("chapter_number", chapter.chapterNumber.toLong())
                    } else {
                        put("chapter_number", chapter.chapterNumber)
                    }
                    put("cover_url", manga.thumbnailUrl.orEmpty())
                    put("timestamp", isoTimestamp)
                    putJsonArray("genres") {
                        manga.genre?.forEach { add(it) }
                    }
                }.toString()

                logcat { "CustomTrackerService: Dispatching chapter completion: $payloadJson to $trackerEndpointUrl" }

                val request = Request.Builder()
                    .url(trackerEndpointUrl)
                    .post(payloadJson.toRequestBody(jsonMediaType))
                    .header("Accept", "application/json")
                    .header("User-Agent", "Mihon-Custom-Tracker/1.0")
                    .build()

                httpClient.newCall(request).execute().use { response ->
                    if (response.isSuccessful) {
                        logcat { "CustomTrackerService: Chapter progress sent successfully (HTTP ${response.code})" }
                    } else {
                        logcat { "CustomTrackerService: Tracker error response HTTP ${response.code}: ${response.message}" }
                    }
                }
            } catch (e: Exception) {
                logcat { "CustomTrackerService: Error sending tracking data: ${e.message}" }
            }
        }
    }
}
