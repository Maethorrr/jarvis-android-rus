package com.codotype.jarvis.audio

import android.annotation.SuppressLint
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaRecorder
import java.io.ByteArrayOutputStream
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlin.math.sqrt

class AudioRecorder {

    private val sampleRate = 16_000

    @Volatile
    private var recording = false

    private var record: AudioRecord? = null
    private var readerThread: Thread? = null

    private val audioLock = Any()
    private val pcmBytes = ByteArrayOutputStream()

    private val _amplitude = MutableStateFlow(0f)
    val amplitude: StateFlow<Float> = _amplitude.asStateFlow()

    @SuppressLint("MissingPermission")
    @Synchronized
    fun start(): Boolean {
        if (recording) return true

        val minBuf = AudioRecord.getMinBufferSize(
            sampleRate,
            AudioFormat.CHANNEL_IN_MONO,
            AudioFormat.ENCODING_PCM_16BIT
        ).coerceAtLeast(sampleRate)

        val bufferSize = maxOf(minBuf, sampleRate * 2)

        val rec = runCatching {
            AudioRecord(
                MediaRecorder.AudioSource.VOICE_RECOGNITION,
                sampleRate,
                AudioFormat.CHANNEL_IN_MONO,
                AudioFormat.ENCODING_PCM_16BIT,
                bufferSize
            )
        }.getOrNull() ?: return false

        if (rec.state != AudioRecord.STATE_INITIALIZED) {
            rec.release()
            return false
        }

        synchronized(audioLock) {
            pcmBytes.reset()
        }
        _amplitude.value = 0f

        val started = runCatching {
            rec.startRecording()
            rec.recordingState == AudioRecord.RECORDSTATE_RECORDING
        }.getOrDefault(false)

        if (!started) {
            runCatching { rec.release() }
            return false
        }

        record = rec
        recording = true

        readerThread = Thread({
            val buffer = ShortArray(2048)

            while (recording) {
                val count = try {
                    rec.read(
                        buffer,
                        0,
                        buffer.size,
                        AudioRecord.READ_BLOCKING
                    )
                } catch (_: Exception) {
                    break
                }

                if (count <= 0) break

                val bytes = ByteArray(count * 2)

                for (i in 0 until count) {
                    val value = buffer[i].toInt()
                    bytes[i * 2] = (value and 0xff).toByte()
                    bytes[i * 2 + 1] = (value shr 8).toByte()
                }

                synchronized(audioLock) {
                    pcmBytes.write(bytes, 0, bytes.size)
                }

                updateAmplitude(buffer, count)
            }
        }, "JarvisAudioReader").apply {
            isDaemon = true
            start()
        }

        return true
    }

    fun stop(): FloatArray {
        val rec: AudioRecord
        val thread: Thread?

        synchronized(this) {
            rec = record ?: run {
                recording = false
                _amplitude.value = 0f
                return FloatArray(0)
            }

            recording = false
            runCatching {
                if (rec.recordingState == AudioRecord.RECORDSTATE_RECORDING) {
                    rec.stop()
                }
            }

            thread = readerThread
        }

        if (thread != null && thread !== Thread.currentThread()) {
            runCatching { thread.join(1000) }
        }

        runCatching { rec.release() }

        if (thread != null && thread !== Thread.currentThread()) {
            runCatching { thread.join(200) }
        }

        synchronized(this) {
            if (record === rec) record = null
            readerThread = null
        }

        val bytes = synchronized(audioLock) {
            pcmBytes.toByteArray().also { pcmBytes.reset() }
        }

        _amplitude.value = 0f

        val count = bytes.size / 2
        return FloatArray(count) { i ->
            val sample = (
                (bytes[i * 2 + 1].toInt() shl 8) or
                    (bytes[i * 2].toInt() and 0xff)
                ).toShort()

            sample.toInt() / 32768f
        }
    }

    private fun updateAmplitude(samples: ShortArray, count: Int) {
        if (count <= 0) return

        var sum = 0.0

        for (i in 0 until count) {
            val value = samples[i].toDouble() / 32768.0
            sum += value * value
        }

        _amplitude.value = sqrt(sum / count)
            .toFloat()
            .coerceIn(0f, 1f)
    }

    @Synchronized
    fun cancel() {
        recording = false

        val rec = record
        val thread = readerThread

        if (rec != null) {
            runCatching {
                if (rec.recordingState == AudioRecord.RECORDSTATE_RECORDING) {
                    rec.stop()
                }
            }
        }

        if (thread != null && thread !== Thread.currentThread()) {
            runCatching { thread.join(1000) }
        }

        runCatching { rec?.release() }

        record = null
        readerThread = null

        synchronized(audioLock) {
            pcmBytes.reset()
        }

        _amplitude.value = 0f
    }
}