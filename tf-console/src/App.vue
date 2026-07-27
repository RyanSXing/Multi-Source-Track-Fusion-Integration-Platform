<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import maplibregl, { type GeoJSONSource, type Map as MapLibreMap } from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'

type TrackStatus = 'TENTATIVE' | 'CONFIRMED' | 'COASTING' | 'DROPPED'

interface Detection {
  sourceId: string
  sourceType: string
  observedAt: string
  receivedAt: string
  latDeg: number
  lonDeg: number
  altMeters: number | null
  speedMps: number | null
  headingDeg: number | null
  positionSigmaMeters: number
  attributes: Record<string, string>
}

interface Track {
  sessionId: string
  trackId: number
  status: TrackStatus
  stateAt: string
  lastObservedAt: string
  latDeg: number
  lonDeg: number
  altMeters: number | null
  eastVelocityMps: number
  northVelocityMps: number
  hitCount: number
  consecutiveMisses: number
  contributors: Detection[]
}

interface TrackEvent {
  sessionId: string
  version: number
  snapshot: boolean
  tracks: Track[]
}

interface SourceHealth {
  sourceId: string
  sourceType: string
  lastMessageAt: string | null
  adapterReceivedCount: number
  kafkaPublishedCount: number
  kafkaConsumedCount: number
  redeliveredCount: number
  lateCount: number
  errorCount: number
  circuitTransitionCount: number
  degraded: boolean
  messageRatePerSecond: number
}

const mapRoot = ref<HTMLElement>()
const tracks = ref(new Map<number, Track>())
const sources = ref<SourceHealth[]>([])
const selectedId = ref<number>()
const sessionId = ref('')
const loading = ref(true)
const error = ref('')
const connection = ref<'connecting' | 'live' | 'retrying'>('connecting')
const lastUpdated = ref<number>()
const now = ref(Date.now())

const trackList = computed(() =>
  [...tracks.value.values()].sort((a, b) => a.trackId - b.trackId),
)
const selected = computed(() =>
  selectedId.value === undefined ? undefined : tracks.value.get(selectedId.value),
)
const confirmedCount = computed(
  () => trackList.value.filter((track) => track.status === 'CONFIRMED').length,
)

let map: MapLibreMap | undefined
let socket: WebSocket | undefined
let reconnectTimer: number | undefined
let healthTimer: number | undefined
let clockTimer: number | undefined
let reconnects = 0
let stopped = false
let latestVersion = -1

function sourceTypes(track: Track) {
  return [...new Set(track.contributors.map((item) => item.sourceType))].sort()
}

function speed(track: Track) {
  return Math.hypot(track.eastVelocityMps, track.northVelocityMps)
}

function formatNumber(value: number | null, digits = 0) {
  return value === null ? '—' : value.toFixed(digits)
}

function formatTime(value: string | undefined) {
  return value
    ? new Intl.DateTimeFormat(undefined, {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      }).format(new Date(value))
    : '—'
}

function age(value: string | null) {
  if (!value) return 'Waiting'
  const seconds = Math.max(0, Math.floor((now.value - Date.parse(value)) / 1000))
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  return `${Math.floor(seconds / 3600)}h ago`
}

function sourceState(source: SourceHealth) {
  if (!source.lastMessageAt) return 'waiting'
  return now.value - Date.parse(source.lastMessageAt) > 30_000 || source.degraded
    ? 'degraded'
    : 'healthy'
}

function replaceTracks(next: Track[]) {
  tracks.value = new Map(
    next
      .filter((track) => track.status !== 'DROPPED')
      .map((track) => [track.trackId, track]),
  )
}

function adoptSession(nextSessionId: string) {
  if (sessionId.value && sessionId.value !== nextSessionId) {
    tracks.value = new Map()
    selectedId.value = undefined
    latestVersion = -1
  }
  sessionId.value = nextSessionId
}

function applyEvent(event: TrackEvent) {
  adoptSession(event.sessionId)
  if (event.version < latestVersion) return
  latestVersion = event.version
  if (event.snapshot) {
    replaceTracks(event.tracks)
  } else {
    const next = new Map(tracks.value)
    event.tracks.forEach((track) => {
      if (track.status === 'DROPPED') {
        next.delete(track.trackId)
        if (selectedId.value === track.trackId) selectedId.value = undefined
      } else {
        next.set(track.trackId, track)
      }
    })
    tracks.value = next
  }
  lastUpdated.value = Date.now()
}

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url)
  if (!response.ok) throw new Error(`${url} returned ${response.status}`)
  return response.json() as Promise<T>
}

async function refreshHealth() {
  try {
    sources.value = await getJson<SourceHealth[]>('/api/sources/health')
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : 'Source health is unavailable'
  }
}

async function loadInitial() {
  loading.value = true
  error.value = ''
  try {
    const [session, current, health] = await Promise.all([
      getJson<{ sessionId: string }>('/api/session'),
      getJson<Track[]>('/api/tracks'),
      getJson<SourceHealth[]>('/api/sources/health'),
    ])
    adoptSession(session.sessionId)
    replaceTracks(current)
    sources.value = health
    lastUpdated.value = Date.now()
  } catch (cause) {
    error.value = cause instanceof Error ? cause.message : 'Track service is unavailable'
  } finally {
    loading.value = false
  }
}

function connect() {
  if (stopped) return
  connection.value = reconnects ? 'retrying' : 'connecting'
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  socket = new WebSocket(`${protocol}//${window.location.host}/ws/tracks`)
  socket.onopen = () => {
    reconnects = 0
    connection.value = 'live'
    error.value = ''
  }
  socket.onmessage = (message) => {
    try {
      applyEvent(JSON.parse(message.data) as TrackEvent)
    } catch {
      error.value = 'A malformed track update was ignored'
    }
  }
  socket.onerror = () => socket?.close()
  socket.onclose = () => {
    if (stopped) return
    connection.value = 'retrying'
    reconnects += 1
    reconnectTimer = window.setTimeout(connect, Math.min(1000 * 2 ** reconnects, 15_000))
  }
}

function geoJson() {
  return {
    type: 'FeatureCollection' as const,
    features: trackList.value.map((track) => {
      const types = sourceTypes(track)
      return {
        type: 'Feature' as const,
        geometry: {
          type: 'Point' as const,
          coordinates: [track.lonDeg, track.latDeg],
        },
        properties: {
          trackId: track.trackId,
          label: `T${track.trackId}`,
          status: track.status,
          sourceType: types.length > 1 ? 'FUSED' : (types[0] ?? 'UNKNOWN'),
          sourceCount: types.length,
        },
      }
    }),
  }
}

function updateMap() {
  const source = map?.getSource('tracks') as GeoJSONSource | undefined
  source?.setData(geoJson())
}

function selectTrack(track: Track, center = false) {
  selectedId.value = track.trackId
  if (center) {
    map?.flyTo({ center: [track.lonDeg, track.latDeg], zoom: Math.max(map.getZoom(), 10) })
  }
}

function changeTrack(event: Event) {
  const track = tracks.value.get(Number((event.target as HTMLSelectElement).value))
  if (track) selectTrack(track, true)
}

function initMap() {
  if (!mapRoot.value) return
  map = new maplibregl.Map({
    container: mapRoot.value,
    style: {
      version: 8,
      sources: {
        basemap: {
          type: 'raster',
          tiles: ['https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png'],
          tileSize: 256,
          attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
        },
      },
      layers: [
        {
          id: 'basemap',
          type: 'raster',
          source: 'basemap',
          paint: {
            'raster-opacity': 0.72,
            'raster-saturation': -0.5,
            'raster-contrast': 0.18,
          },
        },
      ],
    },
    center: [-79.38, 43.65],
    zoom: 9,
    attributionControl: false,
  })
  map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right')
  map.addControl(new maplibregl.AttributionControl({ compact: true }))
  map.on('load', () => {
    map?.addSource('tracks', { type: 'geojson', data: geoJson() })
    map?.addLayer({
      id: 'fusion-halo',
      type: 'circle',
      source: 'tracks',
      paint: {
        'circle-radius': ['case', ['>', ['get', 'sourceCount'], 1], 14, 10],
        'circle-color': [
          'match',
          ['get', 'status'],
          'CONFIRMED',
          '#22c55e',
          'COASTING',
          '#f59e0b',
          'TENTATIVE',
          '#818cf8',
          '#64748b',
        ],
        'circle-opacity': 0.2,
        'circle-blur': 0.25,
      },
    })
    map?.addLayer({
      id: 'track-points',
      type: 'circle',
      source: 'tracks',
      paint: {
        'circle-radius': 7,
        'circle-color': [
          'match',
          ['get', 'sourceType'],
          'FUSED',
          '#f59e0b',
          'ADSB',
          '#38bdf8',
          'RADAR',
          '#fb7185',
          'AIS',
          '#2dd4bf',
          'WEATHER',
          '#a78bfa',
          '#94a3b8',
        ],
        'circle-stroke-width': 2,
        'circle-stroke-color': [
          'match',
          ['get', 'status'],
          'CONFIRMED',
          '#dcfce7',
          'COASTING',
          '#fef3c7',
          'TENTATIVE',
          '#e0e7ff',
          '#e2e8f0',
        ],
      },
    })
    map?.addLayer({
      id: 'track-labels',
      type: 'symbol',
      source: 'tracks',
      layout: {
        'text-field': ['get', 'label'],
        'text-size': 11,
        'text-offset': [0, 1.35],
        'text-anchor': 'top',
      },
      paint: {
        'text-color': '#f8fafc',
        'text-halo-color': '#0f172a',
        'text-halo-width': 1,
      },
    })
    map?.on('click', 'track-points', (event) => {
      const id = Number(event.features?.[0]?.properties?.trackId)
      const track = tracks.value.get(id)
      if (track) selectTrack(track)
    })
    map?.on('mouseenter', 'track-points', () => {
      if (map) map.getCanvas().style.cursor = 'pointer'
    })
    map?.on('mouseleave', 'track-points', () => {
      if (map) map.getCanvas().style.cursor = ''
    })
    updateMap()
  })
}

watch(trackList, updateMap)

onMounted(async () => {
  initMap()
  await loadInitial()
  connect()
  healthTimer = window.setInterval(refreshHealth, 5_000)
  clockTimer = window.setInterval(() => {
    now.value = Date.now()
  }, 1_000)
})

onBeforeUnmount(() => {
  stopped = true
  if (reconnectTimer) window.clearTimeout(reconnectTimer)
  if (healthTimer) window.clearInterval(healthTimer)
  if (clockTimer) window.clearInterval(clockTimer)
  socket?.close()
  map?.remove()
})
</script>

<template>
  <div class="console">
    <header class="topbar">
      <div class="brand">
        <svg viewBox="0 0 40 40" aria-hidden="true">
          <circle cx="20" cy="20" r="15" />
          <circle cx="20" cy="20" r="8" />
          <path d="M20 20 31 9M20 5v30M5 20h30" />
          <circle class="brand-blip" cx="28" cy="12" r="2.5" />
        </svg>
        <div>
          <strong>Track Fusion</strong>
          <span>Integration Console</span>
        </div>
      </div>

      <div class="summary" aria-label="Current tracking summary">
        <span><b>{{ trackList.length }}</b> active</span>
        <span><b>{{ confirmedCount }}</b> confirmed</span>
        <span class="session" :title="sessionId">session {{ sessionId.slice(0, 8) || '—' }}</span>
      </div>

      <div class="connection" :class="connection" role="status">
        <i aria-hidden="true"></i>
        {{ connection }}
      </div>
    </header>

    <div v-if="error" class="error-banner" role="alert">
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M12 3 2 21h20L12 3Zm0 6v5m0 3v1" />
      </svg>
      <span>{{ error }}</span>
      <button type="button" @click="loadInitial">Retry</button>
    </div>

    <main class="workspace">
      <aside class="panel sources-panel" aria-labelledby="sources-title">
        <div class="panel-heading">
          <div>
            <span class="eyebrow">Ingest</span>
            <h1 id="sources-title">Source health</h1>
          </div>
          <span class="count">{{ sources.length }}</span>
        </div>

        <div v-if="loading" class="skeleton-list" aria-label="Loading source health">
          <i v-for="item in 4" :key="item"></i>
        </div>
        <p v-else-if="!sources.length" class="empty-small">Waiting for source traffic.</p>
        <ul v-else class="source-list">
          <li v-for="source in sources" :key="`${source.sourceType}:${source.sourceId}`">
            <div class="source-title">
              <span class="health-dot" :class="sourceState(source)"></span>
              <div>
                <strong>{{ source.sourceType }}</strong>
                <span>{{ source.sourceId }}</span>
              </div>
              <b>{{ source.messageRatePerSecond.toFixed(1) }}/s</b>
            </div>
            <dl>
              <div>
                <dt>Last message</dt>
                <dd>{{ age(source.lastMessageAt) }}</dd>
              </div>
              <div>
                <dt>Consumed</dt>
                <dd>{{ source.kafkaConsumedCount.toLocaleString() }}</dd>
              </div>
              <div>
                <dt>Errors</dt>
                <dd :class="{ danger: source.errorCount }">{{ source.errorCount }}</dd>
              </div>
              <div>
                <dt>Late / retry</dt>
                <dd>{{ source.lateCount }} / {{ source.redeliveredCount }}</dd>
              </div>
            </dl>
          </li>
        </ul>

        <div class="legend" aria-label="Track legend">
          <span class="eyebrow">Map legend</span>
          <div><i class="source fused"></i>Multi-source</div>
          <div><i class="source adsb"></i>ADS-B</div>
          <div><i class="source radar"></i>Radar</div>
          <div><i class="state confirmed"></i>Confirmed</div>
          <div><i class="state coasting"></i>Coasting</div>
          <div><i class="state tentative"></i>Tentative</div>
        </div>
      </aside>

      <section class="map-panel" aria-label="Live fused track map">
        <div ref="mapRoot" class="map"></div>
        <div class="map-heading">
          <span class="eyebrow">Operational picture</span>
          <strong>Live tracks</strong>
        </div>
        <div v-if="loading" class="map-state">
          <span class="loader" aria-hidden="true"></span>
          <strong>Building operational picture</strong>
          <small>Loading current tracks and source state</small>
        </div>
        <div v-else-if="!trackList.length" class="map-state">
          <svg viewBox="0 0 48 48" aria-hidden="true">
            <circle cx="24" cy="24" r="18" />
            <circle cx="24" cy="24" r="9" />
            <path d="M24 24 38 11" />
          </svg>
          <strong>No active tracks</strong>
          <small>The map will update when detections clear the fusion window.</small>
        </div>
        <div class="updated">
          Last update {{ lastUpdated ? age(new Date(lastUpdated).toISOString()) : '—' }}
        </div>
      </section>

      <aside class="panel detail-panel" aria-labelledby="detail-title">
        <template v-if="selected">
          <div class="panel-heading">
            <div>
              <span class="eyebrow">Track detail</span>
              <h2 id="detail-title">Track {{ selected.trackId }}</h2>
            </div>
            <span class="status-chip" :class="selected.status.toLowerCase()">
              {{ selected.status }}
            </span>
          </div>

          <label class="track-switcher">
            <span>Change selected track</span>
            <select :value="selected.trackId" @change="changeTrack">
              <option v-for="track in trackList" :key="track.trackId" :value="track.trackId">
                Track {{ track.trackId }} · {{ sourceTypes(track).join(' + ') || 'Unknown source' }}
              </option>
            </select>
          </label>

          <div class="coordinate">
            <strong>{{ selected.latDeg.toFixed(5) }}, {{ selected.lonDeg.toFixed(5) }}</strong>
            <span>observed {{ formatTime(selected.lastObservedAt) }}</span>
          </div>

          <dl class="track-metrics">
            <div>
              <dt>Altitude</dt>
              <dd>{{ formatNumber(selected.altMeters) }} <small>m</small></dd>
            </div>
            <div>
              <dt>Ground speed</dt>
              <dd>{{ speed(selected).toFixed(1) }} <small>m/s</small></dd>
            </div>
            <div>
              <dt>Hits</dt>
              <dd>{{ selected.hitCount }}</dd>
            </div>
            <div>
              <dt>Misses</dt>
              <dd>{{ selected.consecutiveMisses }}</dd>
            </div>
          </dl>

          <section class="contributors">
            <div class="section-heading">
              <div>
                <span class="eyebrow">Evidence</span>
                <h3>Fused detections</h3>
              </div>
              <span class="count">{{ selected.contributors.length }}</span>
            </div>
            <article
              v-for="item in selected.contributors"
              :key="`${item.sourceType}:${item.sourceId}:${item.observedAt}`"
            >
              <div>
                <span class="source-badge" :class="item.sourceType.toLowerCase()">
                  {{ item.sourceType }}
                </span>
                <strong>{{ item.sourceId }}</strong>
              </div>
              <time :datetime="item.observedAt">{{ formatTime(item.observedAt) }}</time>
              <dl>
                <div>
                  <dt>Position</dt>
                  <dd>{{ item.latDeg.toFixed(5) }}, {{ item.lonDeg.toFixed(5) }}</dd>
                </div>
                <div>
                  <dt>Uncertainty</dt>
                  <dd>±{{ formatNumber(item.positionSigmaMeters) }} m</dd>
                </div>
              </dl>
            </article>
          </section>
        </template>

        <template v-else>
          <div class="panel-heading">
            <div>
              <span class="eyebrow">Track detail</span>
              <h2 id="detail-title">Select a track</h2>
            </div>
          </div>
          <div class="track-index">
            <p v-if="!trackList.length" class="empty-small">
              Active tracks will appear here.
            </p>
            <button
              v-for="track in trackList"
              :key="track.trackId"
              type="button"
              @click="selectTrack(track, true)"
            >
              <span>
                <b>Track {{ track.trackId }}</b>
                <small>{{ sourceTypes(track).join(' + ') || 'Unknown source' }}</small>
              </span>
              <span class="status-chip" :class="track.status.toLowerCase()">
                {{ track.status }}
              </span>
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <path d="m9 5 7 7-7 7" />
              </svg>
            </button>
          </div>
        </template>
      </aside>
    </main>
  </div>
</template>
