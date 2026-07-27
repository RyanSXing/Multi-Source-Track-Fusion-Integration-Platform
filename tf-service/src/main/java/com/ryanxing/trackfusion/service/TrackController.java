package com.ryanxing.trackfusion.service;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api")
public final class TrackController {
    private final TrackService tracks;
    private final SourceHealthRegistry health;

    public TrackController(TrackService tracks, SourceHealthRegistry health) {
        this.tracks = tracks;
        this.health = health;
    }

    @GetMapping("/tracks")
    public List<TrackView> tracks() {
        return tracks.currentTracks();
    }

    @GetMapping("/tracks/{trackId}/history")
    public Flux<TrackView> history(@PathVariable("trackId") long trackId) {
        return tracks.history(trackId);
    }

    @GetMapping("/sources/health")
    public List<SourceHealthRegistry.SourceHealth> health() {
        return health.snapshots();
    }
}
