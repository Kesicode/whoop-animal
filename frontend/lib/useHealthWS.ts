/**
 * useHealthWS.ts
 * ==============
 * React hook that opens a WebSocket connection to:
 *   ws://host/ws/health/{animalId}?token=<jwt>
 *
 * Usage (on the animal detail page):
 *
 *   const { latestReading, isConnected, error } = useHealthWS(animalId);
 *
 * Features:
 *   - Automatic reconnection with exponential backoff (up to 30s)
 *   - Cleans up the socket on unmount
 *   - Keeps `latestReading` state updated on each server push
 *   - Exposes `isConnected` so the UI can show a "LIVE" badge
 */

"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { getAccessToken } from "@/lib/auth";

// ---- Types -----------------------------------------------------------------

export interface HealthReading {
    type: "health_reading" | "connected";
    animal_id: number;
    temperature: number;       // °C
    heart_rate: number;        // bpm
    activity_level: number;    // 0–100
    rumination_score: number;  // 0–100
    timestamp: string;         // ISO 8601
}

interface UseHealthWSReturn {
    latestReading: HealthReading | null;
    isConnected: boolean;
    error: string | null;
}

// ---- Constants -------------------------------------------------------------

const BASE_DELAY_MS = 1_000;   // 1 s initial reconnect delay
const MAX_DELAY_MS = 30_000;  // 30 s max backoff

// ---- Hook ------------------------------------------------------------------

export function useHealthWS(animalId: number | null): UseHealthWSReturn {
    const [latestReading, setLatestReading] = useState<HealthReading | null>(null);
    const [isConnected, setIsConnected] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Refs so the reconnect closure always sees fresh values
    const wsRef = useRef<WebSocket | null>(null);
    const attemptRef = useRef(0);
    const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const mountedRef = useRef(true);

    const connect = useCallback(() => {
        if (!mountedRef.current || animalId == null) return;

        const token = getAccessToken();
        if (!token) {
            setError("Not authenticated");
            return;
        }

        // Build WS URL  (wss:// in production, ws:// in dev)
        const protocol = window.location.protocol === "https:" ? "wss" : "ws";
        const host = process.env.NEXT_PUBLIC_API_HOST ?? "localhost:8000";
        const url = `${protocol}://${host}/ws/health/${animalId}?token=${token}`;

        console.debug("[useHealthWS] connecting to", url);
        const ws = new WebSocket(url);
        wsRef.current = ws;

        // ------------------------------------------------------------------
        // onopen — reset backoff counter
        // ------------------------------------------------------------------
        ws.onopen = () => {
            if (!mountedRef.current) return;
            console.debug("[useHealthWS] connected animal_id=%d", animalId);
            setIsConnected(true);
            setError(null);
            attemptRef.current = 0;
        };

        // ------------------------------------------------------------------
        // onmessage — the core handler
        //
        // The server sends two kinds of messages:
        //   { type: "connected", ... }   — initial handshake, ignore
        //   { type: "health_reading", temperature, heart_rate, ... }  — live data
        // ------------------------------------------------------------------
        ws.onmessage = (event: MessageEvent) => {
            if (!mountedRef.current) return;
            try {
                const data = JSON.parse(event.data) as HealthReading;
                if (data.type === "health_reading") {
                    setLatestReading(data);
                }
            } catch {
                console.warn("[useHealthWS] failed to parse message", event.data);
            }
        };

        // ------------------------------------------------------------------
        // onclose — reconnect with exponential backoff
        // Codes:
        //   4001 → bad token → do NOT reconnect
        //   4003 → forbidden → do NOT reconnect
        //   4004 → not found → do NOT reconnect
        //   1000 → normal close (component unmounting) → do NOT reconnect
        //   everything else → reconnect
        // ------------------------------------------------------------------
        ws.onclose = (event: CloseEvent) => {
            if (!mountedRef.current) return;
            setIsConnected(false);
            console.debug("[useHealthWS] closed code=%d reason=%s", event.code, event.reason);

            const permanentErrors = [4001, 4003, 4004, 1000];
            if (permanentErrors.includes(event.code)) {
                setError(event.reason || "Connection closed");
                return;
            }

            // Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s, 30s, …
            const delay = Math.min(BASE_DELAY_MS * 2 ** attemptRef.current, MAX_DELAY_MS);
            attemptRef.current += 1;
            console.debug("[useHealthWS] reconnecting in %dms (attempt %d)", delay, attemptRef.current);

            timeoutRef.current = setTimeout(() => {
                if (mountedRef.current) connect();
            }, delay);
        };

        ws.onerror = () => {
            // onerror always fires before onclose, so onclose handles reconnect
            console.warn("[useHealthWS] websocket error for animal_id=%d", animalId);
        };
    }, [animalId]);

    // ------------------------------------------------------------------
    // Mount / unmount
    // ------------------------------------------------------------------
    useEffect(() => {
        mountedRef.current = true;
        connect();

        return () => {
            mountedRef.current = false;                // stop reconnect loop
            if (timeoutRef.current) clearTimeout(timeoutRef.current);
            if (wsRef.current) {
                wsRef.current.close(1000, "Component unmounted");
                wsRef.current = null;
            }
        };
    }, [connect]);

    return { latestReading, isConnected, error };
}
