/**
 * useAlerts.ts
 * ============
 * React hook that opens a WebSocket connection to:
 *   ws://host/ws/alerts/{userId}?token=<jwt>
 *
 * Usage (in the root Farmer Layout — so it's always mounted):
 *
 *   const { alerts, unreadCount, markRead } = useAlerts(userId);
 *
 * Features:
 *   - Same exponential reconnect as useHealthWS
 *   - Maintains a local list of recent alerts for the AlertBanner
 *   - `unreadCount` drives the red badge on the sidebar Alerts link
 *   - `markRead(alertId)` called when farmer clicks Resolve / dismisses banner
 */

"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { getAccessToken } from "@/lib/auth";

// ---- Types -----------------------------------------------------------------

export interface LiveAlert {
    alert_id: number;
    animal_id: number;
    animal_name: string;
    alert_type: "fever" | "inactivity" | "low_rumination" | "heart_rate" | "device_offline";
    severity: "info" | "warning" | "critical";
    message: string;
    created_at: string;
    read: boolean;  // local-only flag
}

interface UseAlertsReturn {
    alerts: LiveAlert[];
    unreadCount: number;
    markRead: (alertId: number) => void;
    clearAll: () => void;
    isConnected: boolean;
}

// ---- Constants -------------------------------------------------------------

const BASE_DELAY_MS = 1_000;
const MAX_DELAY_MS = 30_000;
const MAX_STORED = 50;       // cap the in-memory alert list

// ---- Hook ------------------------------------------------------------------

export function useAlerts(userId: number | null): UseAlertsReturn {
    const [alerts, setAlerts] = useState<LiveAlert[]>([]);
    const [isConnected, setIsConnected] = useState(false);

    const wsRef = useRef<WebSocket | null>(null);
    const attemptRef = useRef(0);
    const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const mountedRef = useRef(true);

    const unreadCount = alerts.filter((a) => !a.read).length;

    // ------------------------------------------------------------------
    // connect
    // ------------------------------------------------------------------
    const connect = useCallback(() => {
        if (!mountedRef.current || userId == null) return;

        const token = getAccessToken();
        if (!token) return;

        const protocol = window.location.protocol === "https:" ? "wss" : "ws";
        const host = process.env.NEXT_PUBLIC_API_HOST ?? "localhost:8000";
        const url = `${protocol}://${host}/ws/alerts/${userId}?token=${token}`;

        console.debug("[useAlerts] connecting to", url);
        const ws = new WebSocket(url);
        wsRef.current = ws;

        ws.onopen = () => {
            if (!mountedRef.current) return;
            console.debug("[useAlerts] connected user_id=%d", userId);
            setIsConnected(true);
            attemptRef.current = 0;
        };

        // ------------------------------------------------------------------
        // onmessage
        //
        // Server sends two message kinds:
        //   { type: "connected" }   — handshake, ignore
        //   { type: "new_alert", alert_id, animal_id, animal_name,
        //           alert_type, severity, message, created_at }
        // ------------------------------------------------------------------
        ws.onmessage = (event: MessageEvent) => {
            if (!mountedRef.current) return;
            try {
                const data = JSON.parse(event.data);
                if (data.type !== "new_alert") return;

                const incoming: LiveAlert = {
                    alert_id: data.alert_id,
                    animal_id: data.animal_id,
                    animal_name: data.animal_name,
                    alert_type: data.alert_type,
                    severity: data.severity,
                    message: data.message,
                    created_at: data.created_at,
                    read: false,
                };

                setAlerts((prev) => {
                    // Dedup: don't add if the same alert_id already exists
                    if (prev.some((a) => a.alert_id === incoming.alert_id)) return prev;
                    // Prepend, cap at MAX_STORED
                    const updated = [incoming, ...prev];
                    return updated.slice(0, MAX_STORED);
                });

                // Optional: browser notification
                _tryBrowserNotification(incoming);

            } catch {
                console.warn("[useAlerts] failed to parse message", event.data);
            }
        };

        ws.onclose = (event: CloseEvent) => {
            if (!mountedRef.current) return;
            setIsConnected(false);
            const permanentErrors = [4001, 4003, 1000];
            if (permanentErrors.includes(event.code)) return;

            const delay = Math.min(BASE_DELAY_MS * 2 ** attemptRef.current, MAX_DELAY_MS);
            attemptRef.current += 1;
            timeoutRef.current = setTimeout(() => {
                if (mountedRef.current) connect();
            }, delay);
        };

        ws.onerror = () => {
            console.warn("[useAlerts] websocket error for user_id=%d", userId);
        };
    }, [userId]);

    // ------------------------------------------------------------------
    // Mount / unmount
    // ------------------------------------------------------------------
    useEffect(() => {
        mountedRef.current = true;
        connect();

        return () => {
            mountedRef.current = false;
            if (timeoutRef.current) clearTimeout(timeoutRef.current);
            if (wsRef.current) {
                wsRef.current.close(1000, "Component unmounted");
                wsRef.current = null;
            }
        };
    }, [connect]);

    // ------------------------------------------------------------------
    // Actions
    // ------------------------------------------------------------------
    const markRead = useCallback((alertId: number) => {
        setAlerts((prev) =>
            prev.map((a) => (a.alert_id === alertId ? { ...a, read: true } : a))
        );
    }, []);

    const clearAll = useCallback(() => {
        setAlerts((prev) => prev.map((a) => ({ ...a, read: true })));
    }, []);

    return { alerts, unreadCount, markRead, clearAll, isConnected };
}

// ---------------------------------------------------------------------------
// Browser Notification helper
// ---------------------------------------------------------------------------
function _tryBrowserNotification(alert: LiveAlert): void {
    if (typeof window === "undefined") return;
    if (!("Notification" in window)) return;
    if (Notification.permission !== "granted") return;

    const icons: Record<string, string> = {
        fever: "🌡️",
        inactivity: "😴",
        low_rumination: "🐄",
        heart_rate: "❤️",
        device_offline: "📡",
    };

    new Notification(`AgriGuard — ${alert.animal_name}`, {
        body: `${icons[alert.alert_type] ?? "⚠️"} ${alert.message}`,
        icon: "/favicon.ico",
        tag: `alert-${alert.alert_id}`,    // prevents duplicate toasts
    });
}
