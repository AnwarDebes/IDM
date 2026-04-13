import React, { createContext, useContext, useState, useRef, useCallback, useEffect } from 'react';
import { SSE_URL } from './api';

const StreamContext = createContext(null);

export function useStream() {
  return useContext(StreamContext);
}

export function StreamProvider({ children }) {
  const [connected, setConnected] = useState(false);
  const [totalCount, setTotalCount] = useState(0);
  const [events, setEvents] = useState([]);
  const [anomalies, setAnomalies] = useState([]);

  const eventSourceRef = useRef(null);
  const countRef = useRef(0);
  const windowRef = useRef([]);

  const connect = useCallback(() => {
    if (eventSourceRef.current) eventSourceRef.current.close();

    const es = new EventSource(SSE_URL);
    eventSourceRef.current = es;

    es.onopen = () => setConnected(true);

    es.onerror = () => {
      // EventSource auto-reconnects; only mark disconnected if CLOSED
      if (es.readyState === EventSource.CLOSED) {
        setConnected(false);
      }
    };

    es.addEventListener('disease-event', (e) => {
      try {
        const data = JSON.parse(e.data);
        countRef.current += 1;
        setTotalCount(countRef.current);
        setEvents(prev => [data, ...prev].slice(0, 50));

        if (data.incidence_rate > 50) {
          setAnomalies(prev => [{
            ...data,
            timestamp: new Date().toLocaleTimeString(),
          }, ...prev].slice(0, 20));
        }

        windowRef.current.push(Date.now());
        windowRef.current = windowRef.current.filter(t => Date.now() - t < 10000);
      } catch {}
    });

    es.addEventListener('heartbeat', () => {
      setConnected(true);
    });
  }, []);

  const disconnect = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    setConnected(false);
  }, []);

  // Cleanup on unmount (app close)
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) eventSourceRef.current.close();
    };
  }, []);

  const value = {
    connected,
    totalCount,
    events,
    anomalies,
    windowRef,
    connect,
    disconnect,
  };

  return (
    <StreamContext.Provider value={value}>
      {children}
    </StreamContext.Provider>
  );
}
