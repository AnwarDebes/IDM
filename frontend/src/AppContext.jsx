import React, { createContext, useContext, useState, useEffect, useCallback, useRef } from 'react';
import { getQueries } from './api';

const AppContext = createContext(null);

export function useApp() {
  return useContext(AppContext);
}

export function AppProvider({ children }) {
  // Shared query list (fetched once, used by Explorer + Compare)
  const [queries, setQueries] = useState([]);

  // Explorer state
  const [explorerState, setExplorerState] = useState({
    selectedQuery: 'Q1',
    selectedBackend: 'postgres',
    params: {},
    result: null,
    showQuery: false,
  });

  // Compare state
  const [compareState, setCompareState] = useState({
    selectedQuery: 'Q1',
    result: null,
    allTimings: [],
  });

  // Toast state (managed centrally so it persists across pages)
  const [toasts, setToasts] = useState([]);
  const toastIdRef = useRef(0);

  // Fetch queries once at app level
  useEffect(() => {
    getQueries().then(d => setQueries(d.queries)).catch(() => {});
  }, []);

  // Explorer helpers
  const updateExplorer = useCallback((updates) => {
    setExplorerState(prev => ({ ...prev, ...updates }));
  }, []);

  // Compare helpers
  const updateCompare = useCallback((updates) => {
    setCompareState(prev => ({ ...prev, ...updates }));
  }, []);

  // Toast helpers
  const addToast = useCallback((message, type = 'loading') => {
    const id = ++toastIdRef.current;
    const toast = { id, message, type, startTime: Date.now(), collapsed: false };
    setToasts(prev => [...prev, toast]);
    return id;
  }, []);

  const updateToast = useCallback((id, updates) => {
    setToasts(prev => prev.map(t => t.id === id ? { ...t, ...updates } : t));
  }, []);

  const removeToast = useCallback((id) => {
    setToasts(prev => prev.filter(t => t.id !== id));
  }, []);

  const value = {
    queries,
    explorerState, updateExplorer,
    compareState, updateCompare,
    toasts, addToast, updateToast, removeToast,
  };

  return (
    <AppContext.Provider value={value}>
      {children}
    </AppContext.Provider>
  );
}
