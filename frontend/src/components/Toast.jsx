import React, { useState, useEffect } from 'react';
import { useApp } from '../AppContext';
import { colors } from '../styles';

function formatElapsed(ms) {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${s % 60}s`;
}

function ToastItem({ toast, onToggle, onDismiss }) {
  const [elapsed, setElapsed] = useState(0);

  useEffect(() => {
    if (toast.type !== 'loading') return;
    const interval = setInterval(() => {
      setElapsed(Date.now() - toast.startTime);
    }, 200);
    return () => clearInterval(interval);
  }, [toast.startTime, toast.type]);

  const isLoading = toast.type === 'loading';
  const isSuccess = toast.type === 'success';
  const isError = toast.type === 'error';

  const borderColor = isLoading ? colors.primary : isSuccess ? colors.success : colors.error;
  const icon = isLoading ? '⏳' : isSuccess ? '✓' : '✕';

  return (
    <div style={{
      background: colors.card,
      border: `1px solid ${borderColor}`,
      borderLeft: `4px solid ${borderColor}`,
      borderRadius: '8px',
      padding: toast.collapsed ? '8px 12px' : '12px 16px',
      marginBottom: '8px',
      minWidth: '280px',
      maxWidth: '380px',
      boxShadow: '0 4px 20px rgba(0,0,0,0.4)',
      transition: 'all 0.2s ease',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '10px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flex: 1, minWidth: 0 }}>
          <span style={{ fontSize: isLoading ? '14px' : '16px', flexShrink: 0, fontWeight: 700, color: borderColor }}>
            {icon}
          </span>
          {toast.collapsed ? (
            <span style={{ fontSize: '12px', color: colors.textMuted, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
              {isLoading ? formatElapsed(elapsed) : toast.message}
            </span>
          ) : (
            <span style={{ fontSize: '13px', color: colors.text, overflow: 'hidden', textOverflow: 'ellipsis' }}>
              {toast.message}
            </span>
          )}
        </div>
        <div style={{ display: 'flex', gap: '4px', flexShrink: 0 }}>
          {isLoading && (
            <button onClick={() => onToggle(toast.id)} style={{
              background: 'none', border: 'none', color: colors.textMuted, cursor: 'pointer',
              fontSize: '11px', padding: '2px 6px', borderRadius: '4px',
            }}>
              {toast.collapsed ? '▲' : '▼'}
            </button>
          )}
          {!isLoading && (
            <button onClick={() => onDismiss(toast.id)} style={{
              background: 'none', border: 'none', color: colors.textMuted, cursor: 'pointer',
              fontSize: '14px', padding: '2px 6px',
            }}>
              ×
            </button>
          )}
        </div>
      </div>

      {!toast.collapsed && isLoading && (
        <div style={{ marginTop: '8px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', color: colors.textMuted, marginBottom: '4px' }}>
            <span>Elapsed: {formatElapsed(elapsed)}</span>
            <span style={{ color: colors.primary }}>Processing...</span>
          </div>
          <div style={{ height: '3px', background: colors.border, borderRadius: '2px', overflow: 'hidden' }}>
            <div style={{
              height: '100%',
              background: `linear-gradient(90deg, ${colors.primary}, ${colors.primaryHover})`,
              borderRadius: '2px',
              width: '40%',
              animation: 'toastPulse 1.5s ease-in-out infinite alternate',
            }} />
          </div>
        </div>
      )}

      {!toast.collapsed && !isLoading && toast.detail && (
        <p style={{ fontSize: '11px', color: colors.textMuted, margin: '6px 0 0' }}>{toast.detail}</p>
      )}
    </div>
  );
}

export default function ToastContainer() {
  const { toasts, updateToast, removeToast } = useApp();

  // Auto-dismiss success/error toasts after 4 seconds
  useEffect(() => {
    const timers = toasts
      .filter(t => t.type !== 'loading')
      .map(t => setTimeout(() => removeToast(t.id), 4000));
    return () => timers.forEach(clearTimeout);
  }, [toasts, removeToast]);

  if (toasts.length === 0) return null;

  return (
    <>
      <style>{`
        @keyframes toastPulse {
          from { transform: translateX(-60%); }
          to { transform: translateX(150%); }
        }
      `}</style>
      <div style={{
        position: 'fixed',
        bottom: '20px',
        right: '20px',
        zIndex: 9999,
        display: 'flex',
        flexDirection: 'column-reverse',
      }}>
        {toasts.map(toast => (
          <ToastItem
            key={toast.id}
            toast={toast}
            onToggle={(id) => updateToast(id, { collapsed: !toast.collapsed })}
            onDismiss={removeToast}
          />
        ))}
      </div>
    </>
  );
}
