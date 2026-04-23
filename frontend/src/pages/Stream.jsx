import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { useStream } from '../StreamContext';
import { startDemo, getDemoStatus } from '../api';
import { colors, layout, btn } from '../styles';

export default function Stream() {
  const { connected, totalCount, events, anomalies, windowRef, connect, disconnect } = useStream();
  const [rateData, setRateData] = useState([]);
  const [demoRunning, setDemoRunning] = useState(false);
  const [demoMessage, setDemoMessage] = useState('');

  // Update rate chart every second
  useEffect(() => {
    const interval = setInterval(() => {
      const now = Date.now();
      windowRef.current = windowRef.current.filter(t => now - t < 10000);
      const rate = windowRef.current.length / 10;
      setRateData(prev => [
        ...prev.slice(-30),
        { time: new Date().toLocaleTimeString().slice(0, 8), rate: Math.round(rate * 10) / 10 },
      ]);
    }, 1000);
    return () => clearInterval(interval);
  }, [windowRef]);

  // Poll demo status while running
  useEffect(() => {
    if (!demoRunning) return;
    const interval = setInterval(() => {
      getDemoStatus().then(s => {
        if (!s.running) {
          setDemoRunning(false);
          setDemoMessage('Streaming completed');
          setTimeout(() => setDemoMessage(''), 3000);
        }
      }).catch(() => {});
    }, 2000);
    return () => clearInterval(interval);
  }, [demoRunning]);

  const handleStartDemo = async () => {
    // Auto-connect if not connected
    if (!connected) {
      connect();
    }
    setDemoRunning(true);
    setDemoMessage('');
    try {
      const res = await startDemo(500);
      if (res.status === 'already_running') {
        setDemoMessage('Already streaming');
      } else {
        setDemoMessage('Streaming 500 events...');
      }
    } catch (err) {
      setDemoRunning(false);
      setDemoMessage('Failed to start demo: ' + (err.response?.data?.detail || err.message));
    }
  };

  return (
    <div>
      <h2 style={{ fontSize: '22px', fontWeight: 700, marginBottom: '4px' }}>Live Stream Monitor</h2>
      <p style={{ color: colors.textMuted, fontSize: '14px', marginBottom: '24px' }}>
        Real-time disease event feed via Server-Sent Events
      </p>

      <div style={{ ...layout.card, display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '12px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <span style={{
            display: 'inline-block', width: 10, height: 10, borderRadius: '50%',
            background: connected ? colors.success : colors.error,
          }} />
          <span style={{ fontWeight: 600, fontSize: '14px' }}>
            {connected ? 'Connected' : 'Disconnected'}
          </span>
          <span style={{ color: colors.textMuted, fontSize: '13px' }}>
            {totalCount.toLocaleString()} events received
          </span>
        </div>
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
          <button onClick={connect} disabled={connected} style={{ ...btn.primary, opacity: connected ? 0.5 : 1 }}>
            Connect
          </button>
          <button onClick={disconnect} disabled={!connected} style={{ ...btn.secondary, opacity: !connected ? 0.5 : 1 }}>
            Disconnect
          </button>
          <div style={{ width: '1px', height: '24px', background: colors.border, margin: '0 4px' }} />
          <button
            onClick={handleStartDemo}
            disabled={demoRunning}
            style={{
              ...btn.primary,
              background: demoRunning ? colors.warning : '#22c55e',
              opacity: demoRunning ? 0.7 : 1,
            }}
          >
            {demoRunning ? 'Streaming...' : 'Stream Sample Data'}
          </button>
          {demoMessage && (
            <span style={{ fontSize: '12px', color: demoRunning ? colors.warning : colors.success }}>
              {demoMessage}
            </span>
          )}
        </div>
      </div>

      <div style={layout.grid2}>
        <div style={layout.card}>
          <p style={{ fontWeight: 600, fontSize: '14px', marginBottom: '12px' }}>Events per Second</p>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={rateData}>
              <CartesianGrid strokeDasharray="3 3" stroke={colors.border} />
              <XAxis dataKey="time" tick={{ fill: colors.textMuted, fontSize: 10 }} />
              <YAxis tick={{ fill: colors.textMuted, fontSize: 11 }} />
              <Tooltip contentStyle={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, color: colors.text }} />
              <Line type="monotone" dataKey="rate" stroke={colors.primary} strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div style={layout.card}>
          <p style={{ fontWeight: 600, fontSize: '14px', marginBottom: '12px' }}>
            Anomaly Alerts <span style={{ color: colors.error, fontSize: '12px' }}>({anomalies.length})</span>
          </p>
          <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
            {anomalies.length === 0 ? (
              <p style={{ color: colors.textMuted, fontSize: '13px' }}>No anomalies detected yet</p>
            ) : (
              anomalies.map((a, i) => (
                <div key={i} style={{ padding: '8px 10px', background: 'rgba(239,68,68,0.1)', borderRadius: '6px', marginBottom: '6px', borderLeft: `3px solid ${colors.error}` }}>
                  <p style={{ fontSize: '12px', fontWeight: 600, margin: 0 }}>
                    {a.disease} {a.state} (Week {a.epi_week})
                  </p>
                  <p style={{ fontSize: '11px', color: colors.textMuted, margin: '2px 0 0' }}>
                    {a.cases} cases, rate: {a.incidence_rate} | {a.timestamp}
                  </p>
                </div>
              ))
            )}
          </div>
        </div>
      </div>

      <div style={layout.card}>
        <p style={{ fontWeight: 600, fontSize: '14px', marginBottom: '12px' }}>Recent Events</p>
        <div style={{ maxHeight: '400px', overflowY: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
            <thead>
              <tr>
                {['Disease', 'State', 'Epi Week', 'Cases', 'Incidence Rate'].map(h => (
                  <th key={h} style={{ textAlign: 'left', padding: '6px 10px', borderBottom: `1px solid ${colors.border}`, color: colors.textMuted, fontWeight: 600, position: 'sticky', top: 0, background: colors.card }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {events.map((e, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${colors.border}` }}>
                  <td style={{ padding: '4px 10px' }}>{e.disease}</td>
                  <td style={{ padding: '4px 10px' }}>{e.state}</td>
                  <td style={{ padding: '4px 10px' }}>{e.epi_week}</td>
                  <td style={{ padding: '4px 10px' }}>{e.cases}</td>
                  <td style={{ padding: '4px 10px', color: e.incidence_rate > 50 ? colors.error : colors.text }}>
                    {e.incidence_rate}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {events.length === 0 && (
            <p style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
              Click "Stream Sample Data" to begin the real-time feed
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
