import React, { useCallback } from 'react';
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { executeQuery } from '../api';
import { useApp } from '../AppContext';
import { colors, layout, btn } from '../styles';

const BACKENDS = ['postgres', 'mongodb', 'neo4j'];

function ResultTable({ results }) {
  if (!results || results.length === 0) return <p style={{ color: colors.textMuted }}>No results</p>;
  const cols = Object.keys(results[0]);
  return (
    <div style={{ overflowX: 'auto', maxHeight: '400px', overflowY: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
        <thead>
          <tr>{cols.map(c => <th key={c} style={{ textAlign: 'left', padding: '8px 12px', borderBottom: `1px solid ${colors.border}`, color: colors.textMuted, fontWeight: 600, position: 'sticky', top: 0, background: colors.card }}>{c}</th>)}</tr>
        </thead>
        <tbody>
          {results.slice(0, 100).map((row, i) => (
            <tr key={i} style={{ borderBottom: `1px solid ${colors.border}` }}>
              {cols.map(c => <td key={c} style={{ padding: '6px 12px', color: colors.text }}>{typeof row[c] === 'number' ? row[c].toLocaleString() : String(row[c] ?? '')}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
      {results.length > 100 && <p style={{ color: colors.textMuted, fontSize: '12px', marginTop: '8px' }}>Showing 100 of {results.length} rows</p>}
    </div>
  );
}

function AutoChart({ results, queryId }) {
  if (!results || results.length === 0) return null;
  const cols = Object.keys(results[0]);
  const numericCols = cols.filter(c => typeof results[0][c] === 'number');
  const catCols = cols.filter(c => typeof results[0][c] === 'string');

  if (numericCols.length === 0) return null;

  const xKey = catCols[0] || cols[0];
  const yKey = numericCols.find(c => c.includes('cases') || c.includes('total')) || numericCols[0];
  const isTimeSeries = xKey.includes('year') || xKey.includes('decade') || xKey.includes('month');
  const data = results.slice(0, 50);

  if (isTimeSeries) {
    return (
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke={colors.border} />
          <XAxis dataKey={xKey} tick={{ fill: colors.textMuted, fontSize: 11 }} />
          <YAxis tick={{ fill: colors.textMuted, fontSize: 11 }} />
          <Tooltip contentStyle={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, color: colors.text }} />
          <Line type="monotone" dataKey={yKey} stroke={colors.primary} strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={data} layout={data.length > 15 ? 'vertical' : 'horizontal'} margin={{ left: data.length > 15 ? 80 : 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={colors.border} />
        {data.length > 15 ? (
          <>
            <XAxis type="number" tick={{ fill: colors.textMuted, fontSize: 11 }} />
            <YAxis dataKey={xKey} type="category" tick={{ fill: colors.textMuted, fontSize: 11 }} width={75} />
          </>
        ) : (
          <>
            <XAxis dataKey={xKey} tick={{ fill: colors.textMuted, fontSize: 11 }} />
            <YAxis tick={{ fill: colors.textMuted, fontSize: 11 }} />
          </>
        )}
        <Tooltip contentStyle={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, color: colors.text }} />
        <Bar dataKey={yKey} fill={colors.primary} radius={[4, 4, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

export default function Explorer() {
  const { queries, explorerState, updateExplorer, addToast, updateToast } = useApp();
  const { selectedQuery, selectedBackend, params, result, showQuery } = explorerState;
  const [loading, setLoading] = React.useState(false);

  const currentQuery = queries.find(q => q.query_id === selectedQuery);
  const availableBackends = currentQuery?.backends || BACKENDS;

  const handleExecute = useCallback(() => {
    setLoading(true);
    updateExplorer({ result: null });

    const castParams = {};
    (currentQuery?.params || []).forEach(p => {
      const val = params[p.name];
      if (val !== undefined && val !== '') {
        castParams[p.name] = p.type === 'int' ? parseInt(val, 10) : val;
      }
    });

    // Show toast only after 2s delay
    let toastId = null;
    const toastTimer = setTimeout(() => {
      toastId = addToast(`Running ${selectedQuery} on ${selectedBackend}...`);
    }, 2000);

    executeQuery(selectedQuery, selectedBackend, castParams)
      .then(data => {
        updateExplorer({ result: data });
        clearTimeout(toastTimer);
        if (toastId) {
          updateToast(toastId, {
            type: 'success',
            message: `${selectedQuery} completed in ${data.execution_time_ms}ms`,
          });
        }
      })
      .catch(err => {
        const errorMsg = err.response?.data?.detail || err.message;
        updateExplorer({ result: { error: errorMsg } });
        clearTimeout(toastTimer);
        if (toastId) {
          updateToast(toastId, { type: 'error', message: `${selectedQuery} failed`, detail: errorMsg });
        }
      })
      .finally(() => setLoading(false));
  }, [selectedQuery, selectedBackend, params, currentQuery, updateExplorer, addToast, updateToast]);

  return (
    <div>
      <h2 style={{ fontSize: '22px', fontWeight: 700, marginBottom: '4px' }}>OLAP Explorer</h2>
      <p style={{ color: colors.textMuted, fontSize: '14px', marginBottom: '24px' }}>
        Execute decision support queries on any backend
      </p>

      <div style={layout.card}>
        <div style={{ display: 'flex', gap: '12px', marginBottom: '16px', flexWrap: 'wrap' }}>
          {BACKENDS.map(b => (
            <button key={b} onClick={() => updateExplorer({ selectedBackend: b })}
              disabled={!availableBackends.includes(b)}
              style={{
                ...btn.secondary,
                background: selectedBackend === b ? colors.primary : 'transparent',
                color: selectedBackend === b ? '#fff' : availableBackends.includes(b) ? colors.textMuted : colors.border,
                opacity: availableBackends.includes(b) ? 1 : 0.4,
                textTransform: 'capitalize',
              }}>
              {b}
            </button>
          ))}
        </div>

        <div style={{ display: 'flex', gap: '12px', alignItems: 'flex-end', flexWrap: 'wrap' }}>
          <div style={{ flex: 1, minWidth: '250px' }}>
            <label style={{ display: 'block', fontSize: '12px', color: colors.textMuted, marginBottom: '4px' }}>Query</label>
            <select value={selectedQuery} onChange={e => updateExplorer({ selectedQuery: e.target.value, result: null })}
              style={{ width: '100%', padding: '8px 12px', background: colors.bg, color: colors.text, border: `1px solid ${colors.border}`, borderRadius: '6px', fontSize: '13px' }}>
              {queries.map(q => (
                <option key={q.query_id} value={q.query_id}>{q.query_id}: {q.name}</option>
              ))}
            </select>
          </div>

          {currentQuery?.params?.map(p => (
            <div key={p.name} style={{ minWidth: '120px' }}>
              <label style={{ display: 'block', fontSize: '12px', color: colors.textMuted, marginBottom: '4px' }}>{p.name}</label>
              <input
                placeholder={String(p.default)}
                value={params[p.name] || ''}
                onChange={e => updateExplorer({ params: { ...params, [p.name]: e.target.value } })}
                style={{ width: '100%', padding: '8px 12px', background: colors.bg, color: colors.text, border: `1px solid ${colors.border}`, borderRadius: '6px', fontSize: '13px', boxSizing: 'border-box' }}
              />
            </div>
          ))}

          <button onClick={handleExecute} disabled={loading} style={{ ...btn.primary, opacity: loading ? 0.6 : 1 }}>
            {loading ? 'Executing...' : 'Execute'}
          </button>
        </div>

        {currentQuery && (
          <p style={{ fontSize: '12px', color: colors.textMuted, marginTop: '8px' }}>
            {currentQuery.olap_operation} - {currentQuery.description}
          </p>
        )}
      </div>

      {result && !result.error && (
        <>
          <div style={{ display: 'flex', gap: '12px', marginBottom: '16px' }}>
            <div style={{ ...layout.card, flex: 1, textAlign: 'center' }}>
              <p style={{ color: colors.textMuted, fontSize: '12px', margin: 0 }}>Rows</p>
              <p style={{ fontSize: '24px', fontWeight: 700, margin: '4px 0 0' }}>{result.row_count}</p>
            </div>
            <div style={{ ...layout.card, flex: 1, textAlign: 'center' }}>
              <p style={{ color: colors.textMuted, fontSize: '12px', margin: 0 }}>Execution Time</p>
              <p style={{ fontSize: '24px', fontWeight: 700, margin: '4px 0 0' }}>{result.execution_time_ms}ms</p>
            </div>
            <div style={{ ...layout.card, flex: 1, textAlign: 'center' }}>
              <p style={{ color: colors.textMuted, fontSize: '12px', margin: 0 }}>Backend</p>
              <p style={{ fontSize: '24px', fontWeight: 700, margin: '4px 0 0', textTransform: 'capitalize' }}>{result.backend}</p>
            </div>
          </div>

          <div style={layout.card}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
              <p style={{ fontWeight: 600, fontSize: '14px', margin: 0 }}>Results</p>
              <button onClick={() => updateExplorer({ showQuery: !showQuery })} style={btn.secondary}>
                {showQuery ? 'Hide Query' : 'Show Query'}
              </button>
            </div>
            {showQuery && (
              <pre style={{ background: colors.bg, padding: '16px', borderRadius: '8px', overflow: 'auto', fontSize: '12px', color: colors.textMuted, marginBottom: '16px', maxHeight: '300px', border: `1px solid ${colors.border}` }}>
                {result.query_text}
              </pre>
            )}
            <AutoChart results={result.results} queryId={selectedQuery} />
            <div style={{ marginTop: '16px' }}>
              <ResultTable results={result.results} />
            </div>
          </div>
        </>
      )}

      {result?.error && (
        <div style={{ ...layout.card, borderColor: colors.error }}>
          <p style={{ color: colors.error, fontWeight: 600 }}>Error</p>
          <p style={{ color: colors.textMuted }}>{result.error}</p>
        </div>
      )}
    </div>
  );
}
