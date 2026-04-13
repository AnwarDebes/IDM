import React, { useCallback } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend } from 'recharts';
import { compareQuery } from '../api';
import { useApp } from '../AppContext';
import { colors, layout, btn } from '../styles';

const backendColors = { postgres: colors.postgres, mongodb: colors.mongodb, neo4j: colors.neo4j };

function BackendPanel({ name, data }) {
  if (!data) return null;
  const isError = data.status === 'error';
  return (
    <div style={{ ...layout.card, borderColor: isError ? colors.error : colors.border }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
        <h3 style={{ fontSize: '16px', fontWeight: 700, margin: 0, textTransform: 'capitalize', color: backendColors[name] || colors.text }}>{name}</h3>
        <span style={{ fontSize: '20px', fontWeight: 700, color: isError ? colors.error : colors.success }}>
          {isError ? 'ERROR' : `${data.execution_time_ms}ms`}
        </span>
      </div>

      {isError ? (
        <p style={{ color: colors.error, fontSize: '13px' }}>{data.error}</p>
      ) : (
        <>
          <p style={{ color: colors.textMuted, fontSize: '12px', marginBottom: '12px' }}>{data.row_count} rows returned</p>
          <div style={{ overflowX: 'auto', maxHeight: '250px', overflowY: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
              <thead>
                <tr>
                  {data.results.length > 0 && Object.keys(data.results[0]).map(c => (
                    <th key={c} style={{ textAlign: 'left', padding: '6px 8px', borderBottom: `1px solid ${colors.border}`, color: colors.textMuted, fontWeight: 600, position: 'sticky', top: 0, background: colors.card }}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.results.slice(0, 10).map((row, i) => (
                  <tr key={i}>
                    {Object.values(row).map((v, j) => (
                      <td key={j} style={{ padding: '4px 8px', borderBottom: `1px solid ${colors.border}`, color: colors.text }}>
                        {typeof v === 'number' ? v.toLocaleString() : String(v ?? '')}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <details style={{ marginTop: '12px' }}>
            <summary style={{ cursor: 'pointer', fontSize: '12px', color: colors.textMuted }}>View query</summary>
            <pre style={{ background: colors.bg, padding: '12px', borderRadius: '6px', fontSize: '11px', color: colors.textMuted, overflow: 'auto', maxHeight: '200px', marginTop: '8px', border: `1px solid ${colors.border}` }}>
              {data.query_text}
            </pre>
          </details>
        </>
      )}
    </div>
  );
}

export default function Compare() {
  const { queries, compareState, updateCompare, addToast, updateToast } = useApp();
  const { selectedQuery, result, allTimings } = compareState;
  const [loading, setLoading] = React.useState(false);

  const handleCompare = useCallback(async () => {
    setLoading(true);
    updateCompare({ result: null });

    let toastId = null;
    const toastTimer = setTimeout(() => {
      toastId = addToast(`Comparing ${selectedQuery} across all backends...`);
    }, 2000);

    try {
      const data = await compareQuery(selectedQuery);
      updateCompare({ result: data });

      // Add to running timings list
      const existing = allTimings.filter(t => t.query !== selectedQuery);
      const entry = { query: selectedQuery };
      Object.entries(data.backends).forEach(([b, d]) => {
        entry[b] = d.status === 'success' ? d.execution_time_ms : 0;
      });
      updateCompare({ allTimings: [...existing, entry].sort((a, b) => a.query.localeCompare(b.query)) });

      clearTimeout(toastTimer);
      if (toastId) {
        updateToast(toastId, { type: 'success', message: `${selectedQuery} comparison complete` });
      }
    } catch (err) {
      updateCompare({ result: { error: err.message } });
      clearTimeout(toastTimer);
      if (toastId) {
        updateToast(toastId, { type: 'error', message: `${selectedQuery} comparison failed` });
      }
    }
    setLoading(false);
  }, [selectedQuery, allTimings, updateCompare, addToast, updateToast]);

  const handleRunAll = useCallback(async () => {
    setLoading(true);
    const toastId = addToast('Running all Q1-Q10 comparisons...');
    const timings = [];
    const commonQueries = queries.filter(q => q.backends.length === 3);

    for (let i = 0; i < commonQueries.length; i++) {
      const q = commonQueries[i];
      updateToast(toastId, { message: `Running ${q.query_id} (${i + 1}/${commonQueries.length})...` });
      try {
        const data = await compareQuery(q.query_id);
        const entry = { query: q.query_id };
        Object.entries(data.backends).forEach(([b, d]) => {
          entry[b] = d.status === 'success' ? d.execution_time_ms : 0;
        });
        timings.push(entry);
      } catch {}
    }

    updateCompare({ allTimings: timings.sort((a, b) => a.query.localeCompare(b.query)) });
    updateToast(toastId, { type: 'success', message: `All ${commonQueries.length} queries compared` });
    setLoading(false);
  }, [queries, updateCompare, addToast, updateToast]);

  return (
    <div>
      <h2 style={{ fontSize: '22px', fontWeight: 700, marginBottom: '4px' }}>Comparison Dashboard</h2>
      <p style={{ color: colors.textMuted, fontSize: '14px', marginBottom: '24px' }}>
        Execute the same query across all backends and compare performance
      </p>

      <div style={{ ...layout.card, display: 'flex', gap: '12px', alignItems: 'flex-end', flexWrap: 'wrap' }}>
        <div style={{ flex: 1, minWidth: '250px' }}>
          <label style={{ display: 'block', fontSize: '12px', color: colors.textMuted, marginBottom: '4px' }}>Query</label>
          <select value={selectedQuery} onChange={e => updateCompare({ selectedQuery: e.target.value })}
            style={{ width: '100%', padding: '8px 12px', background: colors.bg, color: colors.text, border: `1px solid ${colors.border}`, borderRadius: '6px', fontSize: '13px' }}>
            {queries.map(q => (
              <option key={q.query_id} value={q.query_id}>{q.query_id}: {q.name}</option>
            ))}
          </select>
        </div>
        <button onClick={handleCompare} disabled={loading} style={{ ...btn.primary, opacity: loading ? 0.6 : 1 }}>
          {loading ? 'Comparing...' : 'Compare All Backends'}
        </button>
        <button onClick={handleRunAll} disabled={loading} style={{ ...btn.secondary, opacity: loading ? 0.6 : 1 }}>
          Run All Q1-Q10
        </button>
      </div>

      {result && !result.error && (
        <>
          <div style={{ ...layout.card, marginTop: '8px' }}>
            <p style={{ fontWeight: 600, fontSize: '14px', marginBottom: '16px' }}>Execution Time Comparison</p>
            <ResponsiveContainer width="100%" height={80}>
              <BarChart data={[{
                ...Object.fromEntries(Object.entries(result.backends).map(([b, d]) => [b, d.execution_time_ms])),
              }]} layout="vertical">
                <XAxis type="number" tick={{ fill: colors.textMuted, fontSize: 11 }} />
                <YAxis type="category" dataKey="name" hide />
                <Tooltip contentStyle={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, color: colors.text }} />
                <Legend />
                {Object.keys(result.backends).map(b => (
                  <Bar key={b} dataKey={b} fill={backendColors[b]} name={b} radius={[0, 4, 4, 0]} />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div style={layout.grid3}>
            {Object.entries(result.backends).map(([name, data]) => (
              <BackendPanel key={name} name={name} data={data} />
            ))}
          </div>
        </>
      )}

      {result?.error && (
        <div style={{ ...layout.card, borderColor: colors.error }}>
          <p style={{ color: colors.error }}>{result.error}</p>
        </div>
      )}

      {allTimings.length > 1 && (
        <div style={layout.card}>
          <p style={{ fontWeight: 600, fontSize: '14px', marginBottom: '16px' }}>All Queries — Execution Time (ms)</p>
          <ResponsiveContainer width="100%" height={Math.max(200, allTimings.length * 40)}>
            <BarChart data={allTimings} layout="vertical" margin={{ left: 40 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={colors.border} />
              <XAxis type="number" tick={{ fill: colors.textMuted, fontSize: 11 }} label={{ value: 'ms', position: 'insideRight', fill: colors.textMuted }} />
              <YAxis dataKey="query" type="category" tick={{ fill: colors.textMuted, fontSize: 11 }} width={35} />
              <Tooltip contentStyle={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, color: colors.text }} />
              <Legend />
              <Bar dataKey="postgres" fill={colors.postgres} name="PostgreSQL" />
              <Bar dataKey="mongodb" fill={colors.mongodb} name="MongoDB" />
              <Bar dataKey="neo4j" fill={colors.neo4j} name="Neo4j" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
