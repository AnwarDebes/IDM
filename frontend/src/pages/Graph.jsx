import React, { useState } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { executeQuery } from '../api';
import { colors, layout, btn, chartColors } from '../styles';

const GRAPH_QUERIES = [
  { id: 'Q11', name: 'Disease Spread by State Borders', params: [{ name: 'disease', default: 'MEASLES' }, { name: 'threshold', default: 50 }] },
  { id: 'Q12', name: 'State Similarity by Disease Profile', params: [] },
  { id: 'Q13', name: 'Disease Centrality & Coverage', params: [] },
];

export default function Graph() {
  const [selectedQuery, setSelectedQuery] = useState('Q11');
  const [params, setParams] = useState({});
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);

  const currentQuery = GRAPH_QUERIES.find(q => q.id === selectedQuery);

  const handleExecute = () => {
    setLoading(true);
    setResult(null);
    const castParams = {};
    (currentQuery?.params || []).forEach(p => {
      const val = params[p.name];
      if (val !== undefined && val !== '') {
        castParams[p.name] = typeof p.default === 'number' ? parseInt(val, 10) : val;
      }
    });
    executeQuery(selectedQuery, 'neo4j', castParams)
      .then(setResult)
      .catch(err => setResult({ error: err.response?.data?.detail || err.message }))
      .finally(() => setLoading(false));
  };

  return (
    <div>
      <h2 style={{ fontSize: '22px', fontWeight: 700, marginBottom: '4px' }}>Graph Explorer</h2>
      <p style={{ color: colors.textMuted, fontSize: '14px', marginBottom: '24px' }}>
        Neo4j-exclusive graph analytics border spread, state similarity, disease centrality
      </p>

      <div style={{ ...layout.card, display: 'flex', gap: '12px', alignItems: 'flex-end', flexWrap: 'wrap' }}>
        <div style={{ flex: 1, minWidth: '250px' }}>
          <label style={{ display: 'block', fontSize: '12px', color: colors.textMuted, marginBottom: '4px' }}>Graph Query</label>
          <select value={selectedQuery} onChange={e => { setSelectedQuery(e.target.value); setResult(null); }}
            style={{ width: '100%', padding: '8px 12px', background: colors.bg, color: colors.text, border: `1px solid ${colors.border}`, borderRadius: '6px', fontSize: '13px' }}>
            {GRAPH_QUERIES.map(q => (
              <option key={q.id} value={q.id}>{q.id}: {q.name}</option>
            ))}
          </select>
        </div>
        {currentQuery?.params?.map(p => (
          <div key={p.name} style={{ minWidth: '120px' }}>
            <label style={{ display: 'block', fontSize: '12px', color: colors.textMuted, marginBottom: '4px' }}>{p.name}</label>
            <input
              placeholder={String(p.default)}
              value={params[p.name] || ''}
              onChange={e => setParams({ ...params, [p.name]: e.target.value })}
              style={{ width: '100%', padding: '8px 12px', background: colors.bg, color: colors.text, border: `1px solid ${colors.border}`, borderRadius: '6px', fontSize: '13px', boxSizing: 'border-box' }}
            />
          </div>
        ))}
        <button onClick={handleExecute} disabled={loading} style={{ ...btn.primary, opacity: loading ? 0.6 : 1 }}>
          {loading ? 'Running...' : 'Execute'}
        </button>
      </div>

      {result && !result.error && (
        <>
          <div style={{ display: 'flex', gap: '12px', marginBottom: '16px', marginTop: '8px' }}>
            <div style={{ ...layout.card, flex: 1, textAlign: 'center' }}>
              <p style={{ color: colors.textMuted, fontSize: '12px', margin: 0 }}>Rows</p>
              <p style={{ fontSize: '24px', fontWeight: 700, margin: '4px 0 0' }}>{result.row_count}</p>
            </div>
            <div style={{ ...layout.card, flex: 1, textAlign: 'center' }}>
              <p style={{ color: colors.textMuted, fontSize: '12px', margin: 0 }}>Execution Time</p>
              <p style={{ fontSize: '24px', fontWeight: 700, margin: '4px 0 0' }}>{result.execution_time_ms}ms</p>
            </div>
          </div>

          {/* Q13 visualization - bar chart of disease coverage */}
          {selectedQuery === 'Q13' && result.results.length > 0 && (
            <div style={layout.card}>
              <p style={{ fontWeight: 600, fontSize: '14px', marginBottom: '12px' }}>Disease Coverage (%)</p>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={result.results}>
                  <CartesianGrid strokeDasharray="3 3" stroke={colors.border} />
                  <XAxis dataKey="disease_name" tick={{ fill: colors.textMuted, fontSize: 11 }} />
                  <YAxis tick={{ fill: colors.textMuted, fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, color: colors.text }} />
                  <Bar dataKey="coverage_pct" fill={colors.neo4j} radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Q11 visualization - spread table */}
          {selectedQuery === 'Q11' && result.results.length > 0 && (
            <div style={layout.card}>
              <p style={{ fontWeight: 600, fontSize: '14px', marginBottom: '12px' }}>Border Spread Patterns</p>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={result.results.reduce((acc, r) => {
                  const key = `${r.lag_months}mo`;
                  const existing = acc.find(a => a.lag === key);
                  if (existing) { existing.count += 1; }
                  else { acc.push({ lag: key, count: 1 }); }
                  return acc;
                }, [])}>
                  <CartesianGrid strokeDasharray="3 3" stroke={colors.border} />
                  <XAxis dataKey="lag" tick={{ fill: colors.textMuted, fontSize: 11 }} label={{ value: 'Lag (months)', position: 'insideBottom', offset: -5, fill: colors.textMuted }} />
                  <YAxis tick={{ fill: colors.textMuted, fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, color: colors.text }} />
                  <Bar dataKey="count" fill={colors.neo4j} name="State pairs" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Q12 - state profiles */}
          {selectedQuery === 'Q12' && result.results.length > 0 && (
            <div style={layout.card}>
              <p style={{ fontWeight: 600, fontSize: '14px', marginBottom: '12px' }}>State Disease Profiles</p>
              <div style={{ overflowX: 'auto', maxHeight: '400px', overflowY: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                  <thead>
                    <tr>
                      <th style={{ textAlign: 'left', padding: '6px 10px', borderBottom: `1px solid ${colors.border}`, color: colors.textMuted, position: 'sticky', top: 0, background: colors.card }}>State</th>
                      <th style={{ textAlign: 'left', padding: '6px 10px', borderBottom: `1px solid ${colors.border}`, color: colors.textMuted, position: 'sticky', top: 0, background: colors.card }}>Disease Profile</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.results.map((r, i) => (
                      <tr key={i} style={{ borderBottom: `1px solid ${colors.border}` }}>
                        <td style={{ padding: '6px 10px', fontWeight: 600 }}>{r.state} ({r.code})</td>
                        <td style={{ padding: '6px 10px' }}>
                          <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
                            {(r.profile || []).map((p, j) => (
                              <span key={j} style={{
                                display: 'inline-block', padding: '2px 8px', borderRadius: '4px', fontSize: '11px',
                                background: `${chartColors[j % chartColors.length]}22`, color: chartColors[j % chartColors.length],
                              }}>
                                {p.disease}: {p.pct}%
                              </span>
                            ))}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Generic results table */}
          <div style={layout.card}>
            <details open={selectedQuery !== 'Q12'}>
              <summary style={{ cursor: 'pointer', fontWeight: 600, fontSize: '14px', marginBottom: '12px' }}>Raw Results</summary>
              <div style={{ overflowX: 'auto', maxHeight: '300px', overflowY: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                  <thead>
                    <tr>
                      {result.results.length > 0 && Object.keys(result.results[0]).map(c => (
                        <th key={c} style={{ textAlign: 'left', padding: '6px 8px', borderBottom: `1px solid ${colors.border}`, color: colors.textMuted, fontWeight: 600, position: 'sticky', top: 0, background: colors.card }}>{c}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.results.slice(0, 50).map((row, i) => (
                      <tr key={i}>
                        {Object.values(row).map((v, j) => (
                          <td key={j} style={{ padding: '4px 8px', borderBottom: `1px solid ${colors.border}` }}>
                            {typeof v === 'object' ? JSON.stringify(v) : typeof v === 'number' ? v.toLocaleString() : String(v ?? '')}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </details>

            <details style={{ marginTop: '12px' }}>
              <summary style={{ cursor: 'pointer', fontSize: '12px', color: colors.textMuted }}>View Cypher query</summary>
              <pre style={{ background: colors.bg, padding: '12px', borderRadius: '6px', fontSize: '11px', color: colors.textMuted, overflow: 'auto', maxHeight: '200px', marginTop: '8px', border: `1px solid ${colors.border}` }}>
                {result.query_text}
              </pre>
            </details>
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
