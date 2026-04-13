import React, { useState, useEffect } from 'react';
import { BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { getHealth, getMetadata, executeQuery } from '../api';
import { colors, layout, chartColors } from '../styles';

function StatCard({ label, value, sub }) {
  return (
    <div style={layout.card}>
      <p style={{ fontSize: '12px', color: colors.textMuted, margin: 0, textTransform: 'uppercase', letterSpacing: '0.5px' }}>{label}</p>
      <p style={{ fontSize: '28px', fontWeight: 700, margin: '8px 0 4px', color: colors.text }}>{value}</p>
      {sub && <p style={{ fontSize: '12px', color: colors.textMuted, margin: 0 }}>{sub}</p>}
    </div>
  );
}

export default function Home() {
  const [health, setHealth] = useState(null);
  const [pgMeta, setPgMeta] = useState(null);
  const [mongoMeta, setMongoMeta] = useState(null);
  const [neo4jMeta, setNeo4jMeta] = useState(null);
  const [diseaseData, setDiseaseData] = useState([]);
  const [decadeData, setDecadeData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      getHealth().catch(() => null),
      getMetadata('postgres').catch(() => null),
      getMetadata('mongodb').catch(() => null),
      getMetadata('neo4j').catch(() => null),
      executeQuery('Q1', 'postgres').catch(() => null),
    ]).then(([h, pg, mongo, neo4j, q1]) => {
      setHealth(h);
      setPgMeta(pg);
      setMongoMeta(mongo);
      setNeo4jMeta(neo4j);

      if (q1 && q1.results) {
        // Aggregate by disease for bar chart
        const byDisease = {};
        const byDecade = {};
        q1.results.forEach(r => {
          byDisease[r.disease_name] = (byDisease[r.disease_name] || 0) + r.total_cases;
          byDecade[r.decade] = (byDecade[r.decade] || 0) + r.total_cases;
        });
        setDiseaseData(Object.entries(byDisease).map(([name, cases]) => ({ name, cases })).sort((a, b) => b.cases - a.cases));
        setDecadeData(Object.entries(byDecade).map(([decade, cases]) => ({ decade: String(decade), cases })).sort((a, b) => a.decade.localeCompare(b.decade)));
      }
      setLoading(false);
    });
  }, []);

  const pgRows = pgMeta?.tables?.find(t => t.table_name === 'fact_disease_incidence')?.estimated_row_count || 0;
  const mongoDocs = mongoMeta?.collections?.find(c => c.collection === 'disease_observations')?.document_count || 0;
  const neo4jNodes = neo4jMeta?.node_labels?.reduce((s, n) => s + n.count, 0) || 0;

  if (loading) return <p style={{ color: colors.textMuted }}>Loading dashboard...</p>;

  return (
    <div>
      <h2 style={{ fontSize: '22px', fontWeight: 700, marginBottom: '4px' }}>Overview</h2>
      <p style={{ color: colors.textMuted, fontSize: '14px', marginBottom: '24px' }}>
        System health and data summary across all backends
      </p>

      <div style={layout.grid4}>
        <StatCard label="PostgreSQL Rows" value={pgRows.toLocaleString()} sub={pgMeta?.database_size} />
        <StatCard label="MongoDB Documents" value={mongoDocs.toLocaleString()} sub={`${mongoMeta?.collections?.length || 0} collections`} />
        <StatCard label="Neo4j Nodes" value={neo4jNodes.toLocaleString()} sub={`${neo4jMeta?.node_labels?.length || 0} labels`} />
        <StatCard label="Diseases Tracked" value="8" sub="1930-2010" />
      </div>

      {health && (
        <div style={{ ...layout.card, marginTop: '8px' }}>
          <p style={{ fontSize: '14px', fontWeight: 600, marginBottom: '12px' }}>
            System Status: <span style={{ color: health.status === 'ok' ? colors.success : colors.warning }}>{health.status.toUpperCase()}</span>
            <span style={{ color: colors.textMuted, fontWeight: 400, fontSize: '12px', marginLeft: '8px' }}>
              ({health.response_time_ms}ms)
            </span>
          </p>
          <div style={{ display: 'flex', gap: '24px' }}>
            {Object.entries(health.services).map(([name, svc]) => (
              <div key={name} style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <span style={{ display: 'inline-block', width: 10, height: 10, borderRadius: '50%', background: svc.status === 'healthy' ? colors.success : colors.error }} />
                <span style={{ textTransform: 'capitalize', fontSize: '13px' }}>{name}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div style={layout.grid2}>
        <div style={layout.card}>
          <p style={{ fontSize: '14px', fontWeight: 600, marginBottom: '16px' }}>Cases by Disease</p>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={diseaseData} layout="vertical" margin={{ left: 80 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={colors.border} />
              <XAxis type="number" tick={{ fill: colors.textMuted, fontSize: 11 }} />
              <YAxis dataKey="name" type="category" tick={{ fill: colors.textMuted, fontSize: 11 }} width={75} />
              <Tooltip contentStyle={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, color: colors.text }} />
              <Bar dataKey="cases" fill={colors.primary} radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div style={layout.card}>
          <p style={{ fontSize: '14px', fontWeight: 600, marginBottom: '16px' }}>Cases by Decade</p>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={decadeData}>
              <CartesianGrid strokeDasharray="3 3" stroke={colors.border} />
              <XAxis dataKey="decade" tick={{ fill: colors.textMuted, fontSize: 11 }} />
              <YAxis tick={{ fill: colors.textMuted, fontSize: 11 }} />
              <Tooltip contentStyle={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, color: colors.text }} />
              <Line type="monotone" dataKey="cases" stroke={colors.primary} strokeWidth={2} dot={{ fill: colors.primary, r: 4 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div style={layout.card}>
        <p style={{ fontSize: '14px', fontWeight: 600, marginBottom: '16px' }}>Disease Distribution</p>
        <div style={{ display: 'flex', alignItems: 'center', gap: '32px' }}>
          <ResponsiveContainer width="50%" height={280}>
            <PieChart>
              <Pie data={diseaseData} dataKey="cases" nameKey="name" cx="50%" cy="50%" outerRadius={100} innerRadius={40}>
                {diseaseData.map((_, i) => <Cell key={i} fill={chartColors[i % chartColors.length]} />)}
              </Pie>
              <Tooltip contentStyle={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, color: colors.text }} />
            </PieChart>
          </ResponsiveContainer>
          <div style={{ flex: 1 }}>
            {diseaseData.map((d, i) => (
              <div key={d.name} style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '8px' }}>
                <span style={{ display: 'inline-block', width: 12, height: 12, borderRadius: 3, background: chartColors[i % chartColors.length], flexShrink: 0 }} />
                <span style={{ fontSize: '13px', flex: 1 }}>{d.name}</span>
                <span style={{ fontSize: '13px', color: colors.textMuted, fontWeight: 600 }}>
                  {diseaseData.length > 0 ? Math.round(d.cases / diseaseData.reduce((s, x) => s + x.cases, 0) * 100) : 0}%
                </span>
                <span style={{ fontSize: '12px', color: colors.textMuted, minWidth: '80px', textAlign: 'right' }}>
                  {d.cases.toLocaleString()}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
