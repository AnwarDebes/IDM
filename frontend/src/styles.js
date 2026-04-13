/* Shared style constants and helpers */

export const colors = {
  bg: '#0f172a',
  sidebar: '#1e293b',
  card: '#1e293b',
  cardHover: '#334155',
  text: '#e2e8f0',
  textMuted: '#94a3b8',
  primary: '#3b82f6',
  primaryHover: '#2563eb',
  success: '#22c55e',
  warning: '#f59e0b',
  error: '#ef4444',
  border: '#334155',
  postgres: '#336791',
  mongodb: '#4db33d',
  neo4j: '#018bff',
};

export const chartColors = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#a855f7', '#ec4899', '#14b8a6', '#f97316'];

export const layout = {
  sidebar: {
    position: 'fixed', left: 0, top: 0, bottom: 0, width: '240px',
    background: colors.sidebar, padding: '24px 0', display: 'flex',
    flexDirection: 'column', zIndex: 100, borderRight: `1px solid ${colors.border}`,
  },
  main: {
    marginLeft: '240px', padding: '24px 32px', minHeight: '100vh',
    background: colors.bg, color: colors.text,
  },
  card: {
    background: colors.card, borderRadius: '12px', padding: '20px',
    border: `1px solid ${colors.border}`, marginBottom: '16px',
  },
  grid2: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' },
  grid3: { display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '16px' },
  grid4: { display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '16px' },
};

export const btn = {
  primary: {
    background: colors.primary, color: '#fff', border: 'none', padding: '8px 20px',
    borderRadius: '6px', cursor: 'pointer', fontWeight: 600, fontSize: '14px',
  },
  secondary: {
    background: 'transparent', color: colors.textMuted, border: `1px solid ${colors.border}`,
    padding: '6px 14px', borderRadius: '6px', cursor: 'pointer', fontSize: '13px',
  },
};
