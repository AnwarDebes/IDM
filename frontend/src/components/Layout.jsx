import React, { useState, useEffect } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { getHealth } from '../api';
import { useStream } from '../StreamContext';
import { colors, layout } from '../styles';

const navItems = [
  { path: '/', label: 'Overview', icon: '\u25A3' },
  { path: '/explorer', label: 'OLAP Explorer', icon: '\u25B7' },
  { path: '/compare', label: 'Compare', icon: '\u2261' },
  { path: '/stream', label: 'Live Stream', icon: 'stream' },
];

function HealthDot({ status }) {
  const c = status === 'healthy' ? colors.success : colors.error;
  return <span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: '50%', background: c, marginRight: 6 }} />;
}

export default function Layout() {
  const [health, setHealth] = useState(null);
  const { connected } = useStream();

  useEffect(() => {
    const fetch = () => getHealth().then(setHealth).catch(() => {});
    fetch();
    const id = setInterval(fetch, 15000);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <nav style={layout.sidebar}>
        <div style={{ padding: '0 20px', marginBottom: '32px' }}>
          <h1 style={{ fontSize: '18px', fontWeight: 700, color: colors.text, margin: 0 }}>
            EpiDW
          </h1>
          <p style={{ fontSize: '11px', color: colors.textMuted, margin: '4px 0 0' }}>
            Multi-Backend Analytics
          </p>
        </div>

        <div style={{ flex: 1 }}>
          {navItems.map(({ path, label, icon }) => (
            <NavLink
              key={path}
              to={path}
              end={path === '/'}
              style={({ isActive }) => ({
                display: 'flex', alignItems: 'center', gap: '10px',
                padding: '10px 20px', textDecoration: 'none',
                color: isActive ? colors.primary : colors.textMuted,
                background: isActive ? 'rgba(59,130,246,0.1)' : 'transparent',
                borderRight: isActive ? `3px solid ${colors.primary}` : '3px solid transparent',
                fontSize: '14px', fontWeight: isActive ? 600 : 400,
                transition: 'all 0.15s',
              })}
            >
              {icon === 'stream' ? (
                <span style={{
                  display: 'inline-block', width: 10, height: 10, borderRadius: '50%',
                  background: connected ? colors.success : colors.error,
                  flexShrink: 0,
                }} />
              ) : (
                <span style={{ fontSize: '16px', width: '20px', textAlign: 'center' }}>{icon}</span>
              )}
              {label}
            </NavLink>
          ))}
        </div>

        {health && (
          <div style={{ padding: '16px 20px', borderTop: `1px solid ${colors.border}` }}>
            <p style={{ fontSize: '11px', color: colors.textMuted, margin: '0 0 8px', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
              Services
            </p>
            {Object.entries(health.services).map(([name, svc]) => (
              <div key={name} style={{ display: 'flex', alignItems: 'center', fontSize: '12px', color: colors.textMuted, marginBottom: '4px' }}>
                <HealthDot status={svc.status} />
                <span style={{ textTransform: 'capitalize' }}>{name}</span>
              </div>
            ))}
          </div>
        )}
      </nav>

      <main style={layout.main}>
        <Outlet />
      </main>
    </div>
  );
}
