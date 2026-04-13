import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({ baseURL: API_URL });

export const getHealth = () => api.get('/api/v1/health').then(r => r.data);
export const getQueries = () => api.get('/api/v1/queries').then(r => r.data);
export const executeQuery = (query_id, backend, params = {}) =>
  api.post('/api/v1/query', { query_id, backend, params }).then(r => r.data);
export const compareQuery = (query_id, params = {}) =>
  api.post('/api/v1/compare', { query_id, params }).then(r => r.data);
export const olapSlice = (dimension, value) =>
  api.post('/api/v1/olap/slice', { backend: 'postgres', dimension, value }).then(r => r.data);
export const olapDice = (values) =>
  api.post('/api/v1/olap/dice', { backend: 'postgres', values }).then(r => r.data);
export const olapDrilldown = (granularity, value) =>
  api.post('/api/v1/olap/drilldown', { backend: 'postgres', dimension: 'time', granularity, value }).then(r => r.data);
export const olapRollup = (granularity) =>
  api.post('/api/v1/olap/rollup', { backend: 'postgres', dimension: 'time', granularity }).then(r => r.data);
export const olapPivot = () =>
  api.post('/api/v1/olap/pivot', { backend: 'postgres', dimension: 'time' }).then(r => r.data);
export const getMetadata = (backend) =>
  api.get(`/api/v1/metadata/${backend}`).then(r => r.data);
export const refreshSummaries = () =>
  api.post('/api/v1/batch/refresh-summaries').then(r => r.data);
export const getBatchStatus = () =>
  api.get('/api/v1/batch/status').then(r => r.data);

export const startDemo = (limit = 500) =>
  api.post(`/api/v1/stream/demo?limit=${limit}`).then(r => r.data);
export const getDemoStatus = () =>
  api.get('/api/v1/stream/demo/status').then(r => r.data);

export const SSE_URL = `${API_URL}/api/v1/stream/events`;
