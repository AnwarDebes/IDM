import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { StreamProvider } from './StreamContext';
import { AppProvider } from './AppContext';
import Layout from './components/Layout';
import ToastContainer from './components/Toast';
import Home from './pages/Home';
import Explorer from './pages/Explorer';
import Compare from './pages/Compare';
import Stream from './pages/Stream';

function App() {
  return (
    <BrowserRouter>
      <AppProvider>
        <StreamProvider>
          <Routes>
            <Route path="/" element={<Layout />}>
              <Route index element={<Home />} />
              <Route path="explorer" element={<Explorer />} />
              <Route path="compare" element={<Compare />} />
              <Route path="stream" element={<Stream />} />
            </Route>
          </Routes>
          <ToastContainer />
        </StreamProvider>
      </AppProvider>
    </BrowserRouter>
  );
}

export default App;
