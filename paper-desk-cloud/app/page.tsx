import Link from 'next/link';
import Script from 'next/script';

export const dynamic = 'force-dynamic';

export default function PaperDesk() {
  return (
    <>
      <a className="skip-link" href="#page-content">跳到主要内容</a>
      <div className="app-shell">
        <aside className="sidebar" aria-label="Paper Desk navigation">
          <Link className="brand" href="/" data-route aria-label="Paper Desk home">
            <span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>
            <span><strong>Paper</strong><small>Desk</small></span>
          </Link>
          <nav className="primary-nav" id="primary-nav" />
          <div className="market-spine" aria-label="Runtime market spine">
            <span className="spine-label">MARKET SPINE</span>
            <div className="spine-track" aria-hidden="true"><i className="spine-node node-feed" /><i className="spine-node node-bar" /><i className="spine-node node-decision" /></div>
            <div className="spine-status"><span id="spine-state">IDLE</span><small id="spine-detail">IEX · PAPER</small></div>
          </div>
          <div className="sidebar-foot"><span className="safety-dot" aria-hidden="true" /><span>PAPER TRADING ONLY</span><small>IEX · PRIVATE CLOUD · NO LIVE ORDERS</small></div>
        </aside>
        <div className="main-column">
          <header className="topbar">
            <div className="page-heading"><span className="page-kicker" id="page-kicker">WORKSPACE</span><h1 id="page-title">Paper Desk</h1></div>
            <div className="topbar-actions">
              <div className="runtime-pill" id="runtime-pill" role="status" aria-live="polite"><span className="runtime-indicator" aria-hidden="true" /><span id="runtime-label">空闲</span></div>
              <button className="icon-button language-toggle" id="language-toggle" type="button" aria-label="Switch language">EN</button>
              <Link className="icon-button settings-link" href="/settings" data-route aria-label="Settings"><span aria-hidden="true">⚙</span></Link>
            </div>
          </header>
          <main id="page-content" tabIndex={-1} />
          <footer className="app-footer"><span>Paper Desk · private cloud workspace</span><span>Alpaca Basic IEX · single exchange · no live orders</span></footer>
        </div>
      </div>
      <nav className="mobile-nav" id="mobile-nav" aria-label="Mobile navigation" />
      <div className="toast-region" id="toast-region" aria-live="polite" aria-atomic="true" />
      <div className="drawer-backdrop" id="drawer-backdrop" hidden />
      <aside className="detail-drawer" id="detail-drawer" aria-labelledby="drawer-title" aria-hidden="true"><button className="drawer-close" id="drawer-close" type="button" aria-label="Close">×</button><div id="drawer-content" /></aside>
      <Script type="module" src="/app.js" strategy="afterInteractive" />
    </>
  );
}
