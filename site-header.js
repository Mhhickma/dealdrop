/* Black Lab Deals uniform header navigation.
   Step 2: shared header file only. This file does nothing unless a page loads it. */

(function () {
  const MEGA_MENU_HTML = `
    <div class="bld-mega-header">
      <div>
        <div class="bld-mega-title">Shop by Category</div>
        <div class="bld-mega-subtitle">Find the Amazon deal page that matches what you want.</div>
      </div>
      <div class="bld-mega-pill">Updated Daily</div>
    </div>

    <div class="bld-mega-grid">
      <div class="bld-mega-column">
        <h3>Featured</h3>
        <a class="bld-mega-link" href="/top-100-amazon-deals-today/"><span class="bld-mega-icon">🔥</span>Top 100 Deals</a>
        <a class="bld-mega-link" href="/#deals-section"><span class="bld-mega-icon">✨</span>New Deals Today</a>
        <a class="bld-mega-link" href="/best-amazon-deals-under-50/"><span class="bld-mega-icon">💵</span>Deals Under $50</a>
        <a class="bld-mega-link" href="/#hot-deals"><span class="bld-mega-icon">⚡</span>Hot Deals</a>
      </div>

      <div class="bld-mega-column">
        <h3>Popular Categories</h3>
        <a class="bld-mega-link" href="/best-amazon-tool-deals/"><span class="bld-mega-icon">🛠️</span>Tool Deals</a>
        <a class="bld-mega-link" href="/best-amazon-home-kitchen-deals/"><span class="bld-mega-icon">🏠</span>Home &amp; Kitchen</a>
        <a class="bld-mega-link" href="/#deals-section" data-category="electronics"><span class="bld-mega-icon">💻</span>Electronics</a>
        <a class="bld-mega-link" href="/#deals-section" data-category="automotive"><span class="bld-mega-icon">🚗</span>Automotive</a>
        <a class="bld-mega-link" href="/#deals-section" data-category="patio"><span class="bld-mega-icon">🌿</span>Patio &amp; Garden</a>
      </div>

      <div class="bld-mega-column">
        <h3>More</h3>
        <a class="bld-mega-link" href="/#deals-section" data-category="sports"><span class="bld-mega-icon">🏈</span>Sports &amp; Outdoors</a>
        <a class="bld-mega-link" href="/#deals-section" data-category="pet"><span class="bld-mega-icon">🐾</span>Pet Supplies</a>
        <a class="bld-mega-link" href="/#deals-section" data-category="toys"><span class="bld-mega-icon">🧸</span>Toys &amp; Games</a>
        <a class="bld-mega-link" href="/#deals-section" data-category="office"><span class="bld-mega-icon">🏢</span>Office Products</a>
        <a class="bld-mega-link" href="/#deals-section" data-category="health"><span class="bld-mega-icon">🧼</span>Health &amp; Household</a>
      </div>
    </div>

    <a class="bld-mega-footer" href="/categories/">
      <span>View All Categories</span>
      <span>→</span>
    </a>
  `;

  function getHomeAwareHref(anchor) {
    return window.location.pathname === '/' || window.location.pathname === '/index.html' ? anchor : '/' + anchor;
  }

  function buildHeader() {
    return `
      <header class="bld-header-shell">
        <div class="bld-header-main">
          <a href="/" class="bld-brand" aria-label="Black Lab Deals home">
            <img class="bld-brand-logo" src="/logo.png" alt="Black Lab Deals logo">
            <div>
              <div class="bld-brand-title">Black Lab <span>Deals</span></div>
              <div class="bld-brand-rule"></div>
              <div class="bld-brand-tagline">Fresh Amazon deals updated daily</div>
            </div>
          </a>

          <div class="bld-header-actions">
            <nav class="bld-desktop-nav" aria-label="Main navigation">
              <a class="bld-hot-link" href="${getHomeAwareHref('#hot-deals')}">Hot Deals</a>
              <span class="bld-nav-divider" aria-hidden="true"></span>
              <div class="bld-category-wrap">
                <button class="bld-category-trigger" type="button" aria-expanded="false">Categories <span aria-hidden="true">▾</span></button>
                <div class="dropdown-menu bld-mega-menu" id="menu-categories">
                  ${MEGA_MENU_HTML}
                </div>
              </div>
              <span class="bld-nav-divider" aria-hidden="true"></span>
              <a class="bld-all-link" href="${getHomeAwareHref('#deals-section')}">All Deals</a>
            </nav>
            <a class="bld-alert-btn" href="${getHomeAwareHref('#alerts-box')}"><span class="bld-alert-icon">●</span> Get Alerts</a>
          </div>

          <div class="bld-mobile-actions">
            <a class="bld-alert-btn" href="${getHomeAwareHref('#alerts-box')}"><span class="bld-alert-icon">●</span> Get Alerts</a>
          </div>
        </div>
      </header>
    `;
  }

  function wireHomepageScrollLinks(root) {
    const isHome = window.location.pathname === '/' || window.location.pathname === '/index.html';
    if (!isHome) return;

    const hotLink = root.querySelector('.bld-hot-link');
    const allLink = root.querySelector('.bld-all-link');

    if (hotLink && typeof window.scrollToHotDeals === 'function') {
      hotLink.addEventListener('click', function (event) {
        event.preventDefault();
        window.scrollToHotDeals();
      });
    }

    if (allLink && typeof window.scrollToAllDeals === 'function') {
      allLink.addEventListener('click', function (event) {
        event.preventDefault();
        window.scrollToAllDeals();
      });
    }
  }

  function initMegaMenu(root) {
    const trigger = root.querySelector('.bld-category-trigger');
    const menu = root.querySelector('.bld-mega-menu');
    if (!trigger || !menu) return;

    trigger.addEventListener('click', function (event) {
      event.preventDefault();
      const isOpen = menu.classList.toggle('show');
      trigger.setAttribute('aria-expanded', String(isOpen));
    });

    document.addEventListener('click', function (event) {
      if (!root.contains(event.target)) {
        menu.classList.remove('show');
        trigger.setAttribute('aria-expanded', 'false');
      }
    });

    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') {
        menu.classList.remove('show');
        trigger.setAttribute('aria-expanded', 'false');
      }
    });
  }

  function mountHeader() {
    const mount = document.getElementById('site-header');
    if (!mount) return;

    mount.innerHTML = buildHeader();
    wireHomepageScrollLinks(mount);
    initMegaMenu(mount);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mountHeader);
  } else {
    mountHeader();
  }
})();
