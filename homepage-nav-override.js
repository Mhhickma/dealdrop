/* Homepage-only Categories mega menu.
   Keeps the original homepage header and deal-loading scripts intact. */
(function () {
  const html = `
    <div class="homepage-mega-header">
      <div>
        <div class="homepage-mega-title">Shop by Category</div>
        <div class="homepage-mega-subtitle">Find the Amazon deal page that matches what you want.</div>
      </div>
      <div class="homepage-mega-pill">Updated Daily</div>
    </div>
    <div class="homepage-mega-grid">
      <div class="homepage-mega-column">
        <h3>Featured</h3>
        <a class="homepage-mega-link" href="/top-100-amazon-deals-today/"><span class="homepage-mega-icon">🔥</span>Top 100 Deals</a>
        <a class="homepage-mega-link" href="#" data-scroll-target="deals"><span class="homepage-mega-icon">✨</span>New Deals Today</a>
        <a class="homepage-mega-link" href="/best-amazon-deals-under-50/"><span class="homepage-mega-icon">💵</span>Deals Under $50</a>
        <a class="homepage-mega-link" href="#" data-scroll-target="hot"><span class="homepage-mega-icon">⚡</span>Hot Deals</a>
      </div>
      <div class="homepage-mega-column">
        <h3>Popular Categories</h3>
        <a class="homepage-mega-link" href="/best-amazon-tool-deals/"><span class="homepage-mega-icon">🛠️</span>Tool Deals</a>
        <a class="homepage-mega-link" href="/best-amazon-home-kitchen-deals/"><span class="homepage-mega-icon">🏠</span>Home &amp; Kitchen</a>
        <a class="homepage-mega-link" href="/best-amazon-electronics-deals/"><span class="homepage-mega-icon">💻</span>Electronics</a>
        <a class="homepage-mega-link" href="/best-amazon-automotive-deals/"><span class="homepage-mega-icon">🚗</span>Automotive</a>
        <a class="homepage-mega-link" href="/best-amazon-patio-lawn-garden-deals/"><span class="homepage-mega-icon">🌿</span>Patio &amp; Garden</a>
      </div>
      <div class="homepage-mega-column">
        <h3>More</h3>
        <a class="homepage-mega-link" href="/best-amazon-sports-outdoors-deals/"><span class="homepage-mega-icon">🏈</span>Sports &amp; Outdoors</a>
        <a class="homepage-mega-link" href="/best-amazon-pet-supplies-deals/"><span class="homepage-mega-icon">🐾</span>Pet Supplies</a>
        <a class="homepage-mega-link" href="/best-amazon-toys-games-deals/"><span class="homepage-mega-icon">🧸</span>Toys &amp; Games</a>
        <a class="homepage-mega-link" href="/best-amazon-office-products-deals/"><span class="homepage-mega-icon">🏢</span>Office Products</a>
        <a class="homepage-mega-link" href="/best-amazon-health-household-deals/"><span class="homepage-mega-icon">🧼</span>Health &amp; Household</a>
      </div>
    </div>
    <a class="homepage-mega-footer" href="/categories/"><span>View All Categories</span><span>→</span></a>
  `;

  function updateMenu() {
    const menu = document.querySelector('#categoryMenu, #menu-categories, .nav-dropdown .dropdown-menu:not(.hot-dropdown)');
    if (!menu) return;
    menu.classList.add('homepage-mega-menu');
    menu.innerHTML = html;
    menu.querySelectorAll('[data-scroll-target]').forEach(function (link) {
      link.addEventListener('click', function (event) {
        event.preventDefault();
        const target = link.getAttribute('data-scroll-target');
        if (target === 'hot' && typeof window.scrollToHotDeals === 'function') window.scrollToHotDeals();
        if (target === 'deals' && typeof window.scrollToAllDeals === 'function') window.scrollToAllDeals();
        menu.classList.remove('show');
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      updateMenu();
      setTimeout(updateMenu, 500);
      setTimeout(updateMenu, 1500);
    });
  } else {
    updateMenu();
    setTimeout(updateMenu, 500);
    setTimeout(updateMenu, 1500);
  }
})();
