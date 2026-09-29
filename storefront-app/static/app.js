const POLL_INTERVAL_MS = 2000;
const FAST_POLL_MS = 300;
const FAST_POLL_ATTEMPTS = 10;

const cards = Array.from(document.querySelectorAll(".card"));

function renderQuantity(card, quantity) {
  const quantityEl = card.querySelector(".quantity");
  const buyBtn = card.querySelector(".buy-btn");

  if (quantity === null || quantity === undefined) {
    quantityEl.textContent = "--";
    return;
  }

  quantityEl.textContent = quantity;
  const outOfStock = quantity <= 0;
  card.classList.toggle("out-of-stock", outOfStock);
  buyBtn.disabled = outOfStock;
}

async function refreshQuantity(card) {
  const sku = card.dataset.sku;
  try {
    const res = await fetch(`/api/inventory/${sku}`);
    if (!res.ok) return;
    const data = await res.json();
    renderQuantity(card, data.quantity);
  } catch (err) {
    console.error("inventory read failed", sku, err);
  }
}

function selectSize(card, btn) {
  card.querySelectorAll(".size-btn").forEach((b) => b.classList.toggle("active", b === btn));
  card.dataset.sku = btn.dataset.sku;
  const skuEl = card.querySelector(".sku");
  if (skuEl) skuEl.textContent = btn.dataset.sku;
  renderQuantity(card, null);
  refreshQuantity(card);
}

async function buy(card) {
  const sku = card.dataset.sku;
  const buyBtn = card.querySelector(".buy-btn");
  card.classList.add("pending");
  buyBtn.disabled = true;

  try {
    await fetch(`/api/buy/${sku}`, { method: "POST" });
  } catch (err) {
    console.error("purchase failed", sku, err);
  }

  // Flink's materialization lands a moment after the Kafka write, so poll fast for
  // a few seconds to show the drop as soon as it's visible, then fall back to the
  // normal slow poll.
  for (let i = 0; i < FAST_POLL_ATTEMPTS; i++) {
    await new Promise((r) => setTimeout(r, FAST_POLL_MS));
    await refreshQuantity(card);
  }
  card.classList.remove("pending");
}

cards.forEach((card) => {
  refreshQuantity(card);
  card.querySelector(".buy-btn").addEventListener("click", () => buy(card));
  card.querySelectorAll(".size-btn").forEach((btn) => {
    btn.addEventListener("click", () => selectSize(card, btn));
  });
  setInterval(() => refreshQuantity(card), POLL_INTERVAL_MS);
});
