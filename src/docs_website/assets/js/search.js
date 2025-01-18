let pages = [];
let sections = [];

const searchInput = document.querySelector("input[type=search]");
const searchResults = document.querySelector(".search-results");
const searchNotFound = document.querySelector(".search-notfound");
const searchStats = document.querySelector(".search-stats");
const searchHotkey = document.querySelector(".search-box>.hotkey");
const searchClearButton = document.querySelector(".search-box>.clear-button");
const content = document.querySelector("article>.content");

let sidenavWasCollapsed = false;
document.addEventListener("keydown", event => {
  if (event.ctrlKey || event.altKey || event.metaKey) return;
  if (event.key === "/" && searchInput !== document.activeElement) {
    sidenavWasCollapsed = document.body.classList.contains("sidenav-collapsed");
    document.body.classList.remove("sidenav-collapsed");
    searchInput.focus();
    event.preventDefault();
  } else if (event.key === "Escape") {
    if (searchInput === document.activeElement || searchInput.value !== "") {
      closeSearch();
      event.preventDefault();
    }
  } else if (searchInput.value !== "") {
    if (event.key === "ArrowDown") {
      selectNextResult();
      event.preventDefault();
    } else if (event.key === "ArrowUp") {
      selectPreviousResult();
      event.preventDefault();
    } else if (event.key === "Enter") {
      const selected = searchResults.querySelector(".selected");
      if (!selected) selectNextResult();
      closeSearch();
      event.preventDefault();
    }
  }
})

searchInput.addEventListener("focus", () => {
  searchHotkey.style.display = "none";
});
searchInput.addEventListener("blur", () => {
  if (searchInput.value === "") searchHotkey.style.display = "block";
});
searchInput.addEventListener("input", onSearchInput);
searchClearButton.addEventListener("click", () => {
  searchInput.value = "";
  onSearchInput();
  if (searchInput !== document.activeElement) searchHotkey.style.display = "block";
  removeTextHighlight(content);
});

initSearch();

async function initSearch() {
  const response = await fetch(urlPrefix + "/search-index.json");
  pages = await response.json();
  const parser = new DOMParser();
  pages.forEach((page, pageIndex) => {
    const doc = parser.parseFromString(page.html, "text/html");
    const body = doc.querySelector("body");

    let currentSection;
    for (const child of body.children) {
      const anchor = child.querySelector(".anchor");
      if (anchor) {
        const title = anchor.innerText.replace(/\n/g, " ");
        if (!page.title) page.title = title;
        currentSection = {
          pageIndex,
          title,
          path: page.path,
          hash: anchor.hash,
          text: title,
        };
        sections.push(currentSection);
      } else if (currentSection) {
        currentSection.text += " " + child.innerText.replace(/\n/g, " ");
      }
    }
  });

  if (searchInput.value) onSearchInput(); // Repeat search once the index is fetched.
}

function onSearchInput() {
  const results = search(searchInput.value);
  const groups = [];
  let currentGroup;
  for (const result of results) {
    if (!currentGroup || currentGroup.pageIndex !== result.section.pageIndex) {
      currentGroup = { pageIndex: result.section.pageIndex, results: [] };
      groups.push(currentGroup);
    }
    currentGroup.results.push(result);
  }
  let menus = [];
  for (const group of groups) {
    const details = document.createElement("details");
    menus.push(details);
    details.open = true;
    const summary = document.createElement("summary");
    details.appendChild(summary);
    const p = document.createElement("p");
    summary.appendChild(p);
    p.innerText = pages[group.pageIndex].title;
    const menu = document.createElement("ol");
    details.appendChild(menu);
    for (const result of group.results) {
      const li = document.createElement("li");
      menu.appendChild(li);
      li.className = "item";
      const a = document.createElement("a");
      li.appendChild(a);
      a.addEventListener("click", (e) => {
        e.preventDefault();
        selectResult(a);
      });
      a.href = urlPrefix + "/" + result.section.path + "/" + result.section.hash;
      a.pageIndex = result.section.pageIndex;
      const h3 = document.createElement("h3");
      a.appendChild(h3);
      h3.innerText = result.section.title;
      const p = document.createElement("p");
      a.appendChild(p);
      p.innerText = result.context;
    }
  }
  searchResults.replaceChildren(...menus);

  const resultText = results.length === 1 ? "result" : "results";
  const pageText = groups.length === 1 ? "page" : "pages";
  searchStats.innerText = `${results.length} ${resultText} on ${groups.length} ${pageText}`

  highlightText(searchInput.value, searchResults);

  const searchActive = searchInput.value !== "";
  if (searchActive) {
    leftPane.classList.add("search-active");
  } else {
    leftPane.classList.remove("search-active");
  }
  searchNotFound.style.display = searchActive && results.length === 0 ? "flex" : "none";
}

function search(term) {
  if (term.length === 0) return [];
  const escapedTerm = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(escapedTerm, 'gi');
  let hits = [];
  for (const section of sections) {
    const match = regex.exec(section.text);
    if (match) {
      const firstIndex = match.index;
      const count = section.text.match(regex).length;
      hits.push({ firstIndex, count, section });
    }
  }
  hits.forEach(hit => hit.context = makeContext(hit.section.text, hit.firstIndex, term.length));
  return hits;
}

function makeContext(text, i, length) {
  const windowSizeLeft = 15;
  const windowSizeRight = 200;
  const highlight = text.slice(i, i + length);

  let contextLeft = "";
  let i0 = Math.max(0, i - windowSizeLeft);
  if (i0 > 0) contextLeft = "...";
  while (i0 > 0 && text[i0] !== ' ') i0--;
  contextLeft = contextLeft + text.slice(i0, i).trimLeft();

  let contextRight = "";
  let i1 = Math.min(text.length, i + length + windowSizeRight);
  if (i1 < text.length) contextRight = "...";
  while (i1 < text.length && text[i1] !== ' ') i1++;
  contextRight = text.slice(i + length, i1).trimRight() + contextRight;

  return contextLeft + highlight + contextRight;
}

function selectResult(node) {
  if (isMobileView()) {
    location.href = node.href;
    return;
  }
  searchResults.querySelectorAll(".selected").forEach(r => r.classList.remove("selected"));
  node.classList.add("selected");
  scrollIntoViewIfNeeded(node, searchResults.parentNode);
  // Preview result
  const page = pages[node.pageIndex];
  content.innerHTML = page.html;
  const state = { pageIndex: node.pageIndex };
  if (history.state) {
    history.replaceState(state, page.title, node.href);
  } else {
    history.pushState(state, page.title, node.href);
  }
  statePathname = location.pathname;
  if (page.title === "TigerBeetle Docs") {
    document.title = page.title;
  } else {
    document.title = "TigerBeetle Docs | " + page.title;
  }
  handleAnchor();
  highlightText(searchInput.value, content);
  markActiveHighlight(content);
}

function markActiveHighlight(container) {
  let element = container.firstElementChild;
  if (location.hash) {
    element = document.getElementById(location.hash.slice(1));
  }
  for (; element; element = element.nextElementSibling) {
    const highlight = element.querySelector(".highlight");
    if (highlight) {
      highlight.classList.add("active");
      scrollIntoViewIfNeeded(highlight, container.parentNode);
      return;
    }
  }
}

let statePathname = location.pathname;
window.addEventListener("popstate", (e) => {
  if (e.state) {
    const page = pages[e.state.pageIndex];
    content.innerHTML = page.html;
    syncSideNavWithLocation();
  } else {
    if (location.pathname != statePathname) {
      location.reload();
    }
  }
  statePathname = location.pathname;
  handleAnchor();
});

function handleAnchor() {
  document.querySelectorAll(".target>.anchor").forEach(e => e.parentNode?.classList.remove("target"));
  if (location.hash) {
    const anchor = document.getElementById(location.hash.slice(1));
    if (anchor) {
      anchor.classList.add("target");
      scrollIntoViewIfNeeded(anchor, content.parentNode);
    }
  }
}
window.addEventListener("load", () => {
  handleAnchor();
});

function selectNextResult() {
  const nodes = [...searchResults.querySelectorAll("details[open] a")];
  if (nodes.length === 0) return;
  const selected = searchResults.querySelector(".selected");
  const i = selected ? nodes.indexOf(selected) + 1 : 0;
  selectResult(nodes[i % nodes.length]);
}

function selectPreviousResult() {
  const nodes = [...searchResults.querySelectorAll("details[open] a")];
  if (nodes.length === 0) return;
  const selected = searchResults.querySelector(".selected");
  const i = selected ? nodes.indexOf(selected) - 1 : -1;
  selectResult(nodes[(i + nodes.length) % nodes.length]);
}

function scrollIntoViewIfNeeded(element, parent) {
  const elementRect = element.getBoundingClientRect();
  const parentRect = parent.getBoundingClientRect();

  const isOutOfView = (
    elementRect.top < parentRect.top ||
    elementRect.left < parentRect.left ||
    elementRect.bottom > parentRect.bottom ||
    elementRect.right > parentRect.right
  );

  if (isOutOfView) {
    element.scrollIntoView({ block: 'center', inline: 'center' });
  }
}

function closeSearch() {
  searchInput.blur();
  searchHotkey.style.display = "block";
  searchInput.value = "";
  onSearchInput();
  removeTextHighlight(content);
  syncSideNavWithLocation();
  if (sidenavWasCollapsed) document.body.classList.add("sidenav-collapsed");
}

function highlightText(term, container) {
  const escapedTerm = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`(${escapedTerm})`, 'gi');
  let replacements = [];
  const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT, null);
  let node;
  while (node = walker.nextNode()) {
    const parent = node.parentNode;
    if (parent && node.nodeValue.match(regex)) {
      const fragment = document.createDocumentFragment();
      let lastIndex = 0;
      node.nodeValue.replace(regex, (match, p1, index) => {
        if (index > lastIndex) {
          fragment.appendChild(document.createTextNode(node.nodeValue.slice(lastIndex, index)));
        }
        const span = document.createElement("span");
        span.className = "highlight";
        span.textContent = match;
        fragment.appendChild(span);
        lastIndex = index + match.length;
      });
      if (lastIndex < node.nodeValue.length) {
        fragment.appendChild(document.createTextNode(node.nodeValue.slice(lastIndex)));
      }
      replacements.push({parent, fragment, node});
    }
  }
  replacements.forEach(r => r.parent.replaceChild(r.fragment, r.node));
}

function removeTextHighlight(container) {
  container.querySelectorAll(".highlight").forEach(h => h.classList.remove("highlight"));
}

function escapeHtml(unsafe) {
  return unsafe.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#039;');
}

function isMobileView() {
  return window.innerWidth < 810;
}