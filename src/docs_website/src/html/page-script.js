window.urlPrefix = "$url_prefix";

// Enable search if JavaScript is enabled.
document.querySelector(".search-box").style.removeProperty("display");

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register(urlPrefix + "/service-worker.js");
}

const leftPane = document.querySelector(".left-pane");
const resizer = document.querySelector(".resizer");
resizer.addEventListener("mousedown", () => {
  resizer.classList.add("resizing");
  document.body.style.cursor = "col-resize";
  document.body.style.userSelect = document.body.style.webkitUserSelect = "none";
  document.addEventListener("mousemove", onMouseMove);
  document.addEventListener("mouseup", onMouseUp);
  function onMouseMove(e) {
    leftPane.style.width = Math.max(176, e.clientX) + "px";
    if (e.clientX < 130) {
      document.body.classList.add("sidenav-collapsed");
    } else if (e.clientX > 140) {
      document.body.classList.remove("sidenav-collapsed");
    }
  }
  function onMouseUp() {
    resizer.classList.remove("resizing");
    document.body.style.cursor = "";
    document.body.style.userSelect = document.body.style.webkitUserSelect = "";
    document.removeEventListener("mousemove", onMouseMove);
  }
});
resizer.addEventListener("dblclick", () => leftPane.style.removeProperty("width"));

const collapseButton = document.querySelector(".collapse-button");
collapseButton.addEventListener("click", () => document.body.classList.add("sidenav-collapsed"));
const expandButton = document.querySelector(".expand-button");
expandButton.addEventListener("click", () => document.body.classList.remove("sidenav-collapsed"));

const navSide = document.querySelector("nav.side");
const details = [...navSide.querySelectorAll("details")];

syncSideNavWithLocation();

const menuButton = document.querySelector(".menu-button");
menuButton.addEventListener("click", () => {
  document.body.classList.toggle("mobile-expanded");
  if (leftPane.classList.contains("search-active")) closeSearch();
});

// Restore and save the state of the side navigation
const navSideState = JSON.parse(localStorage.getItem("navSideState"));
if (navSideState) {
  leftPane.style.width = navSideState.width;
  if (navSideState.collapsed) document.body.classList.add("sidenav-collapsed");
  navSideState.expanded.forEach((expanded, i) => {
    if (i < details.length) details[i].open = expanded
  });
  leftPane.scrollTop = navSideState.scrollTop;
}
window.addEventListener("beforeunload", () => {
  const navSideState = {
    width: leftPane.style.width,
    collapsed: document.body.classList.contains("sidenav-collapsed"),
    expanded: details.map(detail => detail.open),
    scrollTop: leftPane.scrollTop,
  };
  localStorage.setItem("navSideState", JSON.stringify(navSideState));
});

function syncSideNavWithLocation() {
  const target = document.querySelector("nav.side .target");
  if (target) target.classList.remove("target");

  const path = location.pathname;
  if (path.length > 1) {
    document.querySelectorAll("nav.side a").forEach(a => {
      if (a.href.endsWith(path)) {
        a.classList.add("target");
        for (let parent = a.parentElement; parent; parent = parent.parentElement) {
          if (parent.matches(".menu")) {
            const head = parent.previousSibling;
            if (head?.matches?.(".menu-head")) head.classList.add("expanded");
          } else if (parent.matches(".menu-head")) {
            parent.classList.add("expanded"); // expand for discoverability
          }
        }
      }
    });
  }
}