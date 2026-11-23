// The timeline's pages (decision 040): app colours on every page, and on the
// Day view the lanes, the scrubber and the frame shown. Data from the API is
// only ever set as text (textContent) or as a validated colour; nothing is
// parsed as markup. No storage: nothing is kept outside the page and the
// browser's HTTP cache, which logging out clears.

import {
  LatestOnly,
  MEDIA_NO_THUMBNAILS,
  MEDIA_THUMBNAILS,
  appRuns,
  applyStep,
  clock,
  frameAt,
  frameUrl,
  imageFor,
  keyStep,
  mediaUrl,
  safeColour,
  textOn,
  timeForX,
  xForTime,
} from "./timeline-core.js";

const REST_MS = 250; // after this long without moving, the scrubber rests
const NEAR_SECONDS = 15 * 60;
const VISIT_PAGES = 4; // at most 2,000 visits a day are drawn

// Through the CSSOM, which the policy allows (it refuses style attributes).
function paint(element, value) {
  const colour = safeColour(value);
  element.style.backgroundColor = colour;
  element.style.color = textOn(colour);
}

function chip(app) {
  const span = document.createElement("span");
  span.className = "app-chip";
  span.textContent = app ? app.name : "No app";
  if (app) paint(span, app.colour);
  return span;
}

async function getJson(url, signal) {
  const response = await fetch(url, { signal, credentials: "same-origin" });
  if (response.status === 401) throw new Error("You are logged out. Reload to log in.");
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `The server answered ${response.status}.`);
  return body;
}

class DayView {
  constructor(root) {
    this.root = root;
    this.day = root.dataset.day;
    this.timeZone = root.dataset.timezone;
    this.labels = {};
    for (const code of [0, 1, 2, 3]) this.labels[code] = root.getAttribute(`data-label-${code}`);
    this.status = document.getElementById("timeline-status");
    this.scrubber = document.getElementById("scrubber");
    this.image = document.getElementById("frame-image");
    this.tile = document.getElementById("frame-tile");
    this.time = document.getElementById("frame-time");
    this.app = document.getElementById("frame-app");
    this.state = document.getElementById("frame-state");
    this.text = document.getElementById("frame-text");
    this.original = document.getElementById("frame-original");
    this.screenLane = document.getElementById("lane-screen");
    this.browserLane = document.getElementById("lane-browser");
    this.near = document.getElementById("visits-near");
    this.current = -1;
    this.visits = [];
    this.restTimer = 0;
    this.preloaded = new Set();
    // At most ten small images a second while dragging.
    this.small = new LatestOnly((i, done) => this.#load(i, 360, done), { spacing: 100 });
    this.large = new LatestOnly((i, done) => this.#load(i, 1280, done), { cancelStale: true });
    this.detail = new LatestOnly((i, done) => this.#detail(i, done), { cancelStale: true });
  }

  async start() {
    this.say("Loading the day…");
    try {
      const index = await getJson(`/api/timeline/screen/${this.day}`);
      this.index = index;
      this.times = index.frames.t;
      this.apps = index.apps;
      this.length = (Date.parse(index.end) - Date.parse(index.start)) / 1000;
    } catch (error) {
      this.say(error.message);
      return;
    }
    this.#legend();
    this.#wire();
    this.#drawLanes();
    new ResizeObserver(() => this.#drawLanes()).observe(this.screenLane.parentElement);
    if (this.times.length === 0) {
      this.say("No screen frames on this day.");
      this.#showNothing();
    } else {
      const wanted = this.root.dataset.frame;
      const found = wanted ? this.index.frames.key.indexOf(wanted) : -1;
      this.scrubber.max = String(Math.ceil(this.length));
      this.scrubber.disabled = false;
      this.say(this.index.truncated ? "Only the first frames of this day are shown." : "");
      this.show(found >= 0 ? found : 0, true);
    }
    this.#loadVisits();
  }

  say(message) {
    this.status.textContent = message;
  }

  // ---- what is shown --------------------------------------------------------

  show(index, resting) {
    if (index < 0) return;
    const moved = index !== this.current;
    this.current = index;
    const t = this.times[index];
    const appIndex = this.index.frames.app[index];
    const app = appIndex === null ? null : this.apps[appIndex];
    const at = clock(this.index.start, t, this.timeZone);
    this.scrubber.value = String(t);
    this.scrubber.setAttribute("aria-valuetext", `${at}, ${app ? app.name : "no app"}`);
    this.time.textContent = at;
    this.app.replaceChildren(chip(app));
    this.#cursor(t);
    const code = this.index.frames.media[index];
    if (imageFor(code, resting) === null) {
      this.small.want(null);
      this.large.want(null);
      this.#tile(this.labels[code] || "", code);
    } else if (resting) {
      this.large.want(index);
    } else {
      this.large.want(null);
      this.small.want(index);
    }
    if (moved || resting) this.state.textContent = this.labels[code] || "";
    // A stored file, with or without thumbnails, may be opened as it is.
    const stored = code === MEDIA_THUMBNAILS || code === MEDIA_NO_THUMBNAILS;
    this.original.hidden = !stored;
    if (stored) {
      const key = this.index.frames.key[index];
      this.original.href = mediaUrl(this.index.connection, this.index.media_kind, key, "original");
    } else {
      this.original.removeAttribute("href");
    }
    if (resting) {
      this.detail.want(index);
      this.#preload(index);
      this.#near(t);
      const key = this.index.frames.key[index];
      history.replaceState(null, "", `?frame=${encodeURIComponent(key)}`);
    } else {
      this.detail.want(null);
      this.text.textContent = "";
    }
  }

  move(index) {
    this.show(index, false);
    clearTimeout(this.restTimer);
    this.restTimer = setTimeout(() => this.show(this.current, true), REST_MS);
  }

  rest(index) {
    clearTimeout(this.restTimer);
    this.show(index, true);
  }

  #tile(label, code) {
    this.image.hidden = true;
    this.image.removeAttribute("src");
    this.tile.className = `tile tile-${code}`;
    this.tile.textContent = label;
    this.tile.hidden = false;
  }

  #showNothing() {
    this.#tile("", 0);
    this.tile.hidden = true;
  }

  #load(index, size, done) {
    const key = this.index.frames.key[index];
    const url = mediaUrl(this.index.connection, this.index.media_kind, key, size);
    const probe = new Image();
    probe.decoding = "async";
    probe.onload = () => {
      if (this.current === index) {
        this.image.src = url; // from the cache the probe just filled
        this.image.alt = `Screenshot at ${this.time.textContent}`;
        this.image.hidden = false;
        this.tile.hidden = true;
      }
      done();
    };
    probe.onerror = () => {
      // A record can outlive its file: a tile, never a broken image.
      if (this.current === index) this.#tile("The image could not be loaded", 2);
      done();
    };
    probe.src = url;
    return () => {
      probe.onload = null;
      probe.onerror = null;
      probe.src = ""; // stops the request
    };
  }

  #preload(index) {
    // Modestly: the two frames on each side, small size, once each.
    const codes = this.index.frames.media;
    for (const i of [index + 1, index - 1, index + 2, index - 2]) {
      if (i < 0 || i >= this.times.length || imageFor(codes[i], false) === null) continue;
      const key = this.index.frames.key[i];
      if (this.preloaded.has(key)) continue;
      this.preloaded.add(key);
      new Image().src = mediaUrl(this.index.connection, this.index.media_kind, key, 360);
    }
  }

  #detail(index, done) {
    const controller = new AbortController();
    getJson(frameUrl(this.index.frames.key[index]), controller.signal)
      .then((frame) => {
        if (this.current !== index) return;
        if (frame.withheld) {
          this.state.textContent = `Withheld (${frame.withheld}): no text, no screenshot.`;
        } else {
          const masked = frame.masked ? ` · ${frame.masked} masked` : "";
          this.state.textContent = `${this.labels[this.index.frames.media[index]] || ""}${masked}`;
        }
        this.text.textContent = frame.text || "";
        if (frame.text_truncated) this.text.textContent += "\n…the text is cut here.";
      })
      .catch((error) => {
        if (error.name !== "AbortError" && this.current === index) this.say(error.message);
      })
      .finally(done);
    return () => controller.abort();
  }

  // ---- lanes ------------------------------------------------------------------

  #legend() {
    const legend = document.getElementById("app-legend");
    legend.replaceChildren(
      ...this.apps.map((app) => {
        const item = document.createElement("li");
        item.append(chip(app));
        return item;
      }),
    );
  }

  #canvas(canvas) {
    const width = canvas.parentElement.clientWidth;
    const height = canvas.parentElement.clientHeight;
    const ratio = window.devicePixelRatio || 1;
    canvas.width = Math.max(1, Math.round(width * ratio));
    canvas.height = Math.max(1, Math.round(height * ratio));
    const context = canvas.getContext("2d");
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, width, height);
    return { context, width, height };
  }

  #drawLanes() {
    if (!this.index) return;
    const screen = this.#canvas(this.screenLane);
    for (const run of appRuns(this.times, this.index.frames.app)) {
      const app = run.app === null ? null : this.apps[run.app];
      screen.context.fillStyle = app ? safeColour(app.colour) : "#9e9e9e";
      const x = xForTime(run.from, this.length, screen.width);
      const w = Math.max(1, xForTime(run.to, this.length, screen.width) - x);
      screen.context.fillRect(x, 0, w, screen.height);
    }
    const browser = this.#canvas(this.browserLane);
    browser.context.fillStyle = getComputedStyle(this.browserLane).color;
    for (const visit of this.visits) {
      const x = xForTime(visit.t, this.length, browser.width);
      browser.context.fillRect(Math.floor(x), 0, 2, browser.height);
    }
    if (this.current >= 0) this.#cursor(this.times[this.current]);
  }

  #cursor(t) {
    for (const cursor of this.root.querySelectorAll(".cursor")) {
      const width = cursor.parentElement.clientWidth;
      cursor.style.transform = `translateX(${xForTime(t, this.length, width)}px)`;
    }
  }

  async #loadVisits() {
    const start = Date.parse(this.index.start);
    let after = null;
    try {
      for (let page = 0; page < VISIT_PAGES; page++) {
        const query = new URLSearchParams({
          from: this.day,
          to: this.day,
          source: "browser",
          limit: "500",
        });
        if (after) query.set("after", after);
        const found = await getJson(`/api/timeline/events?${query}`);
        for (const event of found.events) {
          this.visits.push({
            t: (Date.parse(event.started_at) - start) / 1000,
            title: event.title,
            url: event.body,
            site: event.group,
          });
        }
        after = found.next;
        if (!after) break;
      }
    } catch (error) {
      this.say(error.message);
    }
    this.#drawLanes();
    if (this.current >= 0) this.#near(this.times[this.current]);
    else if (this.visits.length) this.#near(this.visits[0].t);
  }

  #near(t) {
    const close = this.visits
      .filter((visit) => Math.abs(visit.t - t) <= NEAR_SECONDS)
      .sort((a, b) => Math.abs(a.t - t) - Math.abs(b.t - t))
      .slice(0, 8)
      .sort((a, b) => a.t - b.t);
    this.near.replaceChildren(
      ...close.map((visit) => {
        const item = document.createElement("li");
        const at = document.createElement("time");
        at.textContent = clock(this.index.start, visit.t, this.timeZone);
        const title = document.createElement(/^https?:\/\//.test(visit.url || "") ? "a" : "span");
        title.textContent = visit.title || visit.url || "";
        if (title.tagName === "A") {
          title.href = visit.url;
          title.rel = "noreferrer noopener";
          title.target = "_blank";
        }
        const site = document.createElement("span");
        site.className = "muted";
        site.textContent = visit.site || "";
        item.append(at, " ", title, " ", site);
        return item;
      }),
    );
    if (close.length === 0) {
      const item = document.createElement("li");
      item.className = "muted";
      item.textContent = "None within fifteen minutes.";
      this.near.replaceChildren(item);
    }
  }

  // ---- input --------------------------------------------------------------------

  #wire() {
    const key = (event) => {
      const step = keyStep(event.key, event.shiftKey);
      if (step === null || this.times.length === 0) return;
      event.preventDefault();
      this.move(applyStep(this.times, this.current, step));
    };
    this.scrubber.addEventListener("keydown", key);
    this.scrubber.addEventListener("input", () => {
      this.move(frameAt(this.times, Number(this.scrubber.value)));
    });
    this.scrubber.addEventListener("change", () => this.rest(this.current));
    for (const lane of [this.screenLane, this.browserLane]) {
      lane.addEventListener("keydown", key);
      const seek = (event) => {
        if (this.times.length === 0) return;
        const box = lane.getBoundingClientRect();
        const t = timeForX(event.clientX - box.left, this.length, box.width);
        return frameAt(this.times, t);
      };
      let dragging = null; // the pointer that scrubs this lane, mouse or finger
      lane.addEventListener("pointerdown", (event) => {
        dragging = event.pointerId;
        try {
          lane.setPointerCapture(event.pointerId); // keeps the drag off the lane
        } catch {
          // a pointer the browser no longer knows: the drag still works on it
        }
        const index = seek(event);
        if (index !== undefined) this.move(index);
      });
      lane.addEventListener("pointermove", (event) => {
        if (event.pointerId !== dragging) return;
        const index = seek(event);
        if (index !== undefined && index !== this.current) this.move(index);
      });
      const stop = (event) => {
        if (event.pointerId !== dragging) return;
        dragging = null;
        this.rest(this.current);
      };
      lane.addEventListener("pointerup", stop);
      lane.addEventListener("pointercancel", stop);
    }
  }
}

for (const element of document.querySelectorAll(".app-chip[data-colour]")) {
  paint(element, element.dataset.colour);
}
const root = document.getElementById("timeline-day");
if (root && document.getElementById("scrubber")) new DayView(root).start();
// Search results come from a POST. Their history entry is made an ordinary
// one, so Back returns to the empty form (no words, nothing to post again)
// instead of the browser's "resend the form?" page.
if (document.querySelector(".timeline-search-page")) {
  history.replaceState(null, "", location.pathname);
}
// A page brought back from the back-forward cache after logging out shows
// nothing it kept: it loads again, and the server asks for the login.
window.addEventListener("pageshow", (event) => {
  if (event.persisted) location.reload();
});
