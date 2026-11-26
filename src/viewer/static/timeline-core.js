// The timeline's logic that needs no page: tested on its own with `node --test`
// (tests/js/). Nothing here touches the DOM or the network.

// What a day index's `media` codes mean (src/viewer/reader.py).
export const MEDIA_UNKNOWN = 0;
export const MEDIA_THUMBNAILS = 1;
export const MEDIA_NO_THUMBNAILS = 2;
export const MEDIA_WITHHELD = 3;

// The last frame at or before `t` seconds into the day; the first frame if `t`
// comes before it; -1 for a day without frames. `times` is sorted.
export function frameAt(times, t) {
  if (times.length === 0) return -1;
  let lo = 0;
  let hi = times.length - 1;
  if (t < times[0]) return 0;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (times[mid] <= t) lo = mid;
    else hi = mid - 1;
  }
  return lo;
}

// The frame `delta` frames away, kept within the day.
export function stepFrame(index, delta, count) {
  if (count === 0) return -1;
  return Math.min(count - 1, Math.max(0, index + delta));
}

// The frame `seconds` away in time (a minute, an hour). Moving forward lands on
// the first frame at or after the target, so a jump always moves when it can.
export function jumpFrame(times, index, seconds) {
  if (times.length === 0) return -1;
  const target = times[index] + seconds;
  if (seconds >= 0) {
    let found = frameAt(times, target);
    if (times[found] < target && found < times.length - 1) found += 1;
    return Math.max(found, stepFrame(index, 1, times.length));
  }
  return Math.min(frameAt(times, target), stepFrame(index, -1, times.length));
}

// What a key press does to the current frame, or null for a key that is not
// the scrubber's: arrows a frame, Shift a minute, Page Up/Down an hour.
export function keyStep(key, shift) {
  switch (key) {
    case "ArrowLeft":
    case "ArrowDown":
      return shift ? { seconds: -60 } : { frames: -1 };
    case "ArrowRight":
    case "ArrowUp":
      return shift ? { seconds: 60 } : { frames: 1 };
    case "PageUp":
      return { seconds: -3600 };
    case "PageDown":
      return { seconds: 3600 };
    case "Home":
      return { frames: -Infinity };
    case "End":
      return { frames: Infinity };
    default:
      return null;
  }
}

// Apply a keyStep result.
export function applyStep(times, index, step) {
  if (step.frames !== undefined) {
    if (step.frames === -Infinity) return 0;
    if (step.frames === Infinity) return times.length - 1;
    return stepFrame(index, step.frames, times.length);
  }
  return jumpFrame(times, index, step.seconds);
}

// Where a time falls on a lane `width` pixels wide, for a day `length` seconds
// long (23 to 25 hours), and back.
export function xForTime(t, length, width) {
  return length > 0 ? (Math.min(Math.max(t, 0), length) / length) * width : 0;
}

export function timeForX(x, length, width) {
  return width > 0 ? (Math.min(Math.max(x, 0), width) / width) * length : 0;
}

// Runs of frames of one app, for the screen lane: [{from, to, app}], where a
// run ends at the next frame, or after `gap` seconds without one.
export function appRuns(times, apps, gap = 300) {
  const runs = [];
  for (let i = 0; i < times.length; i++) {
    const end =
      i + 1 < times.length ? Math.min(times[i + 1], times[i] + gap) : times[i] + 1;
    const last = runs[runs.length - 1];
    if (last && last.app === apps[i] && last.to === times[i]) last.to = end;
    else runs.push({ from: times[i], to: Math.max(end, times[i] + 1), app: apps[i] });
  }
  return runs;
}

// Which size of a frame to show: thumbnails exist only for code 1; anything
// else is a plain tile saying why, never a broken image.
export function imageFor(code, resting) {
  if (code !== MEDIA_THUMBNAILS) return null;
  return resting ? 1280 : 360;
}

// The address of a stored image, with every part of the key encoded but its
// slashes, which the route reads as part of the key.
export function mediaUrl(connection, kind, key, size) {
  const path = key.split("/").map(encodeURIComponent).join("/");
  return `/media/${encodeURIComponent(connection)}/${encodeURIComponent(kind)}/${path}?size=${size}`;
}

export function frameUrl(key) {
  return `/api/timeline/frames/${key.split("/").map(encodeURIComponent).join("/")}`;
}

// A colour the API gave, if it is one; never anything that could be read as
// more than a colour.
export function safeColour(value) {
  return typeof value === "string" && /^#[0-9a-fA-F]{6}$/.test(value)
    ? value
    : "#9e9e9e";
}

// Black or white text, whichever reads better on a background colour (WCAG
// relative luminance).
export function textOn(colour) {
  const hex = safeColour(colour).slice(1);
  const channel = (i) => {
    const c = parseInt(hex.slice(i, i + 2), 16) / 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  };
  const luminance = 0.2126 * channel(0) + 0.7152 * channel(2) + 0.0722 * channel(4);
  // Contrast with black (L + 0.05) / 0.05 against white 1.05 / (L + 0.05).
  return (luminance + 0.05) / 0.05 >= 1.05 / (luminance + 0.05) ? "#000000" : "#ffffff";
}

// A snippet from the API: alternating plain and matched parts, plain first.
// Returns [{text, hit}] for a page to set as text.
export function snippetParts(parts) {
  return (Array.isArray(parts) ? parts : [])
    .map((text, i) => ({ text: String(text), hit: i % 2 === 1 }))
    .filter((part) => part.text !== "");
}

// Loads one thing at a time, the latest asked for. While a load runs, newer
// wishes replace each other, and the one left when it ends starts next: frames
// passed over while dragging are never requested. With `spacing`, loads start
// at least that many milliseconds apart, so a fast connection does not fetch
// every frame dragged past either. With `cancelStale` (large images), a running
// load for a frame no longer wanted is cancelled at once. `start(key, done)`
// begins a load, calls `done()` when it ends, and returns a function that
// cancels it. `want(null)` wants nothing.
export class LatestOnly {
  constructor(
    start,
    {
      cancelStale = false,
      spacing = 0,
      now = () => Date.now(),
      later = (run, ms) => setTimeout(run, ms),
    } = {},
  ) {
    this.start = start;
    this.cancelStale = cancelStale;
    this.spacing = spacing;
    this.now = now;
    this.later = later;
    this.running = null; // {key, cancel}
    this.wanted = null;
    this.lastStart = -Infinity;
    this.waiting = false;
  }

  want(key) {
    this.wanted = key;
    if (this.running && this.running.key !== key && this.cancelStale) {
      this.running.cancel();
      this.running = null;
    }
    if (!this.running && key !== null) this.#next();
  }

  #next() {
    if (this.waiting) return;
    const wait = this.lastStart + this.spacing - this.now();
    if (wait > 0) {
      this.waiting = true;
      this.later(() => {
        this.waiting = false;
        if (!this.running && this.wanted !== null) this.#next();
      }, wait);
      return;
    }
    this.#begin(this.wanted);
  }

  #begin(key) {
    const entry = { key, cancel: () => {} };
    this.running = entry;
    this.lastStart = this.now();
    const cancel = this.start(key, () => {
      if (this.running !== entry) return; // cancelled, or replaced
      this.running = null;
      if (this.wanted !== null && this.wanted !== key) this.#next();
    });
    if (typeof cancel === "function") entry.cancel = cancel;
  }
}

// Clock time of a frame, in the analysis's timezone, from the day's start.
export function clock(startIso, t, timeZone) {
  const at = new Date(Date.parse(startIso) + t * 1000);
  return new Intl.DateTimeFormat("en-GB", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).format(at);
}
