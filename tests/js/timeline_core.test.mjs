// Run by tests/test_timeline_js.py with Node's own test runner; no dependency.
import assert from "node:assert/strict";
import { test } from "node:test";

import {
  LatestOnly,
  MEDIA_NO_THUMBNAILS,
  MEDIA_THUMBNAILS,
  MEDIA_UNKNOWN,
  MEDIA_WITHHELD,
  appRuns,
  applyStep,
  clock,
  frameAt,
  frameUrl,
  groupColour,
  imageFor,
  jumpFrame,
  keyStep,
  mediaUrl,
  safeColour,
  snippetParts,
  textOn,
  timeForX,
  xForTime,
} from "../../src/viewer/static/timeline-core.js";

const times = [0, 60, 120, 3600, 3700, 7200, 7200, 86390];

test("a time maps to the last frame at or before it", () => {
  assert.equal(frameAt([], 10), -1);
  assert.equal(frameAt(times, -5), 0);
  assert.equal(frameAt(times, 0), 0);
  assert.equal(frameAt(times, 59), 0);
  assert.equal(frameAt(times, 60), 1);
  assert.equal(frameAt(times, 7200), 6); // the last of two frames of one second
  assert.equal(frameAt(times, 90000), 7);
});

test("keys step frames, minutes and hours, and stay within the day", () => {
  const go = (index, key, shift = false) => applyStep(times, index, keyStep(key, shift));
  assert.equal(go(0, "ArrowRight"), 1);
  assert.equal(go(0, "ArrowLeft"), 0);
  assert.equal(go(7, "ArrowRight"), 7);
  assert.equal(go(0, "ArrowRight", true), 1); // a minute on: the frame at 60
  assert.equal(go(1, "ArrowRight", true), 2);
  assert.equal(go(0, "PageDown"), 3); // an hour on: the frame at 3600
  assert.equal(go(5, "PageUp"), 3); // an hour back from 7200: the frame at 3600
  assert.equal(go(4, "Home"), 0);
  assert.equal(go(1, "End"), 7);
  assert.equal(keyStep("a", false), null);
  // A jump always moves when it can, even inside a long gap.
  assert.equal(jumpFrame(times, 6, 60), 7);
  assert.equal(jumpFrame(times, 7, -60), 6);
});

test("lanes map time and position both ways, over 23- and 25-hour days", () => {
  for (const length of [82800, 86400, 90000]) {
    assert.equal(xForTime(0, length, 1000), 0);
    assert.equal(xForTime(length, length, 1000), 1000);
    assert.equal(xForTime(length * 2, length, 1000), 1000);
    assert.equal(Math.round(timeForX(xForTime(4321, length, 900), length, 900)), 4321);
  }
  assert.equal(timeForX(-10, 86400, 100), 0);
});

test("runs of one app join consecutive frames and break on a pause", () => {
  assert.deepEqual(appRuns([0, 10, 20, 1000], [0, 0, 1, 1]), [
    { from: 0, to: 20, app: 0 },
    { from: 20, to: 320, app: 1 },
    { from: 1000, to: 1001, app: 1 },
  ]);
  assert.deepEqual(appRuns([], []), []);
});

test("only frames with thumbnails get an image; the rest a tile", () => {
  assert.equal(imageFor(MEDIA_THUMBNAILS, false), 360);
  assert.equal(imageFor(MEDIA_THUMBNAILS, true), 1280);
  for (const code of [MEDIA_UNKNOWN, MEDIA_NO_THUMBNAILS, MEDIA_WITHHELD, 9]) {
    assert.equal(imageFor(code, true), null);
  }
});

test("addresses encode every part of a key but its slashes", () => {
  assert.equal(
    mediaUrl("screen", "screenshots", "2026/11/20/a b#?.jpeg", 360),
    "/media/screen/screenshots/2026/11/20/a%20b%23%3F.jpeg?size=360",
  );
  assert.equal(frameUrl("2026/11/20/x%.jpeg"), "/api/timeline/frames/2026/11/20/x%25.jpeg");
});

test("colours are only colours, and text on them stays readable", () => {
  assert.equal(safeColour("#2196f3"), "#2196f3");
  for (const bad of ["red", "#fff", "#2196f3; background: url(x)", null, 3]) {
    assert.equal(safeColour(bad), "#9e9e9e");
  }
  assert.equal(textOn("#ffffff"), "#000000");
  assert.equal(textOn("#000000"), "#ffffff");
  assert.equal(textOn("#fdd835"), "#000000"); // Chrome's yellow
  assert.equal(textOn("#611f69"), "#ffffff"); // Slack's aubergine
});

test("a snippet becomes text parts, matches marked, nothing parsed", () => {
  assert.deepEqual(snippetParts(["a ", "<b>", " c"]), [
    { text: "a ", hit: false },
    { text: "<b>", hit: true },
    { text: " c", hit: false },
  ]);
  assert.deepEqual(snippetParts(["", "hit"]), [{ text: "hit", hit: true }]);
  assert.deepEqual(snippetParts(null), []);
});

test("while one load runs, only the latest wish starts next", () => {
  const started = [];
  const finish = {};
  const cancelled = [];
  const loader = new LatestOnly((key, done) => {
    started.push(key);
    finish[key] = done;
    return () => cancelled.push(key);
  });
  loader.want("a");
  for (const key of ["b", "c", "d", "e"]) loader.want(key); // dragging past them
  assert.deepEqual(started, ["a"]);
  finish.a();
  assert.deepEqual(started, ["a", "e"]); // b, c and d were never requested
  finish.e();
  loader.want("e"); // already shown: nothing more
  assert.deepEqual(started, ["a", "e", "e"]);
  assert.deepEqual(cancelled, []);
  finish.a(); // a late answer for a frame long left changes nothing
  assert.deepEqual(started, ["a", "e", "e"]);
});

test("a large load for a frame left is cancelled at once", () => {
  const started = [];
  const cancelled = [];
  const loader = new LatestOnly(
    (key) => {
      started.push(key);
      return () => cancelled.push(key);
    },
    { cancelStale: true },
  );
  loader.want("a");
  loader.want("b");
  assert.deepEqual(cancelled, ["a"]);
  assert.deepEqual(started, ["a", "b"]);
  loader.want(null);
  assert.deepEqual(cancelled, ["a", "b"]);
});

test("loads start no closer than their spacing, the latest wish first", () => {
  let clockMs = 0;
  const timers = [];
  const started = [];
  const finish = {};
  const loader = new LatestOnly(
    (key, done) => {
      started.push([key, clockMs]);
      finish[key] = done;
    },
    { spacing: 100, now: () => clockMs, later: (run, ms) => timers.push([run, clockMs + ms]) },
  );
  loader.want("a");
  clockMs = 10;
  finish.a(); // fast: done long before the spacing has passed
  for (const key of ["b", "c", "d"]) loader.want(key);
  assert.deepEqual(started, [["a", 0]]);
  assert.equal(timers.length, 1); // one timer, however many wishes
  const [run, at] = timers.shift();
  clockMs = at;
  run();
  assert.deepEqual(started, [["a", 0], ["d", 100]]);
  loader.want(null);
  finish.d();
  assert.equal(timers.length, 0);
});

test("clock times follow the timezone across a change of offset", () => {
  // Toronto's 25-hour day: the clocks go back at 02:00.
  const start = "2026-11-01T00:00:00-04:00";
  assert.equal(clock(start, 0, "America/Toronto"), "00:00:00");
  assert.equal(clock(start, 3 * 3600, "America/Toronto"), "02:00:00");
  assert.equal(clock(start, 88200, "America/Toronto"), "23:30:00");
});

test("a group keeps one colour of the palette; no group is grey", () => {
  assert.equal(groupColour("example.org"), groupColour("example.org"));
  assert.match(groupColour("example.org"), /^#[0-9a-f]{6}$/);
  assert.equal(safeColour(groupColour("a conversation")), groupColour("a conversation"));
  assert.equal(groupColour(""), "#9e9e9e");
  assert.equal(groupColour(null), "#9e9e9e");
  const colours = new Set(["a", "b", "c", "d", "e", "f", "g", "h"].map(groupColour));
  assert.ok(colours.size > 1);
});
