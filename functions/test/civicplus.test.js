// Offline tests for the CivicPlus adapter (sources/civicplus.js) and the iCalendar parser
// (utils/ical.js). A synthetic feed and a fake fetch; no network, Firestore or paid APIs.
process.env.TZ = "America/Los_Angeles";
process.env.GCLOUD_PROJECT = process.env.GCLOUD_PROJECT || "offline-test";

const test = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const civicplus = require("../sources/civicplus");
const { parseEvents, parseDate } = require("../utils/ical");

const FEED = fs.readFileSync(path.join(__dirname, "fixtures", "civicplus-calendar.ics"), "utf8");
const NOW = Math.floor(Date.parse("2026-10-13T00:00:00Z") / 1000);
const CITY = {
    id: "examplecity",
    name: "City of Hayward",
    town: "Hayward",
    baseUrl: "https://www.example-city.gov",
    categories: { 31: "Special Events" },
    familyCategories: [],
    crawlDelaySec: 0,
};
const events = () => parseEvents(FEED).map((e) => ({ ...e, category: "Special Events", categoryId: 31 }));
const build = (uid) => civicplus.buildActivity(CITY, events().find((e) => e.uid === String(uid)), { nowSec: NOW });

test("parses folded lines, escaped text, Pacific local times and all-day dates", () => {
    const all = parseEvents(FEED);
    assert.strictEqual(all.length, 11, "the time zone's DTSTART/RRULE are not events");
    const preK = all.find((e) => e.uid === "501");
    assert.strictEqual(preK.summary, "Pre-K: Pumpkins, Ghosts & Giggles");
    assert.strictEqual(preK.location, "Kenneth C. Aitken Senior & Community Center > Room 2 - 17800 Redwood Road  Castro Valley CA 94546");
    assert.strictEqual(preK.start, Date.parse("2026-10-23T17:00:00Z") / 1000, "10 AM PDT");
    assert.strictEqual(preK.allDay, false);
    const festival = all.find((e) => e.uid === "507");
    assert.strictEqual(festival.allDay, true);
    assert.strictEqual(festival.start, Date.parse("2026-11-27T08:00:00Z") / 1000, "midnight PST");
    assert.strictEqual(parseDate("20261023T170000Z").sec, Date.parse("2026-10-23T17:00:00Z") / 1000);
    assert.ok(Number.isNaN(parseDate("soon").sec));
});

test("keeps events for young children and families, by title", () => {
    const cls = (t) => civicplus.classifyEvent(t);
    assert.deepStrictEqual(cls("Pre-K: Gratitude Gathering"), { include: true, ageRange: "3-5 years", ageRangeYears: [3, 5] });
    assert.strictEqual(cls("Serramonte Baby Bounce Storytime").ageRange, "0-3 years");
    assert.strictEqual(cls("Westlake Family Storytime").ageRange, "0-5 years");
    assert.strictEqual(cls("Elf Workshop (1-9 Yrs)").ageRange, "1-9 years");
    for (const t of ["Trunk or Treat", "Crawl-O-Ween", "Kids' Indoor Play Space", "Mobile Rec at Rochambeau Playground",
        "Photos with Santa at Magical Bridge", "Napa Dia de los Muertos", "Family Night: Halloween Bash"]) {
        assert.deepStrictEqual(cls(t), { include: true, ageRange: "All", ageRangeYears: null }, t);
    }
});

test("leaves out meetings, grown-up and older-child programs, and untagged events", () => {
    for (const t of ["Regular City Council Meeting", "Parks & Recreation Commission", "Halloween Movie Night at the Senior Center",
        "Holiday Adult 50+ Social Dance", "Tricks F'or Teens: Halloween Costume Workshop", "Boos and Brews on B Street",
        "Halloween Improv Night at the Village Theatre", "Parents' Night Out-LEGO STEM", "Kids Night Out",
        "Volunteer at the Randall Family Halloween Event", "Adaptive Halloween Dance", "Teen Adaptive Hangout Wednesdays, Grades 6-12",
        "Ages 6-12 Soccer Clinic", "Chess and Mahjong", "Christmas Day", "Howl-O-Ween - Dog Costume Contest and Party",
        "Golden Age Matinee"]) {
        assert.strictEqual(civicplus.classifyEvent(t).include, false, t);
    }
});

test("family categories keep untagged community events, but not grown-up, older-child or pet ones", () => {
    const fam = (t) => civicplus.classifyEvent(t, { familyCategory: true });
    assert.deepStrictEqual(fam("H.A.R.D. Holiday Lights: Meek Mansion"), { include: true, ageRange: "All", ageRangeYears: null });
    assert.strictEqual(fam("Pre-K: Gratitude Gathering").ageRange, "3-5 years", "a stated age still wins");
    assert.strictEqual(civicplus.classifyEvent("H.A.R.D. Holiday Lights: Meek Mansion").include, false, "other categories need a family word");
    for (const t of ["Volunteers in Parks -- Memorial Park", "Parents' Night Out-LEGO STEM", "Beneficial Bats - For Adult Nature Lovers",
        "Golden Age Matinee", "Howl-O-Ween - Dog Costume Contest and Party", "Board of Directors Meeting"]) {
        assert.strictEqual(fam(t).include, false, t);
    }
});

test("maps a Pre-K event with its venue, room and event page", () => {
    const { activityId, activity, venue } = build(501);
    assert.strictEqual(activityId, "civicplus_examplecity_501");
    assert.strictEqual(activity.title, "Pre-K: Pumpkins, Ghosts & Giggles");
    assert.strictEqual(activity.venue, "Kenneth C. Aitken Senior & Community Center, Castro Valley");
    assert.strictEqual(activity.locationDetails, "Room 2");
    assert.strictEqual(activity.durationMinutes, 60);
    assert.strictEqual(activity.durationEstimated, false);
    assert.strictEqual(activity.ageRange, "3-5 years");
    assert.strictEqual(activity.isIndoor, true);
    assert.strictEqual(activity.venueType, "community_center");
    assert.strictEqual(activity.isFree, false, "city calendars list no prices");
    assert.strictEqual(activity.description, null, "the description is only the page URL");
    assert.strictEqual(activity.sourceUrl, "https://www.example-city.gov/Calendar.aspx?EID=501");
    assert.strictEqual(activity.source, "civicplus");
    assert.strictEqual(activity.tenant, "examplecity");
    assert.deepStrictEqual(activity.tags, ["Special Events"]);
    assert.deepStrictEqual(venue, {
        name: "Kenneth C. Aitken Senior & Community Center, Castro Valley",
        geocodeQuery: "Kenneth C. Aitken Senior & Community Center, 17800 Redwood Road Castro Valley CA 94546",
    });
});

test("venue names carry their city, so places in different cities stay apart", () => {
    assert.strictEqual(build(502).activity.venue, "Kennedy Park, Hayward");
    assert.strictEqual(build(502).activity.isIndoor, false);
    assert.strictEqual(build(502).activity.venueType, "park");
    assert.strictEqual(build(505).activity.venue, "Serramonte Main Library, Daly City");
    assert.strictEqual(build(505).activity.venueType, "library");
    // A room inside a park building is indoors
    assert.strictEqual(build(511).activity.isIndoor, true);
    // No place name: the street address is the venue
    const streets = build(506);
    assert.strictEqual(streets.activity.venue, "Mission St and Avalon Avenue San Francisco");
    assert.strictEqual(streets.venue.geocodeQuery, "Mission St and Avalon Avenue San Francisco CA 94114");
    assert.strictEqual(streets.activity.description, "Come out to the Dunes for live music on every 2nd Sunday.");
    // HTML in the place name; the site's town when the address names none
    assert.strictEqual(civicplus.eventVenue(CITY, "<p>Civic Center Plaza</p> - 335 McAllister St").name, "Civic Center Plaza, Hayward");
});

test("an untagged event is kept only in a family category", () => {
    const tour = { uid: "601", summary: "Tour the Gardens", location: "Meek Estate Park - 240 Hampton Road  Hayward CA 94541",
        start: NOW + 86400, end: NOW + 90000, allDay: false, description: "", status: "", category: "Special Events", categoryId: 31 };
    assert.strictEqual(civicplus.buildActivity(CITY, tour, { nowSec: NOW }), null);
    const kept = civicplus.buildActivity({ ...CITY, familyCategories: [31] }, tour, { nowSec: NOW });
    assert.strictEqual(kept.activity.ageRange, "All");
    assert.strictEqual(kept.activity.venue, "Meek Estate Park, Hayward");
});

test("all-day events end on their day; events with no location are skipped", () => {
    const festival = build(507).activity;
    assert.strictEqual(festival.isAllDay, true);
    assert.strictEqual(festival.venue, "Historic Downtown, Danville");
    assert.strictEqual(festival.endTime - festival.startTime, 24 * 60 * 60 - 60);
    assert.strictEqual(build(508), null, "no place or address");
});

test("cancelled events are reported; left-out events are null", () => {
    assert.deepStrictEqual(build(509), { activityId: "civicplus_examplecity_509", activity: null, cancelled: true });
    for (const uid of [503, 504, 510]) assert.strictEqual(build(uid), null, String(uid));
});

test("reads each category feed once, keeps an event listed twice once, and survives a failed feed", async () => {
    const city = { ...CITY, categories: { 31: "Special Events", 35: "Arts", 40: "Teens" }, familyCategories: [35] };
    const urls = [];
    const fetchImpl = async (url) => {
        urls.push(url);
        if (url.includes("catID=40")) return { ok: false, status: 500 };
        return { ok: true, status: 200, text: async () => FEED };
    };
    const { events: got, complete } = await civicplus.fetchAllEvents(city, { fetchImpl, sleep: async () => {} });
    assert.deepStrictEqual(urls, [35, 31, 40].map((c) => civicplus.feedUrl(city, c)), "family categories first");
    assert.strictEqual(got.length, 11);
    assert.strictEqual(got[0].category, "Arts", "first category wins");
    assert.strictEqual(got[0].categoryId, 35);
    assert.strictEqual(complete, false, "a failed feed means unlisted events are not removed");

    const down = async () => ({ ok: false, status: 403 });
    await assert.rejects(civicplus.fetchAllEvents(city, { fetchImpl: down, sleep: async () => {} }), /every category feed failed/);
});

test("supersedes Gemini-scraped activities from the same site", () => {
    assert.strictEqual(civicplus.isSupersededUrl("https://example-city.gov/Calendar.aspx?EID=9", CITY), true);
    assert.strictEqual(civicplus.isSupersededUrl("https://other.gov/events", CITY), false);
    assert.strictEqual(civicplus.isSupersededUrl("not a url", CITY), false);
});

test("site config: every city has a town and calendar categories, and reads its family categories", () => {
    const ids = new Set();
    for (const c of civicplus.CIVICPLUS_CITIES) {
        assert.ok(!ids.has(c.id), `duplicate ${c.id}`);
        ids.add(c.id);
        assert.ok(c.town && Object.keys(c.categories).length > 0, c.id);
        for (const id of c.familyCategories) assert.ok(String(id) in c.categories, `${c.id} family category ${id}`);
        assert.match(c.baseUrl, /^https:\/\/[^/]+$/);
    }
});
