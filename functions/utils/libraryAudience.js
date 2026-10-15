// Which library calendar events are for children 0-5, from their audience and category
// labels and title. Shared by the library calendar adapters (LibCal, Communico), whose
// labels differ by library: "Babies", "Toddler (19 mos-3 yrs)", "Early Childhood",
// "Infants & Toddlers", "Storytime", "Children's Program", ...
// No Firebase dependency, so source parsers can use it directly.

// Calendar entries that are not activities: closures and changed hours ("Holiday -- Library
// Closed", "CLOSED: Veterans Day", "Libraries Close @ 5PM", "Library Late Opening")
const NOT_AN_ACTIVITY = /\bclosed\b|closure|\blibrar(y|ies) closes?\b|late opening/i;
const CANCELLED_TITLE = /^(cancel+ed|postponed)\b/i;
// Audience, category and title words for young children. "Babytime" and "Baby (0-18 mos)"
// count, so no word boundary after "baby".
const BABY_TODDLER = /\bbab(y|ies)|toddler|infant|lapsit/i;
const PRESCHOOL = /pre-?school/i;
// Labels for the whole 0-5 range
const EARLY_CHILDHOOD = /early childhood|birth to 5\b|\b0\s*-\s*5\b/i;
const STORYTIME = /story ?time|cuentos|rhyme ?time/i;
const FAMILY = /\bfamil(y|ies)\b/i;
// Labels for school-age or grown-up audiences ("Grades 1-6", "Children (grades K-5)",
// "Tweens", "Adults 18+"). They exclude an event only when it has no young-child label.
const OLDER_AUDIENCE = /grade|\bK\s?-\s?\d|tween|teen|middle school|high school|school age|elementary|adult|senior/i;
// Grown-up labels. Some libraries tag events for everyone with every age group, from
// "Early Childhood" to "Adults"; on those, the young-child labels say nothing.
const GROWN_UP_AUDIENCE = /teen|adult|senior/i;
const SCHOOL_AGE_TITLE = /homework|after[- ]?school|tutor|\btweens?\b|\bteens?\b|grades?\s*\d|\bK\s?-\s?\d/i;
// An age span in a title, e.g. "Storytime for Ages 2-5", "Tween LEGO Club (Ages 9-13)"
const AGE_SPAN = /\bages?\s*(\d{1,2})\s*(?:-|–|to)\s*(\d{1,2})\b/i;

/** Youngest age ToddleGo is for is 0; programs whose minimum age is above this are left out. */
const MAX_MIN_AGE = 5;

/**
 * Decides whether an event suits children 0-5, and the age range to show.
 * @param {string[]} audiences - Audience or age labels ("Babies", "Early Childhood").
 * @param {string[]} categories - Category or tag labels ("Storytime", "Children's Program").
 * @param {string} title - Decoded title.
 * @returns {{include: boolean, ageRange: string|null, ageRangeYears: number[]|null}}
 */
function classifyEvent(audiences, categories, title) {
    const no = { include: false, ageRange: null, ageRangeYears: null };
    const labels = [...audiences, ...categories];
    if (labels.some((l) => NOT_AN_ACTIVITY.test(l)) || NOT_AN_ACTIVITY.test(title)) return no;

    const text = [...labels, title];
    // Young-child labels count from the audiences only when the event is not for everyone;
    // categories and the title always count ("Baby Storytime")
    const forEveryone = audiences.some((a) => GROWN_UP_AUDIENCE.test(a));
    const young = [...(forEveryone ? [] : audiences), ...categories, title];
    const early = young.some((t) => EARLY_CHILDHOOD.test(t));
    const baby = early || young.some((t) => BABY_TODDLER.test(t));
    const preschool = early || young.some((t) => PRESCHOOL.test(t));

    const span = title.match(AGE_SPAN);
    if (span) {
        const [min, max] = [Number(span[1]), Number(span[2])];
        if (min > MAX_MIN_AGE || max < min) return no;
        if (!baby && !preschool && audiences.some((a) => OLDER_AUDIENCE.test(a))) return no; // "Ages 5-11", Grades 1-6
        return { include: true, ageRange: `${min}-${max} years`, ageRangeYears: [min, max] };
    }
    if (baby && preschool) return { include: true, ageRange: "0-5 years", ageRangeYears: [0, 5] };
    if (baby) return { include: true, ageRange: "0-3 years", ageRangeYears: [0, 3] };
    if (preschool) return { include: true, ageRange: "3-5 years", ageRangeYears: [3, 5] };

    // No young-child label: school-age and grown-up programs are left out
    if (SCHOOL_AGE_TITLE.test(title)) return no;
    const family = audiences.some((a) => FAMILY.test(a));
    const olderOnly = audiences.some((a) => OLDER_AUDIENCE.test(a)) && !audiences.some((a) => /child/i.test(a));
    if (!family && (forEveryone || olderOnly)) return no;

    // Library storytimes without a stated age are for babies to preschoolers
    if (text.some((t) => STORYTIME.test(t))) return { include: true, ageRange: "0-5 years", ageRangeYears: [0, 5] };
    if (family) return { include: true, ageRange: "All", ageRangeYears: null };
    // "Children" alone covers school-age clubs and classes, so it is not enough
    return no;
}

module.exports = { CANCELLED_TITLE, classifyEvent };
