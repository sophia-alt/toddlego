const { db } = require("./helpers");

/**
 * Collects writes so they can be committed in chunks below Firestore's 500-op batch limit.
 * Has the batch methods linkSeries() uses.
 */
function writeQueue() {
    const ops = [];
    return {
        set: (ref, data, options) => ops.push((b) => b.set(ref, data, options)),
        delete: (ref) => ops.push((b) => b.delete(ref)),
        size: () => ops.length,
        async commit() {
            for (let i = 0; i < ops.length; i += 450) {
                const batch = db.batch();
                ops.slice(i, i + 450).forEach((op) => op(batch));
                await batch.commit();
            }
        },
    };
}

module.exports = { writeQueue };
