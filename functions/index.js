const { onSchedule } = require("firebase-functions/v2/scheduler");
const { defineSecret } = require("firebase-functions/params");
const admin = require("firebase-admin");
const axios = require("axios");
const TurndownService = require("turndown");
// Use the correct class name
const { GoogleGenAI } = require("@google/genai");

const GEMINI_API_KEY = defineSecret("GEMINI_API_KEY");

admin.initializeApp();
const db = admin.firestore();
const turndown = new TurndownService();

exports.dailyLibraryScraper = onSchedule({
    schedule: "every 24 hours",
    secrets: [GEMINI_API_KEY]
}, async (event) => {
    console.log("🚀 Scraper Started!"); // Log 1: Confirmation of start

    const targetUrl = "https://aclibrary.bibliocommons.com/v2/events?_gl=1*1imqkwz*_ga*MTc2MDU0NTg1Ni4xNzY2NTMxNzQz*_ga_G99DMMNG39*czE3NjY1MzE3NDIkbzEkZzAkdDE3NjY1MzE3NDUkajU3JGwwJGgw*_ga_DJ3QFJ52TT*czE3NjY1MzE3NDIkbzEkZzAkdDE3NjY1MzE3NDIkajYwJGwwJGgw";

    try {
        const response = await axios.get(targetUrl);
        console.log("🌐 URL Fetched, Content Length:", response.data.length); // Log 2: Fetch check

        const markdown = turndown.turndown(response.data);

        // Initialize with the correct class
        const ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY.value() });

        // Call through the .models service
        const result = await ai.models.generateContent({
            model: "gemini-2.0-flash", // Use a widely available stable model
            contents: [{
                role: "user",
                parts: [{ text: `Analyze this library page: ${markdown}` }]
            }],
            config: {
                responseMimeType: "application/json", // Ensure clean JSON output
                // This is the "Brain" of your scraper
                systemInstruction: `
                    You are a specialized scraper for "Toddlego," an app for parents with kids aged 0-4.
                    Your goal is to extract library events ONLY if they are appropriate for toddlers.
                    
                    RULES:
                    1. Ignore events for "Teens," "Adults," or "School-age children."
                    2. If an event is "All Ages," include it only if the description mentions babies or toddlers.
                    3. Focus on keywords: Storytime, Playtime, Music & Movement, Stay & Play.
                    4. Always return a valid JSON array of objects with keys: title, venue, startTime (Unix timestamp), ageRange, isFree, registrationUrl.
                    5. If no toddler events are found, return an empty array [].
                `,
                // Add safety settings to prevent silent blocking
                safetySettings: [
                    { category: "HARM_CATEGORY_HARASSMENT", threshold: "BLOCK_ONLY_HIGH" },
                    { category: "HARM_CATEGORY_HATE_SPEECH", threshold: "BLOCK_ONLY_HIGH" },
                    { category: "HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold: "BLOCK_ONLY_HIGH" },
                    { category: "HARM_CATEGORY_DANGEROUS_CONTENT", threshold: "BLOCK_ONLY_HIGH" }
                ]
            }
        });

        // Debug: Log the entire response structure
        console.log("📊 Full Gemini Response:", JSON.stringify(result, null, 2));

        // Extract text from different possible response structures
        let rawText = null;
        
        // Try different ways to get the text
        if (result.text) {
            rawText = result.text;
            console.log("✅ Using result.text");
        } else if (result.response && typeof result.response.text === 'function') {
            rawText = result.response.text();
            console.log("✅ Using result.response.text()");
        } else if (result.candidates && result.candidates[0]) {
            const content = result.candidates[0].content;
            if (content && content.parts && content.parts[0]) {
                rawText = content.parts[0].text;
                console.log("✅ Using result.candidates[0].content.parts[0].text");
            }
        }

        console.log("🤖 Gemini Raw Output:", rawText); // Log 3: AI output check

        if (!rawText || rawText.trim() === "") {
            console.log("⚠️ Gemini returned empty response");
            return;
        }

        let activities = [];
        try {
            activities = JSON.parse(rawText);
        } catch (parseError) {
            console.error("❌ JSON Parse Error:", parseError);
            console.log("Raw text was:", rawText);
            return;
        }

        if (!Array.isArray(activities)) {
            console.error("❌ Response is not an array:", activities);
            return;
        }

        console.log(`✅ Found ${activities.length} activities.`); // Log 4: Final count

        if (activities.length === 0) {
            console.log("ℹ️ No toddler activities found in this scrape");
            return;
        }

        const batch = db.batch();
        activities.forEach((activity) => {
            // Validate required fields
            if (!activity.title || !activity.venue) {
                console.warn("⚠️ Skipping activity missing title or venue:", activity);
                return;
            }

            const docRef = db.collection("activities").doc();
            batch.set(docRef, {
                title: activity.title,
                venue: activity.venue,
                startTime: activity.startTime || Math.floor(Date.now() / 1000),
                ageRange: activity.ageRange || "0-4",
                isFree: activity.isFree !== false,
                registrationUrl: activity.registrationUrl || "",
                source: targetUrl,
                createdAt: new Date()
            });
        });

        // Return the final database write so the function stays alive
        const result2 = await batch.commit();
        console.log(`✅ Successfully committed ${result2.length} activities to Firestore`);
        return result2;
    } catch (error) {
        console.error("❌ Scraper Failed:", error); // Log 5: Error detail
        throw error;
    }
});
// updated fallback parsing
