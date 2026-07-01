const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

async function backfill() {
    let totalUpdated = 0;

    for (const saleType of ["CASH_SALE", "CREDIT_SALE"]) {
        console.log(`\n📦 Processing ${saleType}...`);
        const groups = await db.collection(saleType).get();

        for (const groupDoc of groups.docs) {
            const groupId = groupDoc.id;
            const salesSnap = await db
                .collection(saleType).doc(groupId)
                .collection("sales").get();

            for (const productDoc of salesSnap.docs) {
                const entriesSnap = await db
                    .collection(saleType).doc(groupId)
                    .collection("sales").doc(productDoc.id)
                    .collection("entries").get();

                if (entriesSnap.empty) continue;

                const batch = db.batch();
                let count = 0;

                for (const entryDoc of entriesSnap.docs) {
                    const data = entryDoc.data();
                    if (!data.groupId || !data.saleType) {
                        batch.update(entryDoc.ref, { groupId, saleType });
                        count++;
                    }
                }

                if (count > 0) {
                    await batch.commit();
                    totalUpdated += count;
                    console.log(`  ✅ ${saleType}/${groupId}/${productDoc.id} → backfilled ${count} entries`);
                }
            }
        }
    }

    console.log(`\n🎉 Done! Total entries backfilled: ${totalUpdated}`);
    process.exit(0);
}

backfill().catch(e => {
    console.error("❌ Error:", e);
    process.exit(1);
});
