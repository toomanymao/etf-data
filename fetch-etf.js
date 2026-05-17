const https = require('https');
const fs = require('fs');

const SOSO_KEY = process.env.SOSO_KEY;
const ETFS = ['IBIT','FBTC','GBTC','ARKB','BITB','BTCO','HODL','BRRR','EZBC','BTCW','BTC','MSBT'];

const US_HOLIDAYS = new Set([
    '2024-01-01','2024-01-15','2024-02-19','2024-03-29','2024-05-27','2024-06-19','2024-07-04','2024-09-02','2024-11-28','2024-12-25',
    '2025-01-01','2025-01-20','2025-02-17','2025-04-18','2025-05-26','2025-06-19','2025-07-04','2025-09-01','2025-11-27','2025-12-25',
    '2026-01-01','2026-01-19','2026-02-16','2026-04-03','2026-05-25','2026-06-19','2026-07-03','2026-09-07','2026-11-26','2026-12-25',
]);

function isTradingDay(date) {
    const d = new Date(date + 'T12:00:00Z');
    return d.getDay() !== 0 && d.getDay() !== 6 && !US_HOLIDAYS.has(date);
}

function postRequest(body) {
    return new Promise((resolve, reject) => {
        const data = JSON.stringify(body);
        const options = {
            hostname: 'api.sosovalue.xyz',
            port: 443,
            path: '/openapi/v2/etf/historicalInflowChart',
            method: 'POST',
            headers: {
                'x-soso-api-key': SOSO_KEY,
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(data),
            },
            rejectUnauthorized: false,
        };
        const req = https.request(options, res => {
            let raw = '';
            res.on('data', chunk => raw += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(raw)); }
                catch(e) { reject(new Error('JSON parse failed')); }
            });
        });
        req.on('error', reject);
        req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
        req.write(data);
        req.end();
    });
}

async function main() {
    console.log('Starting... API key:', SOSO_KEY ? SOSO_KEY.slice(0,8)+'...' : 'MISSING');
    if (!SOSO_KEY) { console.error('No API key'); process.exit(1); }

    const res = await postRequest({ type: 'us-btc-spot' });
    console.log('Response code:', res.code);

    // Handle both array formats: res.data[] or res.data.list[]
    let list = [];
    if (Array.isArray(res.data)) list = res.data;
    else if (Array.isArray(res.data?.list)) list = res.data.list;

    console.log('List length:', list.length);
    if (list.length > 0) console.log('Sample:', JSON.stringify(list[0]).slice(0,120));

    const freshRows = list
        .map(item => {
            const date = item.date;
            if (!date || !isTradingDay(date)) return null;
            const totalM = parseFloat((item.totalNetInflow / 1e6).toFixed(2));
            if (totalM === 0) return null;
            const row = { date };
            ETFS.forEach(e => { row[e] = null; });
            row.total = totalM;
            return row;
        })
        .filter(Boolean)
        .sort((a, b) => b.date.localeCompare(a.date));

    console.log('Valid rows:', freshRows.length);

    // Merge with existing
    let existingRows = [];
    if (fs.existsSync('etf-flows.json')) {
        try {
            const existing = JSON.parse(fs.readFileSync('etf-flows.json', 'utf8'));
            existingRows = (existing.rows || []).filter(r => r.total !== 0 && isTradingDay(r.date));
        } catch(e) {}
    }

    const merged = {};
    existingRows.forEach(r => { merged[r.date] = r; });
    freshRows.forEach(r => { merged[r.date] = r; });

    const finalRows = Object.values(merged)
        .filter(r => r.total !== 0 && isTradingDay(r.date))
        .sort((a, b) => b.date.localeCompare(a.date));

    fs.writeFileSync('etf-flows.json', JSON.stringify({
        generated: new Date().toISOString(),
        source: 'SoSoValue API via GitHub Actions',
        etfs: ETFS,
        count: finalRows.length,
        latest: finalRows[0]?.date,
        oldest: finalRows[finalRows.length-1]?.date,
        rows: finalRows,
    }, null, 2));

    console.log('Done! Written', finalRows.length, 'rows. Latest:', finalRows[0]?.date);
}

main().catch(err => {
    console.error('Error:', err.message);
    process.exit(0);
});
