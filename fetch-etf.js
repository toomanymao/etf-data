const https = require('https');
const fs = require('fs');

const SOSO_KEY = process.env.SOSO_KEY;
const ETFS = ['IBIT','FBTC','GBTC','ARKB','BITB','BTCO','HODL','BRRR','EZBC','BTCW','BTC','MSBT'];

const ETF_TYPES = [
    ['IBIT','Etf_NASDAQ_IBIT'],
    ['FBTC','Etf_NYSE_FBTC'],
    ['GBTC','Etf_NYSE_GBTC'],
    ['ARKB','Etf_CBOE_ARKB'],
    ['BITB','Etf_NYSE_BITB'],
    ['BTCO','Etf_NYSE_BTCO'],
    ['HODL','Etf_CBOE_HODL'],
    ['BRRR','Etf_NASDAQ_BRRR'],
    ['EZBC','Etf_CBOE_EZBC'],
    ['BTCW','Etf_NYSE_BTCW'],
    ['BTC','Etf_NYSE_BTC'],
    ['MSBT','Etf_NYSE_MSBT'],
];

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
    console.log('Starting... key:', SOSO_KEY ? SOSO_KEY.slice(0,8)+'...' : 'MISSING');
    if (!SOSO_KEY) { console.error('No API key'); process.exit(1); }

    // Step 1: Fetch aggregate totals
    console.log('\n--- Fetching aggregate totals ---');
    const aggRes = await postRequest({ type: 'us-btc-spot' });
    console.log('Code:', aggRes.code, 'rows:', aggRes.data?.length ?? aggRes.data?.list?.length ?? 0);

    let list = Array.isArray(aggRes.data) ? aggRes.data : (aggRes.data?.list || []);
    console.log('List length:', list.length);

    // Build a map of date -> total
    const totalsMap = {};
    list.forEach(item => {
        if (item.date && isTradingDay(item.date)) {
            totalsMap[item.date] = parseFloat((item.totalNetInflow / 1e6).toFixed(2));
        }
    });
    console.log('Trading day totals:', Object.keys(totalsMap).length, '| Latest:', Object.keys(totalsMap).sort().reverse()[0]);

    // Step 2: Fetch per-ETF breakdown
    console.log('\n--- Fetching per-ETF data ---');
    const perEtf = {}; // { date: { IBIT: x, FBTC: y, ... } }

    for (const [ticker, type] of ETF_TYPES) {
        try {
            const res = await postRequest({ type });
            const etfList = Array.isArray(res.data) ? res.data : (res.data?.list || []);
            let count = 0;
            etfList.forEach(item => {
                if (!item.date || !isTradingDay(item.date)) return;
                if (!perEtf[item.date]) perEtf[item.date] = {};
                perEtf[item.date][ticker] = parseFloat((item.totalNetInflow / 1e6).toFixed(2));
                count++;
            });
            console.log(ticker + ':', count, 'days | code:', res.code);
        } catch(e) {
            console.log(ticker + ': failed -', e.message);
        }
    }

    // Step 3: Build final rows combining totals + per-ETF
    const allDates = new Set([...Object.keys(totalsMap), ...Object.keys(perEtf)]);
    const freshRows = [];

    allDates.forEach(date => {
        if (!isTradingDay(date)) return;
        const total = totalsMap[date];
        if (total === undefined || total === 0) return;

        const row = { date };
        ETFS.forEach(e => {
            row[e] = perEtf[date]?.[e] ?? null;
        });
        row.total = total;
        freshRows.push(row);
    });

    freshRows.sort((a, b) => b.date.localeCompare(a.date));
    console.log('\nFresh rows built:', freshRows.length, '| Latest:', freshRows[0]?.date);

    // Check per-ETF coverage
    const hasPerEtf = freshRows.slice(0, 5).some(r => ETFS.some(e => r[e] !== null));
    console.log('Per-ETF data available:', hasPerEtf);

    // Step 4: Merge with existing file
    let existingRows = [];
    if (fs.existsSync('etf-flows.json')) {
        try {
            const existing = JSON.parse(fs.readFileSync('etf-flows.json', 'utf8'));
            existingRows = (existing.rows || []).filter(r => r.total !== 0 && isTradingDay(r.date));
            console.log('Existing rows:', existingRows.length);
        } catch(e) {}
    }

    const merged = {};
    existingRows.forEach(r => { merged[r.date] = r; });
    freshRows.forEach(r => { merged[r.date] = r; }); // fresh wins

    const finalRows = Object.values(merged)
        .filter(r => r.total !== 0 && isTradingDay(r.date))
        .sort((a, b) => b.date.localeCompare(a.date));

    fs.writeFileSync('etf-flows.json', JSON.stringify({
        generated: new Date().toISOString(),
        source: 'SoSoValue API via GitHub Actions',
        hasPerEtf,
        etfs: ETFS,
        count: finalRows.length,
        latest: finalRows[0]?.date,
        oldest: finalRows[finalRows.length-1]?.date,
        rows: finalRows,
    }, null, 2));

    console.log('\nDone! Written', finalRows.length, 'rows | Latest:', finalRows[0]?.date);
}

main().catch(err => {
    console.error('Fatal:', err.message);
    process.exit(0);
});
