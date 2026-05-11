/**
 * fetch-etf.js
 * Runs inside GitHub Actions — fetches SoSoValue API and writes etf-flows.json
 * No npm install needed — uses Node.js built-in https module only
 */

const https = require('https');
const fs    = require('fs');

const SOSO_KEY = process.env.SOSO_KEY;
const ETFS = ['IBIT','FBTC','GBTC','ARKB','BITB','BTCO','HODL','BRRR','EZBC','BTCW','BTC','MSBT'];

if (!SOSO_KEY) {
    console.error('ERROR: SOSO_KEY environment variable not set');
    console.error('Add SOSOVALUE_API_KEY to your GitHub repo secrets');
    process.exit(1);
}

// Simple HTTPS POST using built-in Node.js — no axios, no npm install needed
function post(path, body) {
    return new Promise((resolve, reject) => {
        const data    = JSON.stringify(body);
        const options = {
            hostname: 'api.sosovalue.xyz',
            port:     443,
            path,
            method:   'POST',
            headers: {
                'x-soso-api-key':  SOSO_KEY,
                'Content-Type':    'application/json',
                'Content-Length':  Buffer.byteLength(data),
            },
            rejectUnauthorized: false, // fix for GitHub Actions SSL cert issue
        };

        const req = https.request(options, res => {
            let raw = '';
            res.on('data', chunk => raw += chunk);
            res.on('end', () => {
                try   { resolve(JSON.parse(raw)); }
                catch { reject(new Error('Invalid JSON: ' + raw.slice(0, 200))); }
            });
        });

        req.on('error', reject);
        req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
        req.write(data);
        req.end();
    });
}

async function main() {
    console.log(`[${new Date().toISOString()}] Fetching ETF data from SoSoValue...`);

    // 1. Fetch aggregate totals (free tier — last 300 days)
    const agg = await post('/openapi/v2/etf/historicalInflowChart', { type: 'us-btc-spot' });

    console.log('API response code:', agg.code);
    console.log('API message:', agg.msg);
    console.log('Data rows received:', agg.data?.list?.length ?? 0);

    if (agg.code !== 0) {
        throw new Error(`SoSoValue aggregate error: ${agg.msg}`);
    }

    const list = agg.data?.list || [];
    console.log(`Got ${list.length} days of aggregate data. Latest: ${list[0]?.date}`);

    // 2. Fetch per-ETF data in parallel
    const ETF_TYPES = [
        ['IBIT','Etf_NASDAQ_IBIT'],['FBTC','Etf_NYSE_FBTC'],['GBTC','Etf_NYSE_GBTC'],
        ['ARKB','Etf_CBOE_ARKB'], ['BITB','Etf_NYSE_BITB'], ['BTCO','Etf_NYSE_BTCO'],
        ['HODL','Etf_CBOE_HODL'], ['BRRR','Etf_NASDAQ_BRRR'],['EZBC','Etf_CBOE_EZBC'],
        ['BTCW','Etf_NYSE_BTCW'], ['BTC','Etf_NYSE_BTC'],   ['MSBT','Etf_NYSE_MSBT'],
    ];

    const perEtf = {};
    await Promise.allSettled(ETF_TYPES.map(async ([ticker, type]) => {
        try {
            const r = await post('/openapi/v2/etf/historicalInflowChart', { type });
            if (r.code !== 0) return;
            (r.data?.list || []).forEach(item => {
                if (!perEtf[item.date]) perEtf[item.date] = {};
                perEtf[item.date][ticker] = parseFloat((item.totalNetInflow / 1e6).toFixed(2));
            });
            console.log(`  ${ticker}: ${(r.data?.list||[]).length} days`);
        } catch (e) {
            console.log(`  ${ticker}: skipped (${e.message})`);
        }
    }));

    // 3. Build rows — strip weekends, convert to $M
    const rows = list
        .filter(item => {
            const d = new Date(item.date + 'T12:00:00Z');
            return d.getDay() !== 0 && d.getDay() !== 6;
        })
        .map(item => {
            const row = { date: item.date };
            ETFS.forEach(e => { row[e] = perEtf[item.date]?.[e] ?? null; });
            row.total = parseFloat((item.totalNetInflow / 1e6).toFixed(2));
            return row;
        })
        .filter(r => r.total !== 0)
        .sort((a, b) => b.date.localeCompare(a.date));

    // 4. Merge with existing data so history beyond 300-day window is preserved
    let existingRows = [];
    if (fs.existsSync('etf-flows.json')) {
        try {
            const existing = JSON.parse(fs.readFileSync('etf-flows.json', 'utf8'));
            existingRows = existing.rows || [];
            console.log(`Merging with ${existingRows.length} existing rows`);
        } catch { /* start fresh */ }
    }

    const merged = {};
    existingRows.forEach(r => { merged[r.date] = r; });
    rows.forEach(r => { merged[r.date] = r; }); // fresh data wins

    const finalRows = Object.values(merged)
        .sort((a, b) => b.date.localeCompare(a.date));

    // 5. Write output
    const output = {
        generated: new Date().toISOString(),
        source:    'SoSoValue API via GitHub Actions',
        etfs:      ETFS,
        count:     finalRows.length,
        latest:    finalRows[0]?.date,
        oldest:    finalRows[finalRows.length - 1]?.date,
        rows:      finalRows,
    };

    fs.writeFileSync('etf-flows.json', JSON.stringify(output, null, 2));
    console.log(`\nDone! Wrote ${finalRows.length} rows. Latest: ${finalRows[0]?.date}`);
}

main().catch(err => {
    console.error('FATAL:', err.message);
    process.exit(1);
});
