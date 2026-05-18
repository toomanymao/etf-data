const https = require('https');
const fs    = require('fs');

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

function httpGet(hostname, path, headers = {}) {
    return new Promise((resolve, reject) => {
        const options = { hostname, port: 443, path, method: 'GET',
            headers: { 'User-Agent': 'Mozilla/5.0', ...headers },
            rejectUnauthorized: false };
        const req = https.request(options, res => {
            let raw = '';
            res.on('data', chunk => raw += chunk);
            res.on('end', () => resolve(raw));
        });
        req.on('error', reject);
        req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
        req.end();
    });
}

function httpPost(hostname, path, body, headers = {}) {
    return new Promise((resolve, reject) => {
        const data = JSON.stringify(body);
        const options = { hostname, port: 443, path, method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data), ...headers },
            rejectUnauthorized: false };
        const req = https.request(options, res => {
            let raw = '';
            res.on('data', chunk => raw += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(raw)); }
                catch { reject(new Error('Bad JSON')); }
            });
        });
        req.on('error', reject);
        req.setTimeout(30000, () => { req.destroy(); reject(new Error('Timeout')); });
        req.write(data);
        req.end();
    });
}

// ── SCRAPE FARSIDE ─────────────────────────────────────────────────────────
// Farside table columns: Date, IBIT, FBTC, GBTC, ARKB, BITB, BTCO, HODL, BRRR, EZBC, BTCW, BTC, MSBT, Total
async function fetchFarside() {
    console.log('Scraping Farside...');
    const html = await httpGet('farside.co.uk', '/btc/');

    const rows = [];
    const rowRegex = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
    let match;

    while ((match = rowRegex.exec(html)) !== null) {
        const rowHtml = match[1];
        const cellRegex = /<td[^>]*>([\s\S]*?)<\/td>/gi;
        const cells = [];
        let cm;
        while ((cm = cellRegex.exec(rowHtml)) !== null) {
            cells.push(cm[1].replace(/<[^>]+>/g, '').replace(/&nbsp;/g,' ').trim());
        }

        // Farside format: DD/MM/YYYY in first cell
        if (cells.length >= 13 && /^\d{2}\/\d{2}\/\d{4}$/.test(cells[0])) {
            const parts = cells[0].split('/');
            const date = `${parts[2]}-${parts[1]}-${parts[0]}`;
            if (!isTradingDay(date)) continue;

            const parseVal = s => {
                s = s.replace(/,/g,'').trim();
                if (!s || s === '-' || s === '' || s.toLowerCase() === 'n/a') return null;
                const n = parseFloat(s);
                return isNaN(n) ? null : n;
            };

            // Columns after date: IBIT, FBTC, GBTC, ARKB, BITB, BTCO, HODL, BRRR, EZBC, BTCW, BTC, MSBT, Total
            const etfVals = cells.slice(1, 13).map(parseVal);
            const total = parseVal(cells[13]) ?? etfVals.reduce((s, v) => s + (v || 0), 0);

            if (total === null || total === 0) continue;

            const row = { date };
            ETFS.forEach((e, i) => { row[e] = etfVals[i] ?? null; });
            row.total = parseFloat(total.toFixed(2));
            rows.push(row);
        }
    }

    rows.sort((a, b) => b.date.localeCompare(a.date));
    console.log(`Farside: ${rows.length} rows | Latest: ${rows[0]?.date}`);
    if (rows.length > 0) console.log('Sample:', JSON.stringify(rows[0]).slice(0, 150));
    return rows;
}

// ── SOSOVALUE TOTALS (fallback / gap-fill) ─────────────────────────────────
async function fetchSoSoValue() {
    if (!SOSO_KEY) throw new Error('No key');
    console.log('Fetching SoSoValue totals...');
    const res = await httpPost('api.sosovalue.xyz', '/openapi/v2/etf/historicalInflowChart',
        { type: 'us-btc-spot' }, { 'x-soso-api-key': SOSO_KEY });

    const list = Array.isArray(res.data) ? res.data : (res.data?.list || []);
    console.log(`SoSoValue: ${list.length} rows | code: ${res.code}`);

    return list
        .filter(item => item.date && isTradingDay(item.date))
        .map(item => {
            const row = { date: item.date };
            ETFS.forEach(e => { row[e] = null; });
            row.total = parseFloat((item.totalNetInflow / 1e6).toFixed(2));
            return row;
        })
        .filter(r => r.total !== 0)
        .sort((a, b) => b.date.localeCompare(a.date));
}

// ── MERGE & WRITE ──────────────────────────────────────────────────────────
function mergeAndWrite(primaryRows, fallbackRows, source) {
    // Start with existing file
    let existingRows = [];
    if (fs.existsSync('etf-flows.json')) {
        try {
            const existing = JSON.parse(fs.readFileSync('etf-flows.json', 'utf8'));
            existingRows = (existing.rows || []).filter(r => r.total !== 0 && isTradingDay(r.date));
        } catch(e) {}
    }

    const merged = {};
    existingRows.forEach(r => { merged[r.date] = r; });
    fallbackRows.forEach(r => { merged[r.date] = r; });   // fallback fills gaps
    primaryRows.forEach(r => { merged[r.date] = r; });    // primary wins

    const finalRows = Object.values(merged)
        .filter(r => r.total !== 0 && isTradingDay(r.date))
        .sort((a, b) => b.date.localeCompare(a.date));

    const hasPerEtf = finalRows.slice(0, 10).some(r => ETFS.some(e => r[e] !== null && r[e] !== 0));

    const output = {
        generated: new Date().toISOString(),
        source,
        hasPerEtf,
        etfs: ETFS,
        count: finalRows.length,
        latest: finalRows[0]?.date,
        oldest: finalRows[finalRows.length - 1]?.date,
        rows: finalRows,
    };

    fs.writeFileSync('etf-flows.json', JSON.stringify(output, null, 2));
    console.log(`\nWritten: ${finalRows.length} rows | Latest: ${finalRows[0]?.date} | hasPerEtf: ${hasPerEtf}`);
}

// ── MAIN ───────────────────────────────────────────────────────────────────
async function main() {
    console.log(`[${new Date().toISOString()}] Starting ETF fetch...`);

    let farsideRows = [];
    let sosoRows = [];

    // Try Farside first (has per-ETF breakdown)
    try {
        farsideRows = await fetchFarside();
    } catch(e) {
        console.warn('Farside failed:', e.message);
    }

    // Try SoSoValue for totals (fills any gaps Farside might have)
    try {
        sosoRows = await fetchSoSoValue();
    } catch(e) {
        console.warn('SoSoValue failed:', e.message);
    }

    if (farsideRows.length === 0 && sosoRows.length === 0) {
        console.warn('All sources failed — keeping existing data');
        return;
    }

    // Farside is primary (has per-ETF), SoSoValue fills gaps
    const source = farsideRows.length > 0
        ? 'Farside Investors (scraped daily)'
        : 'SoSoValue API';

    mergeAndWrite(farsideRows, sosoRows, source);
}

main().catch(err => {
    console.error('Fatal:', err.message);
    process.exit(0);
});
